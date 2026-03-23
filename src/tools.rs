use crate::models::KafkaMessage;
use anyhow::Context;
use arrow::array::{
    ArrayRef, BinaryBuilder, Int32Builder, Int64Builder, MapBuilder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use indicatif::ProgressBar;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::message::{BorrowedMessage, Headers, Message};
use std::fs::File;
use std::sync::Arc;
use std::time::Duration as StdDuration;

impl TryFrom<&BorrowedMessage<'_>> for KafkaMessage {
    type Error = anyhow::Error;

    fn try_from(msg: &BorrowedMessage) -> Result<Self, Self::Error> {
        let headers = msg
            .headers()
            .map(|h| {
                (0..h.count())
                    .map(|i| h.get(i))
                    .map(|header| {
                        (
                            header.key.to_string(),
                            header.value.unwrap_or_default().to_vec(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(KafkaMessage {
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp: msg.timestamp().to_millis(),
            key: msg.key().map(Vec::from),
            payload: msg.payload().map(Vec::from),
            headers,
            json: None,
        })
    }
}

pub fn write_parquet(
    path: &std::path::Path,
    messages: Vec<KafkaMessage>,
    compression: Compression,
    json_field: bool,
) -> anyhow::Result<()> {
    let header_key_field = Field::new("keys", DataType::Utf8, false);
    let header_value_field = Field::new("values", DataType::Binary, true);

    let mut fields = vec![
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, true),
        Field::new("key", DataType::Binary, true),
        Field::new("value", DataType::Binary, true),
        Field::new(
            "headers",
            DataType::Map(
                FieldRef::from(Box::new(Field::new(
                    "entries",
                    DataType::Struct(vec![header_key_field, header_value_field].into()),
                    false,
                ))),
                false,
            ),
            true,
        ),
    ];

    if json_field {
        fields.push(Field::new("json", DataType::Utf8, true))
    }

    let schema = Schema::new(fields);

    let schema_ref = Arc::new(schema);

    let mut topic_builder = StringBuilder::new();
    let mut part_builder = Int32Builder::new();
    let mut offset_builder = Int64Builder::new();
    let mut ts_builder = Int64Builder::new();
    let mut key_builder = BinaryBuilder::new();
    let mut val_builder = BinaryBuilder::new();

    let map_key_builder = StringBuilder::new();
    let map_val_builder = BinaryBuilder::new();
    let mut headers_builder = MapBuilder::new(None, map_key_builder, map_val_builder);
    let mut json_builder = StringBuilder::new();

    for msg in messages {
        topic_builder.append_value(msg.topic);
        part_builder.append_value(msg.partition);
        offset_builder.append_value(msg.offset);
        ts_builder.append_option(msg.timestamp);

        match msg.key {
            Some(k) => key_builder.append_value(k),
            None => key_builder.append_null(),
        }
        match msg.payload {
            Some(p) => val_builder.append_value(p),
            None => val_builder.append_null(),
        }

        if msg.headers.is_empty() {
            headers_builder.append(false)?;
        } else {
            for (h_key, h_val) in msg.headers {
                headers_builder.keys().append_value(h_key);
                headers_builder.values().append_value(h_val);
            }

            headers_builder.append(true)?;
        }

        if json_field {
            if let Some(value) = msg.json {
                json_builder.append_option(serde_json::to_string(&value).ok());
            } else {
                json_builder.append_null();
            }
        }
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(topic_builder.finish()),
        Arc::new(part_builder.finish()),
        Arc::new(offset_builder.finish()),
        Arc::new(ts_builder.finish()),
        Arc::new(key_builder.finish()),
        Arc::new(val_builder.finish()),
        Arc::new(headers_builder.finish()),
    ];

    if json_field {
        columns.push(Arc::new(json_builder.finish()));
    }

    let batch = RecordBatch::try_new(schema_ref.clone(), columns)?;

    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let file = File::create(path).context("Impossible de créer le fichier Parquet")?;
    let mut writer = ArrowWriter::try_new(file, schema_ref, Some(props))?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

pub async fn ensure_partition_count(
    admin: &AdminClient<DefaultClientContext>,
    topic_name: &str,
    needed_partitions: i32,
    pb: &ProgressBar,
) -> anyhow::Result<()> {
    let metadata = admin
        .inner()
        .fetch_metadata(Some(topic_name), StdDuration::from_secs(5))
        .context("Échec récupération metadata des partitions")?;

    let current_partitions = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic_name)
        .map(|t| t.partitions().len() as i32)
        .unwrap_or(0);

    if current_partitions >= needed_partitions {
        return Ok(());
    }

    pb.println(format!(
        "[Partitions] Extension du topic '{}' : {} -> {} partitions...",
        topic_name, current_partitions, needed_partitions
    ));

    let new_partitions = NewPartitions::new(topic_name, needed_partitions as usize);
    let options = AdminOptions::new().operation_timeout(Some(StdDuration::from_secs(10)));

    match admin.create_partitions(&[new_partitions], &options).await {
        Ok(results) => {
            for result in results {
                match result {
                    Ok(_) => pb.println("[Partitions] Partitions augmentées."),
                    Err((_, err)) => pb.println(format!("Erreur extension partitions : {:?}", err)),
                }
            }
        }
        Err(e) => pb.println(format!(
            "[Partitions] Erreur administrative Kafka : {:?}",
            e
        )),
    }

    Ok(())
}

pub async fn ensure_topic_exists(
    admin: &AdminClient<DefaultClientContext>,
    topic_name: &str,
    max_message_bytes: Option<&str>,
    pb: &ProgressBar,
) -> anyhow::Result<()> {
    let metadata = admin
        .inner()
        .fetch_metadata(Some(topic_name), StdDuration::from_secs(2));

    if let Ok(meta) = metadata
        && meta
            .topics()
            .iter()
            .any(|t| t.name() == topic_name && !t.partitions().is_empty())
    {
        return Ok(());
    }

    pb.println(format!(
        "[Topic] Création du topic manquant : '{}'",
        topic_name
    ));

    let mut new_topic = NewTopic::new(topic_name, 1, TopicReplication::Fixed(-1));

    if let Some(mb) = max_message_bytes {
        new_topic = new_topic.set("max.message.bytes", mb);
    }

    let options = AdminOptions::new().operation_timeout(Some(StdDuration::from_secs(10)));

    match admin.create_topics(&[new_topic], &options).await {
        Ok(results) => {
            for result in results {
                match result {
                    Ok(_) => pb.println(format!("✅ Topic '{}' créé avec succès.", topic_name)),
                    Err((name, err)) => pb.println(format!(
                        "[Topic] Erreur création topic '{}': {:?}",
                        name, err
                    )),
                }
            }
        }
        Err(e) => pb.println(format!(
            "[Topic] Erreur administrative Kafka lors de la création : {:?}",
            e
        )),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::KafkaMessage;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Compression;
    use tempfile::NamedTempFile;

    #[test]
    fn test_write_parquet_roundtrip() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path();

        let messages = vec![
            KafkaMessage {
                topic: "test_topic".to_string(),
                partition: 0,
                offset: 100,
                timestamp: Some(1672531200000),
                key: Some(b"key1".to_vec()),
                payload: Some(b"payload1".to_vec()),
                headers: vec![("h1".to_string(), b"v1".to_vec())],
                json: None,
            },
            KafkaMessage {
                topic: "test_topic".to_string(),
                partition: 1,
                offset: 101,
                timestamp: None,
                key: None,
                payload: None,
                headers: vec![],
                json: None,
            },
        ];

        let result = write_parquet(path, messages.clone(), Compression::UNCOMPRESSED, false);
        assert!(
            result.is_ok(),
            "L'écriture Parquet a échoué: {:?}",
            result.err()
        );

        let metadata = std::fs::metadata(path).unwrap();
        assert!(metadata.len() > 0);

        println!("{:?}", metadata);
    }

    #[test]
    fn test_parquet_column_count() {
        let file_without_json = NamedTempFile::new().unwrap();
        let file_with_json = NamedTempFile::new().unwrap();

        let messages = vec![KafkaMessage {
            topic: "test".to_string(),
            partition: 0,
            offset: 1,
            timestamp: Some(123456789),
            key: None,
            payload: Some(vec![1, 2, 3]),
            headers: vec![],
            json: Some(serde_json::json!({"data": "test"})),
        }];

        write_parquet(
            file_without_json.path(),
            messages.clone(),
            Compression::UNCOMPRESSED,
            false,
        )
        .unwrap();

        let f_no_json = File::open(file_without_json.path()).unwrap();
        let builder_no_json = ParquetRecordBatchReaderBuilder::try_new(f_no_json).unwrap();
        let schema_no_json = builder_no_json.schema();

        assert_eq!(
            schema_no_json.fields().len(),
            7,
            "Le fichier doit contenir exactement 7 colonnes"
        );

        write_parquet(
            file_with_json.path(),
            messages,
            Compression::UNCOMPRESSED,
            true,
        )
        .unwrap();

        let f_with_json = File::open(file_with_json.path()).unwrap();
        let builder_with_json = ParquetRecordBatchReaderBuilder::try_new(f_with_json).unwrap();
        let schema_with_json = builder_with_json.schema();

        assert_eq!(
            schema_with_json.fields().len(),
            8,
            "Le fichier doit contenir exactement 8 colonnes (json_field true)"
        );
        assert!(
            schema_with_json.field_with_name("json").is_ok(),
            "colonne 'json' manquante"
        );
    }
}
