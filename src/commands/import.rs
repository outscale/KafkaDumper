use crate::handlers::protobuf::{json_to_protobuf_bytes, prepare_schema_descriptor};
use crate::models::{ImportConfiguration, WireFormatHeader};
use crate::tools::{ensure_partition_count, ensure_topic_exists};
use anyhow::Context;
use arrow::array::{Array, BinaryArray, Int32Array, Int64Array, MapArray, StringArray};
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use tokio::time::sleep;

pub async fn import_messages(config: &ImportConfiguration) -> anyhow::Result<()> {
    println!("🚀 Démarrage de l'import...");

    let mut expanded_paths: Vec<PathBuf> = Vec::new();

    for pattern in &config.inputs {
        let paths = glob(pattern).context(format!("Pattern invalide : {}", pattern))?;

        let mut found = false;
        for entry in paths {
            match entry {
                Ok(path) => {
                    expanded_paths.push(path);
                    found = true;
                }
                Err(e) => println!("Erreur de lecture sur un fichier du pattern : {:?}", e),
            }
        }

        if !found {
            println!("Aucun fichier trouvé pour le pattern : {}", pattern);
        }
    }

    if expanded_paths.is_empty() {
        return Err(anyhow::anyhow!("Aucun fichier valide trouvé en entrée."));
    }

    println!("📂 Fichiers trouvé : {}", expanded_paths.len());

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &config.broker)
        .create()
        .context("Échec de création de l'AdminClient")?;

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &config.broker)
        .set("message.timeout.ms", "30000")
        .set("queue.buffering.max.ms", "100");

    client_config.set("topic.metadata.refresh.interval.ms", "1000");

    if let Some(mb) = config.max_message_bytes.as_deref() {
        client_config.set("message.max.bytes", mb);
    }

    let producer: FutureProducer = client_config.create()?;

    let mut total_rows = 0;

    for path in &expanded_paths {
        let file = File::open(path)
            .with_context(|| format!("Impossible d'ouvrir le fichier {:?}", path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        total_rows += builder.metadata().file_metadata().num_rows();
    }

    println!("📊 Total de messages à traiter : {}", total_rows);

    let pb = ProgressBar::new(total_rows as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) (temps estimé : {eta})",
            )?,
    );

    let mut topic_partition_cache: HashMap<String, i32> = HashMap::new();
    let mut schema_cache: HashMap<String, String> = HashMap::new();

    for input_path in &expanded_paths {
        pb.set_message(format!(
            "Traitement de {:?}",
            input_path.file_name().unwrap()
        ));

        let file = File::open(input_path)
            .with_context(|| format!("Erreur lors de l'ouverture de {:?}", input_path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        for batch_result in reader {
            let batch = batch_result.context("Erreur de lecture d'un batch")?;

            let col_topic = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let col_partition = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let col_ts = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let col_key = batch
                .column(4)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let col_val = batch
                .column(5)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();

            let is_json_parsing = config.protobuf_consumer.is_some();

            let col_json = if is_json_parsing {
                Some(
                    batch
                        .column(7)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                )
            } else {
                None
            };

            let col_headers = batch.column(6).as_any().downcast_ref::<MapArray>().unwrap();
            let header_keys = col_headers
                .keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let header_values = col_headers
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                let topic_raw = col_topic.value(i);
                let partition = col_partition.value(i);

                let topic = if config.use_original_topic {
                    topic_raw
                } else {
                    config.target_topic.as_deref().unwrap_or(topic_raw)
                };

                let topic_key = topic.to_string();

                if !topic_partition_cache.contains_key(&topic_key) {
                    ensure_topic_exists(&admin, topic, config.max_message_bytes.as_deref(), &pb)
                        .await?;

                    let meta = producer
                        .client()
                        .fetch_metadata(Some(topic), std::time::Duration::from_secs(5));
                    let count = if let Ok(m) = meta {
                        m.topics()
                            .iter()
                            .find(|t| t.name() == topic)
                            .map(|t| t.partitions().len() as i32)
                            .unwrap_or(1)
                    } else {
                        1
                    };
                    topic_partition_cache.insert(topic_key.clone(), count);
                }

                let payload = &col_val.value(i).to_vec();

                if is_json_parsing
                    && let Some(kpc) = config.protobuf_consumer.as_ref()
                    && !schema_cache.contains_key(&topic_key)
                {
                    match WireFormatHeader::from_bytes(payload) {
                        Ok(header) => {
                            if payload.len() > (header.position) {
                                let schema = match kpc.get_schema_from_topic(topic_raw).await {
                                    Ok(s) => s,
                                    Err(e) => {
                                        eprintln!(
                                            "Impossible de récupérer le schéma {} {} {} {}",
                                            header.schema_id, e, topic, partition
                                        );

                                        continue;
                                    }
                                };

                                schema_cache.insert(topic_key.clone(), schema);
                            }
                        }

                        Err(_) => {
                            eprintln!("WireFormatHeader::from_bytes failed: {:?}", payload);
                            continue;
                        }
                    }
                }

                let needed_partitions = partition + 1;
                let known_partitions = *topic_partition_cache.get(&topic_key).unwrap();

                if needed_partitions > known_partitions {
                    ensure_partition_count(&admin, topic, needed_partitions, &pb).await?;
                    sleep(tokio::time::Duration::from_secs(2)).await;

                    if let Err(e) = producer
                        .client()
                        .fetch_metadata(Some(topic), std::time::Duration::from_secs(10))
                    {
                        pb.println(format!("Erreur fetch metadata : {}", e));
                    }

                    topic_partition_cache.insert(topic_key.clone(), needed_partitions);
                }

                let mut proto_payload = Vec::new();
                let mut has_valid_json = false;

                if is_json_parsing
                    && let Some(json_array) = &col_json
                    && !json_array.is_null(i)
                {
                    let json_str = json_array.value(i);
                    let schema = schema_cache.get(&topic_key).unwrap();

                    match json_to_protobuf_bytes(
                        json_str,
                        &prepare_schema_descriptor(schema, WireFormatHeader::from_bytes(payload)?)
                            .await?,
                    ) {
                        Ok(bytes) => {
                            proto_payload = bytes;
                            has_valid_json = true;
                        }
                        Err(e) => {
                            pb.println(format!(
                                "Erreur de parsing JSON (Topic: {}): {:?}",
                                topic, e
                            ));
                            continue;
                        }
                    }
                }

                let max_retries = 5;
                let mut sent = false;

                for attempt in 0..max_retries {
                    let mut record = FutureRecord::to(topic).partition(partition);

                    if !col_key.is_null(i) {
                        record = record.key(col_key.value(i));
                    }
                    if !col_ts.is_null(i) {
                        record = record.timestamp(col_ts.value(i));
                    }

                    if is_json_parsing {
                        if has_valid_json {
                            record = record.payload(&proto_payload);
                        }
                    } else if !col_val.is_null(i) {
                        record = record.payload(payload);
                    }

                    if !col_headers.is_null(i) {
                        let mut kafka_headers = OwnedHeaders::new();
                        let offset = col_headers.value_offsets();
                        let start = offset[i] as usize;
                        let end = offset[i + 1] as usize;

                        for h_idx in start..end {
                            if !header_keys.is_null(h_idx) {
                                let k = header_keys.value(h_idx);
                                let v = if header_values.is_null(h_idx) {
                                    &[] as &[u8]
                                } else {
                                    header_values.value(h_idx)
                                };
                                kafka_headers = kafka_headers.insert(Header {
                                    key: k,
                                    value: Some(v),
                                });
                            }
                        }
                        record = record.headers(kafka_headers);
                    }

                    match producer
                        .send(record, std::time::Duration::from_secs(0))
                        .await
                    {
                        Ok(_) => {
                            sent = true;
                            break;
                        }
                        Err((KafkaError::PartitionEOF(_), _)) => {
                            pb.println(format!(
                                "Partition inconnue (tentative {}/{}), rafraichissement...",
                                attempt + 1,
                                max_retries
                            ));

                            let _ = producer
                                .client()
                                .fetch_metadata(Some(topic), std::time::Duration::from_secs(5));
                            sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                        Err((e, _)) => {
                            pb.println(format!(
                                "Erreur fatale send (Topic: {}, Part: {}): {:?}",
                                topic, partition, e
                            ));
                            break;
                        }
                    }
                }

                if !sent {
                    pb.println(format!(
                        "Message perdu (Topic: {}, Part: {})",
                        topic, partition
                    ));
                }
                pb.inc(1);
            }
        }
    }

    pb.set_message("Flush des derniers messages...");
    producer.flush(std::time::Duration::from_secs(30))?;
    pb.finish_with_message("✅ Import terminé avec succès !");
    Ok(())
}
