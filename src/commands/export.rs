use crate::models::{ExportConfiguration, KafkaMessage};
use crate::tools::write_parquet;
use anyhow::Context;
use chrono::Duration;
use chrono::Utc;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::basic::Compression;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::Timeout;
use rdkafka::{Message, Offset};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration as StdDuration;

pub async fn export_topics(config: &ExportConfiguration) -> anyhow::Result<()> {
    println!("🚀 Starting export of topics: {:?}", config.topics);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.broker)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .set("socket.timeout.ms", "60000")
        .create()
        .context("Failed to create consumer")?;

    let mut tpl = TopicPartitionList::new();

    for topic in &config.topics {
        if let Some(ref parts) = config.partitions {
            for &part in parts {
                tpl.add_partition(topic, part);
            }
        } else {
            let metadata = consumer
                .fetch_metadata(Some(topic), StdDuration::from_secs(30))
                .context("Failed to retrieve metadata")?;

            if let Some(topic_metadata) = metadata.topics().iter().find(|t| t.name() == topic) {
                for partition in topic_metadata.partitions() {
                    tpl.add_partition(topic, partition.id());
                }
            }
        }
    }

    let elements: Vec<_> = tpl
        .elements()
        .iter()
        .map(|e| (e.topic().to_string(), e.partition()))
        .collect();

    if let Some(start) = config.start_offset {
        println!("Export from offset {}...", start);
        for (topic, partition) in &elements {
            tpl.set_partition_offset(topic, *partition, Offset::Offset(start))
                .ok();
        }
    } else if let Some(d) = config.days {
        println!("Calculating offsets for the last {} days...", d);
        let target_time = Utc::now() - Duration::days(d);
        let timestamp_ms = target_time.timestamp_millis();

        for (topic, partition) in &elements {
            tpl.set_partition_offset(topic, *partition, Offset::Offset(timestamp_ms))
                .ok();
        }
        tpl = consumer.offsets_for_times(tpl, Timeout::After(StdDuration::from_secs(30)))?;
    } else if config.tail
        && let Some(total_limit) = config.max_messages
    {
        let limit_per_part = total_limit as i64;

        println!(
            "Tail mode: Fetching the last {} messages per partition",
            limit_per_part
        );

        if let Some(first_topic) = config.topics.first() {
            let _ = consumer.fetch_metadata(Some(first_topic), StdDuration::from_secs(30));
        }

        for (topic, partition) in &elements {
            let (low, high) = consumer
                .fetch_watermarks(topic, *partition, StdDuration::from_secs(30))
                .context(format!("Failed to fetch watermarks {}:{}", topic, partition))?;

            let start_offset = std::cmp::max(low, high - limit_per_part);
            tpl.set_partition_offset(topic, *partition, Offset::Offset(start_offset))?;
        }
    } else {
        println!("Export from the beginning...");
        for (topic, partition) in &elements {
            tpl.set_partition_offset(topic, *partition, Offset::Beginning)
                .ok();
        }
    }

    consumer.assign(&tpl).context("Assignment failed")?;

    let total_partitions = tpl.count();
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner().template("{spinner:.green} [{elapsed_precise}] {msg}")?,
    );

    let mut messages = Vec::new();
    let mut eof_count = 0;

    let mut partition_counts: HashMap<(String, i32), usize> = HashMap::new();
    let mut finished_partitions: std::collections::HashSet<(String, i32)> =
        std::collections::HashSet::new();

    loop {
        if finished_partitions.len() >= total_partitions {
            break;
        }

        match consumer.recv().await {
            Ok(msg) => {
                let topic = msg.topic().to_string();
                let partition = msg.partition();
                let key = (topic.clone(), partition);
                let current_offset = msg.offset();

                if let Some(end) = config.end_offset {
                    if current_offset > end {
                        finished_partitions.insert(key);
                        continue;
                    }
                    if current_offset == end {
                        finished_partitions.insert(key.clone());
                    }
                }

                if let Some(max) = config.max_messages {
                    let count = partition_counts.entry(key.clone()).or_insert(0);

                    if *count >= max {
                        finished_partitions.insert(key);
                        continue;
                    }

                    *count += 1;
                    if *count >= max {
                        finished_partitions.insert(key);
                    }
                }

                let mut kafka_msg: KafkaMessage = (&msg).try_into()?;
                if let Some(pc) = &config.protobuf_consumer {
                    kafka_msg.json = pc.handle_to_json(&msg).await.ok();
                }

                messages.push(kafka_msg);

                if messages.len() % 100 == 0 {
                    pb.set_message(format!("Messages read: {}", messages.len()));
                }
            }
            Err(e) => match e {
                KafkaError::PartitionEOF(_) => {
                    eof_count += 1;
                    if eof_count >= total_partitions {
                        break;
                    }
                }
                _ => eprintln!("Read error: {:?}", e),
            },
        }
    }

    if messages.is_empty() {
        println!(
            "No messages retrieved from topics {:?}.",
            config.topics
        );
        return Ok(());
    }

    if let Some(max) = config.max_messages {
        println!(
            "Final sorting: keeping {} requested messages (out of {} read)...",
            max,
            messages.len()
        );

        messages.sort_by_key(|m| m.timestamp.unwrap_or(0));

        if config.tail {
            if messages.len() > max {
                let to_remove = messages.len() - max;
                messages.drain(0..to_remove);
            }
        } else if messages.len() > max {
            messages.truncate(max);
        }
    }

    pb.finish_with_message(format!("✅ {} messages exported in total", messages.len()));

    let compression = Compression::from_str(config.compression.as_str()).unwrap_or_else(|e| {
        eprintln!("{}\nThe file will not be compressed.", e);
        Compression::UNCOMPRESSED
    });

    let batch_size = config.split;

    let mut output_path: PathBuf =
        config
            .output
            .clone()
            .unwrap_or(if total_partitions == 1 && config.topics.len() == 1 {
                format!(
                    "{}-{}-{}.parquet",
                    config.topics.first().unwrap_or(&"empty".to_string()),
                    config
                        .partitions
                        .as_ref()
                        .unwrap_or(&vec![0])
                        .first()
                        .unwrap_or(&0),
                    messages
                        .iter()
                        .max_by_key(|m| m.timestamp)
                        .map(|m| m.timestamp)
                        .unwrap_or_default()
                        .unwrap_or(0)
                )
                .parse()?
            } else {
                "output".parse()?
            });

    if output_path.extension().is_none() {
        output_path.set_extension("parquet");
    }

    if batch_size > 0 && messages.len() > batch_size {
        println!(
            "Splitting {} messages into batches of {}...",
            messages.len(),
            batch_size
        );

        let file_stem = output_path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy();

        let extension = output_path.extension().unwrap().to_string_lossy();

        for (index, chunk) in messages.chunks(batch_size).enumerate() {
            let part_number = index + 1;
            let new_filename = format!("{}-part-{:03}.{}", file_stem, part_number, extension);

            let part_path = output_path.with_file_name(new_filename);

            println!(
                "  Writing part {} ({} messages) -> {}",
                part_number,
                chunk.len(),
                part_path.display()
            );

            write_parquet(
                &part_path,
                chunk.to_vec(),
                compression,
                config.protobuf_consumer.is_some(),
            )?;
        }
    } else {
        println!(
            "Writing to single file {}...",
            output_path.display()
        );
        write_parquet(
            &output_path,
            messages,
            compression,
            config.protobuf_consumer.is_some(),
        )?;
    }

    println!("✅ Export completed successfully!");
    Ok(())
}
