use anyhow::Result;
use clap::{Parser, Subcommand};
use kafkadumper::commands::export::export_topics;
use kafkadumper::commands::import::import_messages;
use kafkadumper::commands::inspect::inspect_dump;
use kafkadumper::models::{ExportConfiguration, ImportConfiguration, KafkaProtobufConsumer};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "kafka-dump", version)]
#[command(about = "Tool for dumping and restoring Kafka topics", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Export one or more topics
    Export {
        /// Kafka broker
        #[arg(short, long, default_value = "localhost:9092")]
        broker: String,

        /// Names of topics to export
        #[arg(short, long, required = true, value_delimiter = ',')]
        topics: Vec<String>,

        /// Output file name
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Specific partitions (format: 1, 2, ...)
        #[arg(short, long, value_delimiter = ',')]
        partitions: Option<Vec<i32>>,

        /// Maximum number of messages to export
        #[arg(short = 'n', long)]
        max_messages: Option<usize>,

        /// Tail mode: Retrieve messages from the end
        #[arg(long, default_value = "false")]
        tail: bool,

        /// Export only the last N days
        #[arg(short, long)]
        days: Option<i64>,

        /// Group ID for the consumer
        #[arg(short, long, default_value = "kafka-dumper")]
        group_id: String,

        /// Compression algorithm(level) (https://arrow.apache.org/rust/parquet/basic/enum.Compression.html#variants)
        #[arg(short, long, default_value = "uncompressed")]
        compression: String,

        /// Number of messages per file
        #[arg(short, long, default_value = "0")]
        split: usize,

        /// Use the registry schema to add the decoded message
        #[arg(short, long)]
        use_schema_registry: Option<String>,

        /// Initial offset (takes precedence over the `days` and `tails` properties)
        #[arg(long)]
        start_offset: Option<i64>,

        /// Stop consumption by partition
        #[arg(long)]
        end_offset: Option<i64>,
    },
    /// Import messages into a topic
    Import {
        /// Kafka broker
        #[arg(short, long, default_value = "localhost:9092")]
        broker: String,

        /// Input files
        #[arg(short, long, required = true, value_delimiter = ',')]
        inputs: Vec<String>,

        /// Remap the destination topic
        #[arg(short = 'T', long)]
        target_topic: Option<String>,

        /// 'message.max.bytes' parameter (topic and producer): 1 MiB = 10,485,760 bytes
        #[arg(long)]
        max_message_bytes: Option<String>, // 1Mib=104857600

        /// Move each message back to its original thread
        #[arg(long, default_value = "false")]
        use_original_topic: bool,

        /// Import messages from JSON (dynamic parsing)
        #[arg(long)]
        use_schema_registry: Option<String>,
    },
    /// Analyze a dump file without importing it
    Inspect {
        /// Input file
        #[arg(short, long)]
        input: PathBuf,

        /// Updated verification information
        #[arg(short, long, default_value = "100")]
        count: usize,

        /// View the details of each message
        #[arg(short, long, default_value = "false")]
        verbose: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Export {
            broker,
            topics,
            output,
            partitions,
            max_messages,
            tail,
            days,
            group_id,
            compression,
            split,
            use_schema_registry,
            start_offset,
            end_offset,
        } => {
            export_topics(&ExportConfiguration {
                broker,
                topics,
                output,
                partitions,
                max_messages,
                tail,
                days,
                group_id,
                compression,
                split,
                protobuf_consumer: use_schema_registry.map(KafkaProtobufConsumer::new),
                start_offset,
                end_offset,
            })
            .await?;
        }
        Commands::Import {
            broker,
            inputs,
            target_topic,
            max_message_bytes,
            use_original_topic,
            use_schema_registry,
        } => {
            import_messages(&ImportConfiguration {
                broker,
                target_topic,
                max_message_bytes,
                inputs,
                use_original_topic,
                protobuf_consumer: use_schema_registry.map(KafkaProtobufConsumer::new),
            })
            .await?;
        }
        Commands::Inspect {
            input,
            count,
            verbose,
        } => {
            inspect_dump(input, count, verbose)?;
        }
    }

    Ok(())
}
