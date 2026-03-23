use anyhow::Result;
use clap::{Parser, Subcommand};
use kafkadumper::commands::export::export_topics;
use kafkadumper::commands::import::import_messages;
use kafkadumper::commands::inspect::inspect_dump;
use kafkadumper::models::{ExportConfiguration, ImportConfiguration, KafkaProtobufConsumer};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "kafka-dump", version)]
#[command(about = "Outil pour dumper et restore des topics Kafka", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Exporter un ou plusieurs topics
    Export {
        /// Kafka broker
        #[arg(short, long, default_value = "localhost:9092")]
        broker: String,

        /// Nom des topics à exporter
        #[arg(short, long, required = true, value_delimiter = ',')]
        topics: Vec<String>,

        /// Nom du fichier d'output
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Partitions spécifiques (format: 1,2,...)
        #[arg(short, long, value_delimiter = ',')]
        partitions: Option<Vec<i32>>,

        /// Nombre maximum de messages à exporter
        #[arg(short = 'n', long)]
        max_messages: Option<usize>,

        /// Mode tail: Récupérer les messages depuis la fin
        #[arg(long, default_value = "false")]
        tail: bool,

        /// Exporter seulement les N derniers jours
        #[arg(short, long)]
        days: Option<i64>,

        /// Group ID pour le consumer
        #[arg(short, long, default_value = "kafka-dumper")]
        group_id: String,

        /// Compression algorithm(level) (https://arrow.apache.org/rust/parquet/basic/enum.Compression.html#variants)
        #[arg(short, long, default_value = "uncompressed")]
        compression: String,

        /// Nombre de messages par fichier
        #[arg(short, long, default_value = "0")]
        split: usize,

        /// Utiliser le schéma registry pour ajouter le message décodé
        #[arg(short, long)]
        use_schema_registry: Option<String>,

        /// Offset de départ (priorité sur propriétés days et tails)
        #[arg(long)]
        start_offset: Option<i64>,

        /// Arrêt de consommation par partition
        #[arg(long)]
        end_offset: Option<i64>,
    },
    /// Importer des messages dans un topic
    Import {
        /// Kafka broker
        #[arg(short, long, default_value = "localhost:9092")]
        broker: String,

        /// Fichiers d'input
        #[arg(short, long, required = true, value_delimiter = ',')]
        inputs: Vec<String>,

        /// Remapper le topic de destination (optionnel)
        #[arg(short = 'T', long)]
        target_topic: Option<String>,

        /// Paramètre 'message.max.bytes' (topic et producer) : 1Mib=104857600
        #[arg(long)]
        max_message_bytes: Option<String>, // 1Mib=104857600

        /// Importer chaque message dans son topic initial
        #[arg(long, default_value = "false")]
        use_original_topic: bool,

        /// Importer les messages à partir du json (parsing dynamique)
        #[arg(long)]
        use_schema_registry: Option<String>,
    },
    /// Analyser un fichier de dump sans importer
    Inspect {
        /// Fichier d'input
        #[arg(short, long)]
        input: PathBuf,

        /// Information mise à jour de vérification
        #[arg(short, long, default_value = "100")]
        count: usize,

        /// Afficher le détail de chaque message
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
