use anyhow::Context;
use arrow::array::{Array, BinaryArray, Int32Array, Int64Array, StringArray};
use chrono::{DateTime, Local};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

const DATE_FORMAT: &str = "%d/%m/%Y %H:%M:%S (%Z)";

pub fn inspect_dump(input: PathBuf, count: usize, verbose: bool) -> anyhow::Result<()> {
    let file = File::open(&input).context("Échec d'ouverture du fichier Parquet")?;

    // métadonnées sans lire tout le fichier
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("Le fichier n'est pas un format Parquet valide")?;

    let parquet_metadata = builder.metadata().file_metadata();
    let total_rows = parquet_metadata.num_rows();
    let created_by = parquet_metadata.created_by().unwrap_or("Inconnu");

    println!("RAPPORT D'INSPECTION : {}", input.display());
    println!("--------------------------------------------------");
    println!("Format        : Apache Parquet");
    println!("Créé par      : {}", created_by);
    println!("Total lignes  : {}", total_rows);
    println!("--------------------------------------------------");

    let reader = builder.build()?;

    let mut actual_count = 0;

    let mut last_ts: Option<i64> = None;
    let mut is_increasing = true;
    let mut is_decreasing = true;
    let mut min_ts: Option<i64> = None;
    let mut max_ts: Option<i64> = None;

    let mut found_topics: HashSet<String> = HashSet::new();

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
        let col_offset = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
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

        for i in 0..batch.num_rows() {
            actual_count += 1;

            let topic = col_topic.value(i);
            let partition = col_partition.value(i);
            let offset = col_offset.value(i);

            let ts = if col_ts.is_null(i) {
                None
            } else {
                Some(col_ts.value(i))
            };

            found_topics.insert(topic.to_string());

            if let Some(t) = ts {
                if min_ts.map(|min| t < min).unwrap_or(true) {
                    min_ts = Some(t);
                }
                if max_ts.map(|max| t > max).unwrap_or(true) {
                    max_ts = Some(t);
                }

                if let Some(prev) = last_ts {
                    if t < prev {
                        is_increasing = false;
                    }
                    if t > prev {
                        is_decreasing = false;
                    }
                }
                last_ts = Some(t);
            }

            if verbose {
                let key_disp = if col_key.is_null(i) {
                    "None".to_string()
                } else {
                    String::from_utf8_lossy(col_key.value(i)).to_string()
                };

                let len_disp = if col_val.is_null(i) {
                    0
                } else {
                    col_val.value(i).len()
                };

                println!(
                    "[{}] Part:{} Offset:{} | Timestamp:{} | Key: {:?} | Len: {} bytes",
                    topic,
                    partition,
                    offset,
                    ts.unwrap_or(-1),
                    key_disp,
                    len_disp
                );
            } else if count > 0 && actual_count % count == 0 {
                print!("\r  Vérification en cours... {} messages lus", actual_count);
                std::io::stdout().flush()?;
            }
        }
    }

    if !verbose {
        println!();
    }

    println!("--------------------------------------------------");
    println!("Topics trouvés : {:?}", found_topics);

    print!(" TRI (TIMESTAMP) : ");
    if actual_count == 0 {
        println!("Vide");
    } else if is_increasing && is_decreasing {
        println!("Tous les timestamps sont identiques");
    } else if is_increasing {
        println!("+ Croissant (Du plus ancien au plus récent)");
    } else if is_decreasing {
        println!("- Décroissant (Du plus récent au plus ancien)");
    } else {
        println!("!!! Non ordonné (Mélangé)");
    }

    if let (Some(min), Some(max)) = (min_ts, max_ts) {
        let min_utc = DateTime::from_timestamp_millis(min).unwrap_or_default();
        let max_utc = DateTime::from_timestamp_millis(max).unwrap_or_default();

        let min_local: DateTime<Local> = min_utc.with_timezone(&Local);
        let max_local: DateTime<Local> = max_utc.with_timezone(&Local);

        println!(
            "   Début (Min)    : {} (ts: {})",
            min_local.format(DATE_FORMAT),
            min
        );
        println!(
            "   Fin (Max)      : {} (ts: {})",
            max_local.format(DATE_FORMAT),
            max
        );
    }
    println!("--------------------------------------------------");

    if actual_count as i64 == total_rows {
        println!(
            "✅ Vérification réussie : {} messages valides lus (conforme aux métadonnées Parquet).",
            actual_count
        );
    } else {
        eprintln!("ATTENTION : Incohérence détectée !");
        eprintln!("   Metadonnées Parquet : {}", total_rows);
        eprintln!("   Lignes lues         : {}", actual_count);
    }

    Ok(())
}
