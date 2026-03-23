use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub json: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DumpMetadata {
    pub version: u32,
    pub created_at: DateTime<Utc>,
    pub topics: Vec<String>,
    pub total_messages: usize,
}

pub struct ExportConfiguration {
    pub broker: String,
    pub topics: Vec<String>,
    pub output: Option<PathBuf>,
    pub partitions: Option<Vec<i32>>,
    pub max_messages: Option<usize>,
    pub tail: bool,
    pub days: Option<i64>,
    pub group_id: String,
    pub compression: String,
    pub split: usize,
    pub protobuf_consumer: Option<KafkaProtobufConsumer>,
    pub start_offset: Option<i64>,
    pub end_offset: Option<i64>,
}

pub struct ImportConfiguration {
    pub broker: String,
    pub inputs: Vec<String>,
    pub target_topic: Option<String>,
    pub max_message_bytes: Option<String>,
    pub use_original_topic: bool,
    pub protobuf_consumer: Option<KafkaProtobufConsumer>,
}

#[derive(Clone, Debug)]
pub struct KafkaProtobufConsumer {
    pub schema_registry_url: String,
    pub client: reqwest::Client,
}

#[derive(Debug)]
pub struct WireFormatHeader {
    pub magic_byte: u8,
    pub schema_id: i32,
    pub message_indexes: Vec<i64>,
    pub position: usize,
}
