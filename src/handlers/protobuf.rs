use crate::models::WireFormatHeader;
use anyhow::{Context, anyhow};
use prost::Message;
use prost_reflect::prost_types::DescriptorProto;
use prost_reflect::{
    DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage, Value as ProtoValue,
};
use protox::Compiler;
use serde_json::Value;
use std::path::PathBuf;
use tokio::fs;

pub fn zigzag_decode(n: u64) -> i64 {
    // (n >> 1) ^ -(n & 1)
    let tmp = (n >> 1) as i64;
    if (n & 1) == 1 { !tmp + 1 } else { tmp }
}

pub fn proto_dynamic_to_json(message: &DynamicMessage) -> Value {
    let mut map = serde_json::Map::new();

    for field in message.descriptor().fields() {
        if message.has_field(&field) {
            let value = message.get_field(&field).into_owned();

            let json_value = if let Some(enum_desc) = field.kind().as_enum() {
                match value {
                    prost_reflect::Value::EnumNumber(n) => Value::String(
                        enum_desc
                            .get_value(n)
                            .map(|v| v.name().to_string())
                            .unwrap_or_else(|| n.to_string()),
                    ),
                    _ => proto_value_to_json(&value),
                }
            } else {
                proto_value_to_json(&value)
            };

            map.insert(field.name().to_string(), json_value);
        }
    }

    Value::Object(map)
}

pub fn proto_value_to_json(v: &ProtoValue) -> Value {
    match v {
        ProtoValue::Bool(b) => Value::Bool(*b),
        ProtoValue::I32(n) => Value::Number((*n).into()),
        ProtoValue::I64(n) => Value::Number((*n).into()),
        ProtoValue::U32(n) => Value::Number((*n).into()),
        ProtoValue::U64(n) => Value::Number((*n).into()),
        ProtoValue::F32(f) => Value::Number(serde_json::Number::from_f64(*f as f64).unwrap()),
        ProtoValue::F64(f) => Value::Number(serde_json::Number::from_f64(*f).unwrap()),
        ProtoValue::String(s) => Value::String(s.clone()),
        ProtoValue::Bytes(b) => Value::String(hex::encode(b)),
        ProtoValue::EnumNumber(n) => Value::Number((*n).into()),
        ProtoValue::List(list) => Value::Array(list.iter().map(proto_value_to_json).collect()),
        ProtoValue::Message(m) => proto_dynamic_to_json(m),
        _ => Value::Null,
    }
}

pub fn get_all_message_names(pool: &DescriptorPool) -> Vec<String> {
    let mut names = Vec::new();

    for file_proto in pool.file_descriptor_protos() {
        for msg in &file_proto.message_type {
            collect_message_names(msg, &mut names, file_proto.package.as_deref().unwrap_or(""));
        }
    }

    names
}

fn collect_message_names(msg: &DescriptorProto, names: &mut Vec<String>, prefix: &str) {
    let full_name = if prefix.is_empty() {
        msg.name.clone().unwrap_or_default()
    } else {
        format!("{}.{}", prefix, msg.name.clone().unwrap_or_default())
    };

    names.push(full_name.clone());

    for nested in &msg.nested_type {
        collect_message_names(nested, names, &full_name);
    }
}

pub async fn prepare_descriptor_pool(schema: &str) -> anyhow::Result<DescriptorPool> {
    let temp_path = PathBuf::from("temp_model.proto");

    if let Err(e) = fs::write(&temp_path, &schema).await {
        eprintln!(
            "Écriture du fichier temporaire échouée {} {}",
            temp_path.display(),
            e
        );
        return Err(anyhow!("fs::write failed: {e:?}"));
    }

    let mut compiler = match Compiler::new(["."]) {
        Ok(c) => c,
        Err(e) => {
            let _ = fs::remove_file(&temp_path).await;
            return Err(anyhow!("Compiler::new failed: {e:?}"));
        }
    };

    if let Err(e) = compiler.open_file(&temp_path) {
        let _ = fs::remove_file(&temp_path).await;
        return Err(anyhow!("compiler.open_file failed: {e:?}"));
    }

    let file_descriptor_set = compiler.file_descriptor_set();
    let descriptor_pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set);

    let _ = fs::remove_file(&temp_path).await;

    match descriptor_pool {
        Ok(p) => Ok(p),
        Err(e) => Err(anyhow!(
            "DescriptorPool::from_file_descriptor_set failed: {e:?}"
        )),
    }
}

pub async fn prepare_schema_descriptor(
    schema: &str,
    header: WireFormatHeader,
) -> anyhow::Result<MessageDescriptor> {
    let pool = prepare_descriptor_pool(schema).await?;

    let msg_names = get_all_message_names(&pool);
    let msg_name = &msg_names[header.message_indexes[0] as usize];

    let descriptor = pool
        .get_message_by_name(msg_name)
        .ok_or_else(|| anyhow::anyhow!("Message '{}' introuvable dans le schéma", msg_name))?;

    Ok(descriptor)
}

pub fn json_to_protobuf_bytes(
    json_str: &str,
    descriptor: &MessageDescriptor,
) -> anyhow::Result<Vec<u8>> {
    let mut deserializer = serde_json::Deserializer::from_str(json_str);

    let dynamic_message = DynamicMessage::deserialize(descriptor.clone(), &mut deserializer)
        .context("Échec du parsing JSON vers Protobuf DynamicMessage")?;

    Ok(dynamic_message.encode_to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_reflect::Value as ProtoValue;

    #[test]
    fn test_proto_value_to_json_types() {
        assert_eq!(
            proto_value_to_json(&ProtoValue::Bool(true)),
            Value::Bool(true)
        );
        assert_eq!(
            proto_value_to_json(&ProtoValue::I32(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            proto_value_to_json(&ProtoValue::String("hello".to_string())),
            serde_json::json!("hello")
        );

        let bytes_val = vec![0xDE, 0xAD, 0xBE, 0xEF];
        assert_eq!(
            proto_value_to_json(&ProtoValue::Bytes(bytes_val.into())),
            serde_json::json!("deadbeef")
        );
    }
}
