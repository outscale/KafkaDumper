use crate::handlers::protobuf::{
    get_all_message_names, prepare_descriptor_pool, proto_dynamic_to_json,
};
use crate::models::{KafkaProtobufConsumer, WireFormatHeader};
use anyhow::anyhow;
use prost_reflect::DynamicMessage;
use rdkafka::Message;
use serde_json::Value;

impl KafkaProtobufConsumer {
    pub fn new(schema_registry_url: String) -> Self {
        Self {
            schema_registry_url,
            client: reqwest::Client::new(),
        }
    }

    pub async fn get_schema_from_gloal_id(&self, schema_id: i32) -> anyhow::Result<String> {
        let url = format!(
            "{}/schemas/ids/{}/schema",
            self.schema_registry_url, schema_id
        );

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Échec de récupération du schéma: {}",
                response.status()
            ));
        }

        let schema = response.text().await?;
        Ok(schema)
    }

    pub async fn get_schema_from_topic(&self, topic: &str) -> anyhow::Result<String> {
        let url = format!(
            "{}/subjects/{}-value/versions",
            self.schema_registry_url, topic
        );

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Échec de récupération du schéma: {}",
                response.status()
            ));
        }

        let versions: Vec<i32> = serde_json::from_str(response.text().await?.as_str())?;

        let url = format!("{}/{}/schema", url, versions.iter().max().unwrap_or(&1));
        let response = self.client.get(&url).send().await?;

        let schema = response.text().await?;
        Ok(schema)
    }

    pub async fn handle_to_json<M: Message>(&self, message: &M) -> anyhow::Result<Value> {
        if let Some(payload) = message.payload() {
            match WireFormatHeader::from_bytes(payload) {
                Ok(header) => {
                    if payload.len() > (header.position) {
                        let protobuf_data = &payload[header.position..];

                        // Récupération du schéma
                        let schema = match self.get_schema_from_gloal_id(header.schema_id).await {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!(
                                    "Impossible de récupérer le schéma {} {} {} {} {}",
                                    header.schema_id,
                                    e,
                                    message.topic(),
                                    message.partition(),
                                    message.offset()
                                );
                                return Err(anyhow!("get_schema failed: {e:?}"));
                            }
                        };

                        let pool = prepare_descriptor_pool(&schema).await?;

                        let msg_names = get_all_message_names(&pool);
                        let msg_name = &msg_names[header.message_indexes[0] as usize];

                        let bytes = protobuf_data;
                        let mut msg: Option<DynamicMessage> = None;

                        if let Some(msg_desc) = pool.get_message_by_name(msg_name) {
                            match DynamicMessage::decode(msg_desc, bytes) {
                                Ok(m) => {
                                    msg = Some(m);
                                }
                                Err(e) => {
                                    eprintln!("Impossible de décoder '{}' : {}", msg_name, e);
                                }
                            }
                        } else {
                            eprintln!(
                                "Message introuvable dans le schéma {} : {}",
                                header.schema_id, msg_name
                            );
                        }

                        let msg = match msg {
                            Some(m) => m,
                            None => {
                                return Err(anyhow!(
                                    "Impossible de décoder le protobuf avec aucun des messages {}, {:?}",
                                    message.offset(),
                                    hex::encode(payload),
                                ));
                            }
                        };

                        Ok(proto_dynamic_to_json(&msg))
                    } else {
                        Err(anyhow!(
                            "Payload trop petit pour contenir des données protobuf (<={} octets)",
                            header.position
                        ))
                    }
                }
                Err(e) => {
                    if let Some(payload) = message.payload() {
                        return Err(anyhow!(
                            "WireFormatHeader::from_bytes failed: {e:?}\nPayload (hex) : {}",
                            hex::encode(payload)
                        ));
                    }

                    Err(anyhow!("WireFormatHeader::from_bytes failed: {e:?}"))
                }
            }
        } else {
            Err(anyhow!("Message sans payload."))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;
    use rdkafka::Timestamp;
    use rdkafka::message::OwnedMessage;

    #[tokio::test]
    async fn test_handle_to_json_success() {
        let mut server = Server::new_async().await;
        let url = server.url();

        let schema_content = r#"
            syntax = "proto3";
            package test;
            message Person {
                string name = 1;
                int32 age = 2;
            }
        "#;

        let _ = server
            .mock("GET", "/schemas/ids/1/schema")
            .with_status(200)
            .with_body(schema_content)
            .create_async()
            .await;

        // Magic(0) + SchemaID(1) + SizeIndex(1 en zigzag -> 2) + Index(0 en zigzag -> 0)
        let mut payload = vec![0u8, 0, 0, 0, 1];
        payload.push(2); // index = 1
        payload.push(0);

        // Tag 1 (name): (1 << 3 | 2) = 10. Longueur 5. "Assim" = 65, 115, 115, 105, 109
        // Tag 2 (age): (2 << 3 | 0) = 16. Valeur 20.
        payload.extend_from_slice(&[10, 5, 65, 115, 115, 105, 109, 16, 20]);

        let owned_message = OwnedMessage::new(
            Some(payload),
            Some(vec![1, 2, 3]),
            "test-topic".to_string(),
            Timestamp::NotAvailable,
            0,
            0,
            None,
        );

        let borrowed = owned_message;

        let consumer = KafkaProtobufConsumer::new(url);
        let result = consumer.handle_to_json(&borrowed).await;

        assert!(result.is_ok(), "Le parsing a échoué : {:?}", result.err());
        let json = result.unwrap();
        assert_eq!(json["name"], "Assim");
        assert_eq!(json["age"], 20);
    }

    #[tokio::test]
    async fn test_handle_to_json_invalid_payload() {
        let server = Server::new_async().await;
        let consumer = KafkaProtobufConsumer::new(server.url());

        // Payload trop court (invalid WireFormatHeader)
        let payload = vec![0u8, 0, 0];
        let owned_message = OwnedMessage::new(
            Some(payload),
            None,
            "t".into(),
            Timestamp::NotAvailable,
            0,
            0,
            None,
        );

        let result = consumer.handle_to_json(&owned_message).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Message trop court")
        );
    }
}
