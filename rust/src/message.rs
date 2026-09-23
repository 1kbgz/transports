//! Typed messages used by the live connection protocol.
//!
//! WebSocket, SSE, comm, and anywidget transports already delimit messages, so they do not use the
//! length-prefixed [`crate::Frame`] byte-stream envelope. They still share this model for routing
//! metadata and payload semantics across Python and JavaScript bindings.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Patch, Value};

/// One live protocol message.
#[derive(Clone, Debug, PartialEq)]
pub enum Message {
    Snapshot {
        id: u64,
        model_type: String,
        rev: u64,
        value: Value,
    },
    CrdtSnapshot {
        id: u64,
        model_type: String,
        rev: u64,
        value: Value,
        spec: serde_json::Value,
        state: serde_json::Value,
    },
    Patch {
        id: u64,
        patch: Patch,
        proposal: Option<String>,
    },
    Crdt {
        id: u64,
        rev: u64,
        ops: Vec<serde_json::Value>,
        effect: Option<serde_json::Value>,
        proposal: Option<String>,
    },
    Ack {
        id: u64,
        rev: u64,
        proposal: String,
    },
    Reject {
        id: u64,
        rev: u64,
        error: String,
        proposal: Option<String>,
        crdt_ops: Option<Vec<serde_json::Value>>,
    },
    Batch {
        msgs: Vec<Message>,
    },
    /// A message kind introduced by a newer peer. Clients ignore it for forward compatibility.
    Unknown(serde_json::Value),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "t")]
enum KnownMessage {
    #[serde(rename = "snapshot")]
    Snapshot {
        id: u64,
        #[serde(rename = "type")]
        model_type: String,
        rev: u64,
        value: Value,
    },
    #[serde(rename = "crdt_snapshot")]
    CrdtSnapshot {
        id: u64,
        #[serde(rename = "type")]
        model_type: String,
        rev: u64,
        value: Value,
        spec: serde_json::Value,
        state: serde_json::Value,
    },
    #[serde(rename = "patch")]
    Patch {
        id: u64,
        patch: Patch,
        #[serde(skip_serializing_if = "Option::is_none")]
        proposal: Option<String>,
    },
    #[serde(rename = "crdt")]
    Crdt {
        id: u64,
        rev: u64,
        ops: Vec<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        effect: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        proposal: Option<String>,
    },
    #[serde(rename = "ack")]
    Ack { id: u64, rev: u64, proposal: String },
    #[serde(rename = "reject")]
    Reject {
        id: u64,
        rev: u64,
        error: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        proposal: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        crdt_ops: Option<Vec<serde_json::Value>>,
    },
    #[serde(rename = "batch")]
    Batch { msgs: Vec<Message> },
}

impl From<KnownMessage> for Message {
    fn from(message: KnownMessage) -> Self {
        match message {
            KnownMessage::Snapshot {
                id,
                model_type,
                rev,
                value,
            } => Message::Snapshot {
                id,
                model_type,
                rev,
                value,
            },
            KnownMessage::CrdtSnapshot {
                id,
                model_type,
                rev,
                value,
                spec,
                state,
            } => Message::CrdtSnapshot {
                id,
                model_type,
                rev,
                value,
                spec,
                state,
            },
            KnownMessage::Patch {
                id,
                patch,
                proposal,
            } => Message::Patch {
                id,
                patch,
                proposal,
            },
            KnownMessage::Crdt {
                id,
                rev,
                ops,
                effect,
                proposal,
            } => Message::Crdt {
                id,
                rev,
                ops,
                effect,
                proposal,
            },
            KnownMessage::Ack { id, rev, proposal } => Message::Ack { id, rev, proposal },
            KnownMessage::Reject {
                id,
                rev,
                error,
                proposal,
                crdt_ops,
            } => Message::Reject {
                id,
                rev,
                error,
                proposal,
                crdt_ops,
            },
            KnownMessage::Batch { msgs } => Message::Batch { msgs },
        }
    }
}

impl From<&Message> for KnownMessage {
    fn from(message: &Message) -> Self {
        match message {
            Message::Snapshot {
                id,
                model_type,
                rev,
                value,
            } => KnownMessage::Snapshot {
                id: *id,
                model_type: model_type.clone(),
                rev: *rev,
                value: value.clone(),
            },
            Message::CrdtSnapshot {
                id,
                model_type,
                rev,
                value,
                spec,
                state,
            } => KnownMessage::CrdtSnapshot {
                id: *id,
                model_type: model_type.clone(),
                rev: *rev,
                value: value.clone(),
                spec: spec.clone(),
                state: state.clone(),
            },
            Message::Patch {
                id,
                patch,
                proposal,
            } => KnownMessage::Patch {
                id: *id,
                patch: patch.clone(),
                proposal: proposal.clone(),
            },
            Message::Crdt {
                id,
                rev,
                ops,
                effect,
                proposal,
            } => KnownMessage::Crdt {
                id: *id,
                rev: *rev,
                ops: ops.clone(),
                effect: effect.clone(),
                proposal: proposal.clone(),
            },
            Message::Ack { id, rev, proposal } => KnownMessage::Ack {
                id: *id,
                rev: *rev,
                proposal: proposal.clone(),
            },
            Message::Reject {
                id,
                rev,
                error,
                proposal,
                crdt_ops,
            } => KnownMessage::Reject {
                id: *id,
                rev: *rev,
                error: error.clone(),
                proposal: proposal.clone(),
                crdt_ops: crdt_ops.clone(),
            },
            Message::Batch { msgs } => KnownMessage::Batch { msgs: msgs.clone() },
            Message::Unknown(_) => {
                unreachable!("unknown messages serialize through their raw value")
            }
        }
    }
}

impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Message::Unknown(value) => value.serialize(serializer),
            known => KnownMessage::from(known).serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let kind = value.get("t").and_then(serde_json::Value::as_str);
        match kind {
            Some("snapshot" | "crdt_snapshot" | "patch" | "crdt" | "ack" | "reject" | "batch") => {
                serde_json::from_value::<KnownMessage>(value)
                    .map(Message::from)
                    .map_err(serde::de::Error::custom)
            }
            _ => Ok(Message::Unknown(value)),
        }
    }
}

impl Message {
    /// Parse one JSON live message.
    pub fn from_json(json: &str) -> Result<Message, String> {
        serde_json::from_str(json).map_err(|error| error.to_string())
    }

    /// Serialize one live message as canonical compact JSON.
    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string(self).map_err(|error| error.to_string())
    }

    /// Encode this message with a built-in connection codec.
    pub fn encode(&self, codec: &str) -> Result<Vec<u8>, String> {
        match normalize_codec(codec)? {
            BuiltinCodec::Json => serde_json::to_vec(self).map_err(|error| error.to_string()),
            BuiltinCodec::Msgpack => {
                let value = serde_json::to_value(self).map_err(|error| error.to_string())?;
                rmp_serde::to_vec_named(&value).map_err(|error| error.to_string())
            }
            BuiltinCodec::Cbor => {
                let value = serde_json::to_value(self).map_err(|error| error.to_string())?;
                let mut bytes = Vec::new();
                ciborium::into_writer(&value, &mut bytes).map_err(|error| error.to_string())?;
                Ok(bytes)
            }
        }
    }

    /// Decode one message produced by a built-in connection codec.
    pub fn decode(bytes: &[u8], codec: &str) -> Result<Message, String> {
        match normalize_codec(codec)? {
            BuiltinCodec::Json => serde_json::from_slice(bytes).map_err(|error| error.to_string()),
            BuiltinCodec::Msgpack => {
                rmp_serde::from_slice(bytes).map_err(|error| error.to_string())
            }
            BuiltinCodec::Cbor => ciborium::from_reader(bytes).map_err(|error| error.to_string()),
        }
    }
}

enum BuiltinCodec {
    Json,
    Msgpack,
    Cbor,
}

fn normalize_codec(codec: &str) -> Result<BuiltinCodec, String> {
    match codec {
        "" | "json" | "application/json" => Ok(BuiltinCodec::Json),
        "msgpack" | "application/msgpack" | "application/x-msgpack" | "x-msgpack" => {
            Ok(BuiltinCodec::Msgpack)
        }
        "cbor" | "application/cbor" => Ok(BuiltinCodec::Cbor),
        _ => Err(format!("unknown codec: {codec}")),
    }
}

#[cfg(test)]
mod message_tests {
    use super::*;
    use crate::{Op, PathSeg};

    fn messages() -> Vec<Message> {
        vec![
            Message::Snapshot {
                id: 7,
                model_type: "Counter".into(),
                rev: 3,
                value: Value::map([("count", Value::Int(4))]),
            },
            Message::CrdtSnapshot {
                id: 8,
                model_type: "Document".into(),
                rev: 2,
                value: Value::Str("hello".into()),
                spec: serde_json::json!({
                    "version": 1,
                    "root": {"kind": "sequence", "materialization": "string"}
                }),
                state: serde_json::json!({"version": 1, "spec_hash": "abc"}),
            },
            Message::Patch {
                id: 7,
                patch: Patch {
                    rev: 4,
                    ops: vec![Op::Set {
                        path: vec![PathSeg::Key("count".into())],
                        value: Value::Int(5),
                    }],
                },
                proposal: Some("editor-4".into()),
            },
            Message::Crdt {
                id: 8,
                rev: 3,
                ops: vec![serde_json::json!({
                    "kind": "sequence_insert",
                    "path": [],
                    "after": null,
                    "values": ["!"] ,
                    "dot": {"counter": 1, "replica": "editor"}
                })],
                effect: Some(serde_json::json!({"patch": {"rev": 0, "ops": []}, "deltas": []})),
                proposal: None,
            },
            Message::Ack {
                id: 7,
                rev: 4,
                proposal: "editor-5".into(),
            },
            Message::Reject {
                id: 7,
                rev: 4,
                error: "count must be positive".into(),
                proposal: Some("editor-6".into()),
                crdt_ops: None,
            },
        ]
    }

    #[test]
    fn test_json_shape_matches_the_live_protocol() {
        let message = Message::Patch {
            id: 2,
            patch: Patch {
                rev: 9,
                ops: vec![],
            },
            proposal: None,
        };
        assert_eq!(
            message.to_json().unwrap(),
            r#"{"t":"patch","id":2,"patch":{"rev":9,"ops":[]}}"#
        );
    }

    #[test]
    fn test_every_message_round_trips_through_every_builtin_codec() {
        let mut messages = messages();
        messages.push(Message::Batch {
            msgs: messages.clone(),
        });
        for message in messages {
            for codec in ["json", "msgpack", "cbor"] {
                assert_eq!(
                    Message::decode(&message.encode(codec).unwrap(), codec).unwrap(),
                    message
                );
            }
        }
    }

    #[test]
    fn test_unknown_message_kind_is_forward_compatible() {
        let json = r#"{"t":"presence","id":7,"user":"Ada"}"#;
        let message = Message::from_json(json).unwrap();
        assert!(matches!(message, Message::Unknown(_)));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&message.to_json().unwrap()).unwrap(),
            serde_json::from_str::<serde_json::Value>(json).unwrap()
        );
    }

    #[test]
    fn test_missing_required_fields_are_rejected() {
        assert!(Message::from_json(r#"{"t":"ack","id":7,"rev":3}"#).is_err());
        assert!(Message::from_json(r#"{"t":"patch","id":7}"#).is_err());
    }
}
