//! Binary attachments for transports whose control channel is JSON.

use ciborium::Value;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::Message;

#[derive(Debug, Serialize, Deserialize)]
struct Header {
    message: serde_json::Value,
    buffer_paths: Vec<Vec<serde_json::Value>>,
}

/// A JSON control frame and native buffers, in the order referenced by `buffer_paths`.
pub struct BufferFrame {
    pub header: String,
    pub buffers: Vec<Vec<u8>>,
}

impl BufferFrame {
    pub fn from_message(message: &Message) -> Result<Self, String> {
        let value = message.binary_value()?;
        let mut buffers = Vec::new();
        let mut paths = Vec::new();
        let message = detach(
            value,
            &mut Vec::new(),
            &mut paths,
            &mut buffers,
            &mut BTreeMap::new(),
        )?;
        let header = serde_json::to_string(&Header {
            message,
            buffer_paths: paths,
        })
        .map_err(|e| e.to_string())?;
        Ok(Self { header, buffers })
    }

    pub fn message(&self) -> Result<Message, String> {
        let header: Header = serde_json::from_str(&self.header).map_err(|e| e.to_string())?;
        if header.buffer_paths.len() < self.buffers.len()
            || header.buffer_paths.is_empty() != self.buffers.is_empty()
        {
            return Err("buffer count does not match buffer_paths".into());
        }
        let mut value = Value::serialized(&header.message).map_err(|e| e.to_string())?;
        let mut referenced = BTreeSet::new();
        for path in &header.buffer_paths {
            let target = at_path(&mut value, path)?;
            let index = match target {
                Value::Map(entries)
                    if entries.len() == 1 && entries[0].0.as_text() == Some("buffer") =>
                {
                    entries[0]
                        .1
                        .as_integer()
                        .and_then(|index| usize::try_from(index).ok())
                }
                _ => None,
            }
            .ok_or("invalid or repeated buffer placeholder")?;
            let buffer = self
                .buffers
                .get(index)
                .ok_or("buffer index out of bounds")?;
            referenced.insert(index);
            *target = Value::Bytes(buffer.clone());
        }
        if referenced.len() != self.buffers.len() {
            return Err("unreferenced buffers".into());
        }
        value.deserialized().map_err(|e| e.to_string())
    }
}

fn at_path<'a>(
    mut value: &'a mut Value,
    path: &[serde_json::Value],
) -> Result<&'a mut Value, String> {
    for segment in path {
        value = match (value, segment) {
            (Value::Map(entries), serde_json::Value::String(key)) => entries
                .iter_mut()
                .find_map(|(k, v)| (k.as_text() == Some(key.as_str())).then_some(v))
                .ok_or("buffer path key not found")?,
            (Value::Array(values), serde_json::Value::Number(index)) => values
                .get_mut(
                    index
                        .as_u64()
                        .and_then(|n| usize::try_from(n).ok())
                        .ok_or("invalid buffer index")?,
                )
                .ok_or("buffer path index out of bounds")?,
            _ => return Err("invalid buffer path".into()),
        };
    }
    Ok(value)
}

fn detach(
    value: Value,
    path: &mut Vec<serde_json::Value>,
    paths: &mut Vec<Vec<serde_json::Value>>,
    buffers: &mut Vec<Vec<u8>>,
    hashes: &mut BTreeMap<[u8; 32], usize>,
) -> Result<serde_json::Value, String> {
    match value {
        Value::Bytes(bytes) => {
            let hash: [u8; 32] = Sha256::digest(&bytes).into();
            let index = *hashes.entry(hash).or_insert_with(|| {
                let index = buffers.len();
                buffers.push(bytes);
                index
            });
            paths.push(path.clone());
            Ok(serde_json::json!({"buffer": index}))
        }
        Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for (index, value) in values.into_iter().enumerate() {
                path.push(index.into());
                out.push(detach(value, path, paths, buffers, hashes)?);
                path.pop();
            }
            Ok(out.into())
        }
        Value::Map(values) => {
            let mut out = serde_json::Map::new();
            for (key, value) in values {
                let key = key
                    .into_text()
                    .map_err(|_| "buffer frame requires string map keys")?;
                path.push(key.clone().into());
                out.insert(key, detach(value, path, paths, buffers, hashes)?);
                path.pop();
            }
            Ok(out.into())
        }
        other => other.deserialized().map_err(|e| e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{apply, diff, CborCodec, Codec, JsonCodec, MsgpackCodec, Op};

    #[test]
    fn bytes_are_atomic_and_codecs_use_native_binary() {
        let old = crate::Value::Bytes(vec![0, 255, 1]);
        let new = crate::Value::Bytes(vec![255, 0]);
        let patch = diff(&old, &new);
        assert_eq!(
            patch.ops,
            vec![Op::Set {
                path: vec![],
                value: new.clone()
            }]
        );
        let mut changed = old.clone();
        apply(&mut changed, &patch).unwrap();
        assert_eq!(changed, new);
        assert_eq!(JsonCodec.encode_value(&old), br#"{"Bytes":"AP8B"}"#);
        assert_eq!(
            MsgpackCodec.encode_value(&old),
            vec![0x81, 0xa5, b'B', b'y', b't', b'e', b's', 0xc4, 3, 0, 255, 1]
        );
        assert_eq!(
            CborCodec.encode_value(&old),
            vec![0xa1, 0x65, b'B', b'y', b't', b'e', b's', 0x43, 0, 255, 1]
        );
        for codec in [&JsonCodec as &dyn Codec, &MsgpackCodec, &CborCodec] {
            assert_eq!(codec.decode_value(&codec.encode_value(&old)).unwrap(), old);
            assert_eq!(
                codec.decode_patch(&codec.encode_patch(&patch)).unwrap(),
                patch
            );
        }
        assert!(JsonCodec.decode_value(br#"{"Bytes":"bad!"}"#).is_err());
        assert!(JsonCodec.decode_value(br#"{"Bytes":[1,2]}"#).is_err());
    }

    #[test]
    fn attachments_round_trip_large_bytes_without_json_payloads() {
        let message = Message::Snapshot {
            id: 1,
            model_type: "Sample".into(),
            rev: 0,
            value: crate::Value::map([("data", crate::Value::Bytes(vec![255; 10 * 1024 * 1024]))]),
        };
        for codec in ["json", "msgpack", "cbor"] {
            assert_eq!(
                Message::decode(&message.encode(codec).unwrap(), codec).unwrap(),
                message
            );
        }
        let frame = BufferFrame::from_message(&message).unwrap();
        assert!(frame.header.len() < 300);
        assert_eq!(frame.buffers.len(), 1);
        assert_eq!(frame.buffers[0].len(), 10 * 1024 * 1024);
        assert_eq!(frame.message().unwrap(), message);
        assert!(BufferFrame {
            header: frame.header.clone(),
            buffers: vec![]
        }
        .message()
        .is_err());
    }
}
