//! Wire codecs. [`Codec`] is the seam that makes the format pluggable; [`JsonCodec`] is the
//! bring-up implementation. Binary codecs (MessagePack, Protobuf, FlatBuffers) are Phase 2 and slot
//! in behind this same trait without touching the model or diff layers.

use crate::diff::Patch;
use crate::value::Value;

/// A decode failure (encode is infallible for the formats we support).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodecError(pub String);

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "codec error: {}", self.0)
    }
}

impl std::error::Error for CodecError {}

/// Encodes/decodes models and patches to/from bytes.
pub trait Codec {
    fn encode_value(&self, value: &Value) -> Vec<u8>;
    fn decode_value(&self, bytes: &[u8]) -> Result<Value, CodecError>;
    fn encode_patch(&self, patch: &Patch) -> Vec<u8>;
    fn decode_patch(&self, bytes: &[u8]) -> Result<Patch, CodecError>;
    /// A stable tag negotiated between peers (carried in the [`crate::Frame`]).
    fn content_type(&self) -> &'static str;
}

/// JSON via `serde_json`. Deterministic (maps are `BTreeMap`) so encodings are byte-stable.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn encode_value(&self, value: &Value) -> Vec<u8> {
        serde_json::to_vec(value).expect("Value serializes")
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<Value, CodecError> {
        serde_json::from_slice(bytes).map_err(|e| CodecError(e.to_string()))
    }

    fn encode_patch(&self, patch: &Patch) -> Vec<u8> {
        serde_json::to_vec(patch).expect("Patch serializes")
    }

    fn decode_patch(&self, bytes: &[u8]) -> Result<Patch, CodecError> {
        serde_json::from_slice(bytes).map_err(|e| CodecError(e.to_string()))
    }

    fn content_type(&self) -> &'static str {
        "application/json"
    }
}

/// MessagePack via `rmp-serde`. Compact binary, and self-describing — like JSON it needs no schema,
/// so it works with the dynamic [`Value`] as a drop-in for `JsonCodec`.
#[derive(Clone, Copy, Debug, Default)]
pub struct MsgpackCodec;

impl Codec for MsgpackCodec {
    fn encode_value(&self, value: &Value) -> Vec<u8> {
        rmp_serde::to_vec(value).expect("Value serializes")
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<Value, CodecError> {
        rmp_serde::from_slice(bytes).map_err(|e| CodecError(e.to_string()))
    }

    fn encode_patch(&self, patch: &Patch) -> Vec<u8> {
        rmp_serde::to_vec(patch).expect("Patch serializes")
    }

    fn decode_patch(&self, bytes: &[u8]) -> Result<Patch, CodecError> {
        rmp_serde::from_slice(bytes).map_err(|e| CodecError(e.to_string()))
    }

    fn content_type(&self) -> &'static str {
        "application/msgpack"
    }
}

/// Look up a codec by its content-type tag — the seam codec negotiation (Phase 2.1) builds on.
pub fn codec_for(content_type: &str) -> Option<Box<dyn Codec>> {
    match content_type {
        "application/json" => Some(Box::new(JsonCodec)),
        "application/msgpack" | "application/x-msgpack" => Some(Box::new(MsgpackCodec)),
        _ => None,
    }
}

/*********************************/
#[cfg(test)]
mod codec_tests {
    use super::*;

    #[test]
    fn test_value_round_trip() {
        let c = JsonCodec;
        let v = Value::map([("on", Value::from(true)), ("n", Value::from(3i64))]);
        let bytes = c.encode_value(&v);
        assert_eq!(c.decode_value(&bytes).unwrap(), v);
        assert_eq!(c.content_type(), "application/json");
    }

    #[test]
    fn test_msgpack_round_trip_and_smaller() {
        let c = MsgpackCodec;
        let v = Value::map([
            ("name", Value::from("lamp")),
            ("on", Value::from(true)),
            ("count", Value::from(123456i64)),
        ]);
        let mp = c.encode_value(&v);
        assert_eq!(c.decode_value(&mp).unwrap(), v);
        assert_eq!(c.content_type(), "application/msgpack");
        // self-describing binary, but more compact than JSON
        assert!(mp.len() < JsonCodec.encode_value(&v).len());
    }

    #[test]
    fn test_msgpack_patch_round_trip() {
        let c = MsgpackCodec;
        let p = crate::diff(
            &Value::map([("on", Value::from(false))]),
            &Value::map([("on", Value::from(true))]),
        );
        assert_eq!(c.decode_patch(&c.encode_patch(&p)).unwrap(), p);
    }

    #[test]
    fn test_codec_for() {
        assert_eq!(
            codec_for("application/json").unwrap().content_type(),
            "application/json"
        );
        assert_eq!(
            codec_for("application/msgpack").unwrap().content_type(),
            "application/msgpack"
        );
        assert!(codec_for("application/x-msgpack").is_some());
        assert!(codec_for("application/protobuf").is_none());
    }

    #[test]
    fn test_patch_round_trip() {
        let c = JsonCodec;
        let p = crate::diff(
            &Value::map([("on", Value::from(false))]),
            &Value::map([("on", Value::from(true))]),
        );
        let bytes = c.encode_patch(&p);
        assert_eq!(c.decode_patch(&bytes).unwrap(), p);
    }

    #[test]
    fn test_decode_error() {
        assert!(JsonCodec.decode_value(b"{bad").is_err());
    }
}
