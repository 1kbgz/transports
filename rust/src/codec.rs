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
