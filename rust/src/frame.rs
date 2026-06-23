//! The wire envelope.
//!
//! A [`Frame`] wraps one codec-encoded payload with the metadata a peer needs to route and apply
//! it: which codec produced the payload, the `model_type` (so a first-contact receiver can register
//! the schema), the target [`ModelId`], the `rev` it advances to, and whether the payload is a full
//! snapshot or an incremental patch.
//!
//! [`Frame::encode`] produces a self-delimiting, length-prefixed byte string so frames can be
//! streamed back-to-back over a byte transport (TCP, a WebSocket binary message, …); [`Frame::decode`]
//! reads one frame and returns the remaining bytes. The header is itself JSON for now; richer
//! connections build on this, and binary codecs only change the *payload*, not the framing.

use serde::{Deserialize, Serialize};

use crate::value::ModelId;

/// Whether a frame's payload is a complete model or an incremental patch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameKind {
    Snapshot,
    Patch,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Header {
    codec: String,
    model_type: String,
    target: ModelId,
    rev: u64,
    kind: FrameKind,
}

/// One routable, codec-tagged unit on the wire.
#[derive(Clone, Debug, PartialEq)]
pub struct Frame {
    /// Codec content-type that produced `payload` (e.g. `"application/json"`).
    pub codec: String,
    /// Registered model type name, for first-contact schema registration.
    pub model_type: String,
    pub target: ModelId,
    pub rev: u64,
    pub kind: FrameKind,
    pub payload: Vec<u8>,
}

impl Frame {
    /// Encode to length-prefixed bytes: `[u32 header_len][header json][u32 payload_len][payload]`.
    pub fn encode(&self) -> Vec<u8> {
        let header = Header {
            codec: self.codec.clone(),
            model_type: self.model_type.clone(),
            target: self.target,
            rev: self.rev,
            kind: self.kind,
        };
        let header_bytes = serde_json::to_vec(&header).expect("header serializes");
        let mut out = Vec::with_capacity(8 + header_bytes.len() + self.payload.len());
        out.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(&header_bytes);
        out.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    /// Decode one frame, returning it and the unconsumed remainder of `bytes`.
    pub fn decode(bytes: &[u8]) -> Result<(Frame, &[u8]), FrameError> {
        let header_len = read_u32(bytes, 0)? as usize;
        let header_start = 4usize;
        let header_end = header_start
            .checked_add(header_len)
            .ok_or(FrameError::Truncated)?;
        let header_bytes = bytes
            .get(header_start..header_end)
            .ok_or(FrameError::Truncated)?;
        let header: Header = serde_json::from_slice(header_bytes)
            .map_err(|e| FrameError::BadHeader(e.to_string()))?;

        let payload_len = read_u32(bytes, header_end)? as usize;
        let payload_start = header_end + 4;
        let payload_end = payload_start
            .checked_add(payload_len)
            .ok_or(FrameError::Truncated)?;
        let payload = bytes
            .get(payload_start..payload_end)
            .ok_or(FrameError::Truncated)?
            .to_vec();

        let frame = Frame {
            codec: header.codec,
            model_type: header.model_type,
            target: header.target,
            rev: header.rev,
            kind: header.kind,
            payload,
        };
        Ok((frame, &bytes[payload_end..]))
    }
}

/// A framing decode failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameError {
    Truncated,
    BadHeader(String),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Truncated => write!(f, "truncated frame"),
            FrameError::BadHeader(e) => write!(f, "bad frame header: {e}"),
        }
    }
}

impl std::error::Error for FrameError {}

fn read_u32(bytes: &[u8], at: usize) -> Result<u32, FrameError> {
    let slice = bytes.get(at..at + 4).ok_or(FrameError::Truncated)?;
    Ok(u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

#[cfg(test)]
mod frame_tests {
    use super::*;

    fn sample(rev: u64, payload: &[u8]) -> Frame {
        Frame {
            codec: "application/json".into(),
            model_type: "Device".into(),
            target: ModelId(7),
            rev,
            kind: FrameKind::Patch,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn test_round_trip() {
        let f = sample(3, br#"{"ops":[]}"#);
        let bytes = f.encode();
        let (got, rest) = Frame::decode(&bytes).unwrap();
        assert_eq!(got, f);
        assert!(rest.is_empty());
    }

    #[test]
    fn test_back_to_back_streaming() {
        let mut buf = sample(1, b"aaa").encode();
        buf.extend(sample(2, b"bbbb").encode());
        let (f1, rest) = Frame::decode(&buf).unwrap();
        let (f2, rest) = Frame::decode(rest).unwrap();
        assert_eq!(f1.rev, 1);
        assert_eq!(f2.rev, 2);
        assert_eq!(f2.payload, b"bbbb");
        assert!(rest.is_empty());
    }

    #[test]
    fn test_truncated() {
        let bytes = sample(1, b"xy").encode();
        assert_eq!(Frame::decode(&bytes[..3]), Err(FrameError::Truncated));
    }
}
