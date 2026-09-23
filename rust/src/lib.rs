//! transports core — the Rust marshalling engine.
//!
//! This crate is the single source of truth for the model representation, the diff/patch engine,
//! the codecs, and the wire envelope; it compiles into the PyO3 (`rust/python`) and wasm (`js`)
//! bindings so Python and JavaScript share one implementation.
//!
//! Layers (bottom-up):
//! - [`value`] — the typed [`Value`] a model is made of, with [`ModelId`] submodel references.
//! - [`schema`] — [`Schema`]/[`Registry`]: type-name → schema (ports the prototype's `model_map`).
//! - [`diff`] — structural diff/patch with `rev` sequencing (the missing `onUpdate`).
//! - [`codec`] — the pluggable [`Codec`] trait + [`JsonCodec`]/[`MsgpackCodec`].
//! - [`frame`] — the length-prefixed, codec-tagged [`Frame`] envelope.
//! - [`store`] — a minimal [`Store`]: host / mutate → patch / apply / snapshot.
//! - [`bridge`] — the JSON string facade the bindings call.

mod bridge;
mod client;
mod codec;
mod crdt;
mod diff;
mod frame;
mod message;
mod schema;
mod store;
mod value;

pub use bridge::{
    apply_json, cbor_to_json, crdt_spec_hash_json, decode_as, decode_json, decode_message,
    diff_json, encode_as, encode_json, encode_message, json_to_cbor, json_to_msgpack,
    msgpack_to_json, normalize_crdt_spec_json, normalize_message_json, require_crdt_spec_hash_json,
    JsonCrdtDocument, JsonStore,
};
pub use client::{ClientEffect, ClientState};
pub use codec::{codec_for, CborCodec, Codec, CodecError, JsonCodec, MsgpackCodec};
pub use crdt::{
    CausalContext, CrdtChange, CrdtDelta, CrdtDocument, CrdtEffect, CrdtMutation, CrdtOp, CrdtPath,
    CrdtPathSegment, CrdtPolicy, CrdtSpec, CrdtState, Dot, ElementId, SequenceDeltaElement,
    SequenceMaterialization, VersionVector,
};
pub use diff::{apply, diff, Op, Patch, Path, PathSeg};
pub use frame::{Frame, FrameError, FrameKind};
pub use message::Message;
pub use schema::{Field, FieldType, Registry, Schema};
pub use store::Store;
pub use value::{ModelId, Value};
