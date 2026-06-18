//! transports core — the Rust marshalling engine.
//!
//! This crate is the single source of truth for the model representation, the diff/patch engine,
//! the codecs, and the wire envelope; it compiles into the PyO3 (`rust/python`) and wasm (`js`)
//! bindings so Python and JavaScript share one implementation. See `transports/ROADMAP.md`, Phase 0.
//!
//! Layers (bottom-up):
//! - [`value`] — the typed [`Value`] a model is made of, with [`ModelId`] submodel references.
//! - [`schema`] — [`Schema`]/[`Registry`]: type-name → schema (ports the prototype's `model_map`).
//! - [`diff`] — structural diff/patch with `rev` sequencing (the missing `onUpdate`).
//! - [`codec`] — the pluggable [`Codec`] trait + [`JsonCodec`].
//! - [`frame`] — the length-prefixed, codec-tagged [`Frame`] envelope.
//! - [`store`] — a minimal [`Store`]: host / mutate → patch / apply / snapshot.
//! - [`bridge`] — the JSON string facade the bindings call.

mod bridge;
mod codec;
mod diff;
mod frame;
mod schema;
mod store;
mod value;

pub use bridge::{apply_json, decode_json, diff_json, encode_json, JsonStore};
pub use codec::{Codec, CodecError, JsonCodec};
pub use diff::{apply, diff, Op, Patch, Path, PathSeg};
pub use frame::{Frame, FrameError, FrameKind};
pub use schema::{Field, FieldType, Registry, Schema};
pub use store::Store;
pub use value::{ModelId, Value};
