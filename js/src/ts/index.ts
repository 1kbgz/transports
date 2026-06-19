import * as wasm from "../../dist/pkg/transports";

export * as wasm from "../../dist/pkg/transports";

export const placeholder = "";

/** Diff two JSON-encoded models, returning the JSON-encoded patch. */
export const diff = (oldModel: string, newModel: string): string =>
  wasm.diff(oldModel, newModel);

/** Apply a JSON-encoded patch to a JSON-encoded model, returning the JSON-encoded result. */
export const apply = (model: string, patch: string): string =>
  wasm.apply(model, patch);

/** Encode a JSON-encoded model to codec bytes. */
export const encode = (model: string): Uint8Array => wasm.encode(model);

/** Decode codec bytes back to a JSON-encoded model string. */
export const decode = (bytes: Uint8Array): string => wasm.decode(bytes);

/** Encode a JSON-encoded model with the codec named by `codec` (e.g. "application/msgpack"). */
export const encodeAs = (model: string, codec: string): Uint8Array =>
  wasm.encode_as(model, codec);

/** Decode bytes (from `codec`'s codec) back to a JSON-encoded model string. */
export const decodeAs = (bytes: Uint8Array, codec: string): string =>
  wasm.decode_as(bytes, codec);

/** Convert an arbitrary JSON document to MessagePack bytes (for whole protocol messages). */
export const jsonToMsgpack = (json: string): Uint8Array =>
  wasm.json_to_msgpack(json);

/** Convert MessagePack bytes back to a JSON document. */
export const msgpackToJson = (bytes: Uint8Array): string =>
  wasm.msgpack_to_json(bytes);

/** In-process model store: host / mutate → patch / apply / snapshot. */
export const Store = wasm.Store;

// Plain JS object <-> core `Value` bridge (the JS analog of the Python bridge).
export { toValue, fromValue } from "./bridge";
export type { Value } from "./bridge";

// WebSocket client that mirrors a remote Session.
export { Client } from "./client";

// Custom wire codec registry.
export { registerCodec, unregisterCodec, codecFor } from "./codecs";
export type { CodecEncode, CodecDecode } from "./codecs";
