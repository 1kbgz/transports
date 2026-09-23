import * as wasm from "../../dist/pkg/transports";

import { fromValue, toValue, type Value } from "./bridge";

export * as wasm from "../../dist/pkg/transports";

export const placeholder = "";

/** Diff two JSON-encoded models, returning the JSON-encoded patch. */
export const diff = (oldModel: string, newModel: string): string =>
  wasm.diff(oldModel, newModel);

/** Apply a JSON-encoded patch to a JSON-encoded model, returning the JSON-encoded result. */
export const apply = (model: string, patch: string): string =>
  wasm.apply(model, patch);

export type RegisterPolicy = { kind: "register" };
export type MapPolicy = {
  kind: "map";
  fields?: Record<string, CrdtPolicy>;
  values?: CrdtPolicy;
};
export type SetPolicy = {
  kind: "set";
  keys?: string[][];
  element?: CrdtPolicy;
};
export type SequencePolicy = {
  kind: "sequence";
  materialization?: "list" | "string";
  element?: CrdtPolicy;
};
export type CrdtPolicy =
  | RegisterPolicy
  | MapPolicy
  | SetPolicy
  | SequencePolicy;
export type CrdtSpecValue = { version?: number; root: CrdtPolicy };
export type CanonicalCrdtSpec = { version: number; root: CrdtPolicy };
export type Dot = { counter: number; replica: string };
export type ElementId = { dot: Dot; index: number };
export type CrdtPathSegment =
  | { kind: "key"; key: string }
  | { kind: "member"; key: string }
  | { kind: "element"; id: ElementId };
export type CrdtPath = CrdtPathSegment[];
export type CrdtMutation =
  | { kind: "register_set"; path: CrdtPath; value: unknown }
  | { kind: "map_set"; path: CrdtPath; key: string; value: unknown }
  | { kind: "map_remove"; path: CrdtPath; key: string }
  | { kind: "set_add"; path: CrdtPath; value: unknown }
  | { kind: "set_remove"; path: CrdtPath; key: string }
  | {
      kind: "sequence_insert";
      path: CrdtPath;
      after: ElementId | null;
      values: unknown[];
    }
  | { kind: "sequence_delete"; path: CrdtPath; ids: ElementId[] };
type Mutation<K extends CrdtMutation["kind"]> = Extract<
  CrdtMutation,
  { kind: K }
>;
type Operation<K extends CrdtMutation["kind"]> = Mutation<K> & { dot: Dot };
export type CrdtOp =
  | Operation<"register_set">
  | Operation<"map_set">
  | (Operation<"map_remove"> & { removed: Dot[] })
  | Operation<"set_add">
  | (Operation<"set_remove"> & { removed: Dot[] })
  | Operation<"sequence_insert">
  | Operation<"sequence_delete">;
export type CrdtDelta =
  | { kind: "register_set"; path: CrdtPath; value: unknown }
  | { kind: "map_set"; path: CrdtPath; key: string; value: unknown }
  | { kind: "map_remove"; path: CrdtPath; key: string }
  | { kind: "set_add"; path: CrdtPath; key: string; value: unknown }
  | { kind: "set_remove"; path: CrdtPath; key: string }
  | {
      kind: "sequence_insert";
      path: CrdtPath;
      after: ElementId | null;
      elements: { id: ElementId; value: unknown }[];
    }
  | { kind: "sequence_delete"; path: CrdtPath; ids: ElementId[] };
export type CrdtEffect = { patch: unknown; deltas: CrdtDelta[] };
export type CrdtChange = { ops: CrdtOp[]; effect: CrdtEffect };

/** Validated, canonical merge semantics for one model. */
export class CrdtSpec {
  private canonical!: string;

  constructor(root: CrdtPolicy, version?: number) {
    const value: CrdtSpecValue = { root };
    if (version !== undefined) value.version = version;
    this.canonical = wasm.normalize_crdt_spec(JSON.stringify(value));
  }

  private static fromCanonical(canonical: string): CrdtSpec {
    const spec = Object.create(CrdtSpec.prototype) as CrdtSpec;
    spec.canonical = canonical;
    return spec;
  }

  static fromObject(value: CrdtSpecValue): CrdtSpec {
    return CrdtSpec.fromCanonical(
      wasm.normalize_crdt_spec(JSON.stringify(value)),
    );
  }

  static fromJson(value: string): CrdtSpec {
    return CrdtSpec.fromCanonical(wasm.normalize_crdt_spec(value));
  }

  toObject(): CanonicalCrdtSpec {
    return JSON.parse(this.canonical);
  }

  toJson(): string {
    return this.canonical;
  }

  toJSON(): CanonicalCrdtSpec {
    return this.toObject();
  }

  get hash(): string {
    return wasm.crdt_spec_hash(this.canonical);
  }

  requireHash(peerHash: string): void {
    wasm.require_crdt_spec_hash(this.canonical, peerHash);
  }

  equals(other: CrdtSpec): boolean {
    return this.canonical === other.canonical;
  }
}

const wireItem = (
  item: CrdtMutation | CrdtOp | CrdtDelta,
  encode: boolean,
): Record<string, unknown> => {
  const converted = { ...item } as Record<string, unknown>;
  const transform = encode
    ? (value: unknown): Value => toValue(value)
    : (value: unknown): unknown => fromValue(value as Value);
  if (["register_set", "map_set", "set_add"].includes(item.kind)) {
    if (!("value" in item)) throw new TypeError(`${item.kind} requires value`);
    converted.value = transform(converted.value);
  } else if (item.kind === "sequence_insert") {
    if ("values" in item) converted.values = item.values.map(transform);
    else if ("elements" in item)
      converted.elements = item.elements.map((element) => ({
        ...element,
        value: transform(element.value),
      }));
    else throw new TypeError("sequence_insert requires values or elements");
  }
  return converted;
};

const publicEffect = (effect: CrdtEffect): CrdtEffect => ({
  ...effect,
  deltas: effect.deltas.map((delta) => wireItem(delta, false) as CrdtDelta),
});

/** One schema-directed CRDT replica backed by the shared Rust reducer. */
export class CrdtDocument {
  private inner: wasm.CrdtDocument;

  readonly spec: CrdtSpec;
  readonly replica: string;

  constructor(spec: CrdtSpec, value: unknown, replica: string) {
    this.spec = spec;
    this.replica = replica;
    this.inner = new wasm.CrdtDocument(
      spec.toJson(),
      JSON.stringify(toValue(value)),
      replica,
    );
  }

  private static wrap(
    spec: CrdtSpec,
    replica: string,
    inner: wasm.CrdtDocument,
  ): CrdtDocument {
    const document = Object.create(CrdtDocument.prototype) as CrdtDocument;
    Object.defineProperties(document, {
      spec: { value: spec, enumerable: true },
      replica: { value: replica, enumerable: true },
      inner: { value: inner, writable: true },
    });
    return document;
  }

  static fromState(
    spec: CrdtSpec,
    state: Record<string, unknown>,
    replica: string,
  ): CrdtDocument {
    return CrdtDocument.wrap(
      spec,
      replica,
      wasm.CrdtDocument.from_state(
        spec.toJson(),
        JSON.stringify(state),
        replica,
      ),
    );
  }

  get value(): unknown {
    return fromValue(JSON.parse(this.inner.value()) as Value);
  }

  get state(): Record<string, unknown> {
    return JSON.parse(this.inner.state());
  }

  mutate(mutations: CrdtMutation[]): CrdtChange {
    const wire = mutations.map((mutation) => wireItem(mutation, true));
    const change = JSON.parse(
      this.inner.mutate(JSON.stringify(wire)),
    ) as CrdtChange;
    return {
      ops: change.ops.map((op) => wireItem(op, false) as CrdtOp),
      effect: publicEffect(change.effect),
    };
  }

  apply(ops: CrdtOp[]): CrdtEffect {
    const wire = ops.map((op) => wireItem(op, true));
    return publicEffect(
      JSON.parse(this.inner.apply(JSON.stringify(wire))) as CrdtEffect,
    );
  }

  memberKey(path: CrdtPath, value: unknown): string {
    return this.inner.member_key(
      JSON.stringify(path),
      JSON.stringify(toValue(value)),
    );
  }

  compact(frontier: Record<string, number>): number {
    return this.inner.compact(JSON.stringify(frontier));
  }
}

const crdtSpecJson = (spec: CrdtSpecValue | CrdtSpec): string =>
  spec instanceof CrdtSpec ? spec.toJson() : JSON.stringify(spec);

/** Validate and return the canonical representation of a CRDT specification. */
export const normalizeCrdtSpec = (
  spec: CrdtSpecValue | CrdtSpec,
): CanonicalCrdtSpec =>
  JSON.parse(wasm.normalize_crdt_spec(crdtSpecJson(spec)));

/** SHA-256 of the canonical CRDT specification. */
export const crdtSpecHash = (spec: CrdtSpecValue | CrdtSpec): string =>
  wasm.crdt_spec_hash(crdtSpecJson(spec));

/** Throw when a peer uses different merge semantics. */
export const requireCrdtSpecHash = (
  spec: CrdtSpecValue | CrdtSpec,
  peerHash: string,
): void => wasm.require_crdt_spec_hash(crdtSpecJson(spec), peerHash);

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

/** Convert an arbitrary JSON document to CBOR bytes (for whole protocol messages). */
export const jsonToCbor = (json: string): Uint8Array => wasm.json_to_cbor(json);

/** Convert CBOR bytes back to a JSON document. */
export const cborToJson = (bytes: Uint8Array): string =>
  wasm.cbor_to_json(bytes);

/** Parse and serialize one typed live protocol message as compact JSON. */
export const normalizeMessage = (json: string): string =>
  wasm.normalize_message(json);

/** Encode one JSON live protocol message with a built-in connection codec. */
export const encodeMessage = (json: string, codec: string): Uint8Array =>
  wasm.encode_message(json, codec);

/** Decode one built-in connection-codec payload as typed live protocol message JSON. */
export const decodeMessage = (bytes: Uint8Array, codec: string): string =>
  wasm.decode_message(bytes, codec);

/** In-process model store: host / mutate → patch / apply / snapshot. */
export const Store = wasm.Store;

/** Shared revision and proposal reducer used by `Client`. */
export const ClientState = wasm.ClientState;

// Plain JS object <-> core `Value` bridge (the JS analog of the Python bridge).
export { toValue, fromValue } from "./bridge";
export type { Value } from "./bridge";

// WebSocket client that mirrors a remote Session.
export { Client } from "./client";
export type {
  AckMsg,
  ModelPatch,
  PatchMsg,
  PatchOp,
  PathSeg,
  ReceiveChange,
  RejectMsg,
} from "./client";

// Custom wire codec registry.
export { registerCodec, unregisterCodec, codecFor } from "./codecs";
export type { CodecEncode, CodecDecode } from "./codecs";
