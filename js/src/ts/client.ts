import { codecFor } from "./codecs";
import {
  cborToJson,
  diff,
  jsonToCbor,
  jsonToMsgpack,
  msgpackToJson,
} from "./index";
import type { Value } from "./bridge";

type SnapshotMsg = {
  t: "snapshot";
  id: number;
  type: string;
  rev: number;
  value: unknown;
};
export type PathSeg = { Key: string } | { Index: number };
export type PatchOp =
  | { Set: { path: PathSeg[]; value: Value } }
  | { Remove: { path: PathSeg[] } }
  | { Insert: { path: PathSeg[]; index: number; value: Value } }
  | { RemoveAt: { path: PathSeg[]; index: number } }
  | { Move: { path: PathSeg[]; from: number; to: number } }
  | { Reorder: { path: PathSeg[]; order: number[] } };
export type ModelPatch = { rev: number; ops: PatchOp[] };
type PatchMsg = {
  t: "patch";
  id: number;
  patch: ModelPatch;
};
/** The server refused a proposed edit; `rev` is its current revision and `error` says why (the
 * model's validation message). Sent to the proposer only, after the authoritative revert. */
export type RejectMsg = { t: "reject"; id: number; rev: number; error: string };
/** Frame metadata returned after the mirror accepts a snapshot or patch. */
export type ReceiveChange =
  | { t: "snapshot"; id: number; rev: number }
  | PatchMsg;

function mapValue(value: Value | undefined): Record<string, Value> {
  if (
    value &&
    typeof value === "object" &&
    "Map" in value &&
    value.Map &&
    typeof value.Map === "object" &&
    !Array.isArray(value.Map)
  )
    return value.Map;
  throw new Error("patch path expected a map");
}

function listValue(value: Value | undefined): Value[] {
  if (
    value &&
    typeof value === "object" &&
    "List" in value &&
    Array.isArray(value.List)
  )
    return value.List;
  throw new Error("patch path expected a list");
}

function validIndex(index: number): boolean {
  return Number.isSafeInteger(index) && index >= 0;
}

function updateAt(
  value: Value | undefined,
  path: PathSeg[],
  update: (current: Value | undefined) => Value,
): Value {
  if (!path.length) return update(value);
  const [segment, ...rest] = path;
  if ("Key" in segment) {
    const map = mapValue(value);
    if (rest.length && !Object.prototype.hasOwnProperty.call(map, segment.Key))
      throw new Error(
        `patch path key ${JSON.stringify(segment.Key)} not found`,
      );
    return {
      Map: {
        ...map,
        [segment.Key]: updateAt(map[segment.Key], rest, update),
      },
    };
  }
  const list = listValue(value);
  if (!validIndex(segment.Index) || segment.Index >= list.length)
    throw new Error(
      `patch path index ${segment.Index} out of bounds (len ${list.length})`,
    );
  const next = [...list];
  next[segment.Index] = updateAt(next[segment.Index], rest, update);
  return { List: next };
}

function applyOp(value: Value, op: PatchOp): Value {
  if ("Set" in op) return updateAt(value, op.Set.path, () => op.Set.value);
  if ("Remove" in op) {
    const { path } = op.Remove;
    const segment = path[path.length - 1];
    if (!segment || !("Key" in segment))
      throw new Error("remove path must end in a map key");
    return updateAt(value, path.slice(0, -1), (container) => {
      const map = { ...mapValue(container) };
      delete map[segment.Key];
      return { Map: map };
    });
  }
  if ("Insert" in op) {
    const { path, index, value: inserted } = op.Insert;
    return updateAt(value, path, (container) => {
      const list = listValue(container);
      if (!validIndex(index) || index > list.length)
        throw new Error(
          `insert index ${index} out of bounds (len ${list.length})`,
        );
      const next = [...list];
      next.splice(index, 0, inserted);
      return { List: next };
    });
  }
  if ("RemoveAt" in op) {
    const { path, index } = op.RemoveAt;
    return updateAt(value, path, (container) => {
      const list = listValue(container);
      if (!validIndex(index) || index >= list.length)
        throw new Error(
          `remove index ${index} out of bounds (len ${list.length})`,
        );
      const next = [...list];
      next.splice(index, 1);
      return { List: next };
    });
  }
  if ("Move" in op) {
    const { path, from, to } = op.Move;
    return updateAt(value, path, (container) => {
      const list = listValue(container);
      if (!validIndex(from) || from >= list.length)
        throw new Error(
          `move source index ${from} out of bounds (len ${list.length})`,
        );
      if (!validIndex(to) || to >= list.length)
        throw new Error(
          `move destination index ${to} out of bounds (len ${list.length})`,
        );
      if (from === to) return container as Value;
      const next = [...list];
      const [moved] = next.splice(from, 1);
      next.splice(to, 0, moved);
      return { List: next };
    });
  }
  if ("Reorder" in op) {
    const { path, order } = op.Reorder;
    return updateAt(value, path, (container) => {
      const list = listValue(container);
      if (!Array.isArray(order))
        throw new Error("reorder order must be an array");
      if (order.length !== list.length)
        throw new Error(
          `reorder length ${order.length} does not match list length ${list.length}`,
        );
      const seen = new Set<number>();
      for (const index of order) {
        if (!validIndex(index) || index >= list.length)
          throw new Error(
            `reorder index ${index} out of bounds (len ${list.length})`,
          );
        if (seen.has(index))
          throw new Error(`reorder index ${index} is duplicated`);
        seen.add(index);
      }
      if (order.every((old, current) => old === current))
        return container as Value;
      return { List: order.map((index) => list[index]) };
    });
  }
  throw new Error("unknown patch op");
}

function applyPatch(value: Value, patch: ModelPatch): Value {
  let next = value;
  for (const op of patch.ops) next = applyOp(next, op);
  return next;
}

/** Mirrors a remote transports `Session` from connection messages.
 *
 * Inbound frames are decoded by type — text frames are JSON, binary frames are MessagePack — so a
 * client transparently mirrors a server regardless of the negotiated codec. Binary built-in codecs
 * and edit generation require the wasm core to be initialized.
 */
export class Client {
  private values = new Map<number, unknown>();
  private revs = new Map<number, number>();
  private changeListeners: Array<(change: ReceiveChange) => void> = [];
  private rejectListeners: Array<(reject: RejectMsg) => void> = [];

  constructor(private codec: string = "json") {}

  /** Register a listener fired when the server refuses a proposed edit, with the decoded `reject`
   * frame (model id, the server's current rev, and the validation error). The mirror itself reverts
   * via the authoritative snapshot the server sends alongside. Returns an unsubscribe function.
   */
  onReject(listener: (reject: RejectMsg) => void): () => void {
    this.rejectListeners.push(listener);
    return () => {
      const i = this.rejectListeners.indexOf(listener);
      if (i >= 0) this.rejectListeners.splice(i, 1);
    };
  }

  /** Register a listener fired after each accepted snapshot or patch — the same `ReceiveChange`
   * `recv` returns — so `connect`/`run`/`connectSSE` consumers get path-level changes without
   * managing the socket themselves. Not fired for ignored frames (stale revision, unknown message
   * type). Returns an unsubscribe function. A listener exception propagates to the `recv` caller;
   * the mirror has already updated by then.
   */
  onChange(listener: (change: ReceiveChange) => void): () => void {
    this.changeListeners.push(listener);
    return () => {
      const i = this.changeListeners.indexOf(listener);
      if (i >= 0) this.changeListeners.splice(i, 1);
    };
  }

  private accepted(change: ReceiveChange): ReceiveChange {
    for (const listener of [...this.changeListeners]) listener(change);
    return change;
  }

  /** Apply an inbound snapshot or patch frame to the mirror.
   *
   * Decodes by the client's codec: a registered custom codec, else built-in JSON (text) / msgpack
   * (binary). Returns the accepted change so reactive adapters can update only its paths; returns
   * `undefined` for a patch whose revision was already applied, and for an unrecognized message
   * type — ignored, not an error, so a newer server can add message types without breaking older
   * clients. The returned change and values from `value()` share immutable branches with the
   * mirror; consumers must not mutate them. Invalid frames throw without changing the mirror or its
   * accepted revision.
   */
  recv(data: string | Uint8Array): ReceiveChange | undefined {
    const custom = codecFor(this.codec);
    let msg: SnapshotMsg | PatchMsg | RejectMsg;
    if (custom) {
      msg = custom.decode(data) as SnapshotMsg | PatchMsg | RejectMsg;
    } else if (typeof data === "string") {
      msg = JSON.parse(data);
    } else {
      // binary frame: disambiguate by the connection's codec (msgpack vs cbor)
      msg = JSON.parse(
        this.codec === "cbor" ? cborToJson(data) : msgpackToJson(data),
      );
    }
    if (msg.t === "snapshot") {
      this.values.set(msg.id, msg.value);
      this.revs.set(msg.id, msg.rev);
      return this.accepted({ t: "snapshot", id: msg.id, rev: msg.rev });
    } else if (msg.t === "patch") {
      // rev is the model's sequence number; ignore a patch already reflected in the mirror (e.g. one
      // the opening snapshot already captured, which the server then also broadcasts).
      const seen = this.revs.get(msg.id);
      if (seen !== undefined && msg.patch.rev <= seen) return undefined;
      const current = this.values.get(msg.id);
      if (current === undefined)
        throw new Error(`patch received before snapshot for model ${msg.id}`);
      this.values.set(msg.id, applyPatch(current as Value, msg.patch));
      this.revs.set(msg.id, msg.patch.rev);
      return this.accepted(msg);
    } else if (msg.t === "reject") {
      // the mirror is untouched: the server reverts the proposer with the snapshot sent alongside
      for (const listener of [...this.rejectListeners]) listener(msg);
      return undefined;
    }
    // an unrecognized message type is ignored (not an error): the server may be newer than this
    // client and send types it predates (e.g. a future presence frame)
    return undefined;
  }

  /** The current mirrored core `Value` of a model. */
  value(id: number): unknown {
    return this.values.get(id);
  }

  ids(): number[] {
    return [...this.values.keys()];
  }

  /** Propose an edit to a mirrored model; returns the patch frame to send (encoded in this codec).
   *
   * Server-authoritative: the local mirror updates when the server echoes the authoritative patch
   * back via `recv`, not optimistically.
   */
  edit(id: number, value: unknown): string | Uint8Array {
    const patch = JSON.parse(
      diff(JSON.stringify(this.values.get(id)), JSON.stringify(value)),
    );
    const msg = { t: "patch", id, patch };
    const custom = codecFor(this.codec);
    if (custom) return custom.encode(msg);
    const s = JSON.stringify(msg);
    if (this.codec === "msgpack") return jsonToMsgpack(s);
    if (this.codec === "cbor") return jsonToCbor(s);
    return s;
  }

  /** Connect to a transports server and mirror it. Returns the `WebSocket`.
   *
   * On a reconnect (this client already mirrors models) it appends `?since=` with its last-seen rev per
   * model, so the server replays only the delta instead of re-sending each whole model.
   */
  connect(url: string): WebSocket {
    const sep = url.includes("?") ? "&" : "?";
    let params = `codec=${this.codec}`;
    if (this.revs.size) {
      const since = encodeURIComponent(
        JSON.stringify(Object.fromEntries(this.revs)),
      );
      params += `&since=${since}`;
    }
    const ws = new WebSocket(`${url}${sep}${params}`);
    ws.binaryType = "arraybuffer";
    ws.addEventListener("message", (e) => {
      const data = (e as MessageEvent).data;
      this.recv(
        typeof data === "string" ? data : new Uint8Array(data as ArrayBuffer),
      );
    });
    return ws;
  }

  /** Connect and mirror, **reconnecting** whenever the socket drops — so the client survives a server
   * restart or a refresh. `authority` decides reconciliation on each (re)connect:
   *
   * - `"server"` (default): the server is canonical; the client adopts its state (resuming via `?since=`
   *   when it can, else a fresh snapshot) — the "refetch on refresh" behavior.
   * - `"client"`: the client is canonical; after the server's snapshot it pushes its last-known state
   *   back as an edit, rectifying a server that came back stale/empty (merges under a CRDT, else
   *   overwrites).
   *
   * `onMessage` fires after each applied frame (e.g. to re-render). Returns `{ stop() }`.
   */
  run(
    url: string,
    opts: {
      authority?: "server" | "client";
      retry?: number;
      onMessage?: () => void;
    } = {},
  ): { stop: () => void } {
    const { authority = "server", retry = 1000, onMessage } = opts;
    let stopped = false;
    const loop = () => {
      if (stopped) return;
      const pre = authority === "client" ? new Map(this.values) : null;
      const pushed = new Set<number>();
      const ws = this.connect(url); // reuses connect(): adds the recv listener + ?since= resume
      ws.addEventListener("message", () => {
        onMessage?.();
        if (pre) {
          // rectify: once the server has (re)snapshotted a model, push our copy back to it
          for (const id of this.values.keys()) {
            if (!pushed.has(id) && pre.has(id)) {
              // cast, not copy: wasm-bindgen types its output Uint8Array<ArrayBufferLike>, but it
              // is always ArrayBuffer-backed, which is what WebSocket.send requires
              ws.send(
                this.edit(id, pre.get(id)) as string | Uint8Array<ArrayBuffer>,
              );
              pushed.add(id);
            }
          }
        }
      });
      ws.addEventListener("close", () => {
        if (!stopped) setTimeout(loop, retry);
      });
      ws.addEventListener("error", () => {
        try {
          ws.close();
        } catch {
          /* already closing */
        }
      });
    };
    loop();
    return {
      stop() {
        stopped = true;
      },
    };
  }

  /** Mirror a server over Server-Sent Events (receive-only, JSON). Returns the `EventSource`. */
  connectSSE(url: string): EventSource {
    const es = new EventSource(url);
    es.addEventListener("message", (e) =>
      this.recv((e as MessageEvent).data as string),
    );
    return es;
  }
}
