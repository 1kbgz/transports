import { codecFor } from "./codecs";
import {
  ClientState as WasmClientState,
  CrdtDocument,
  CrdtSpec,
  decodeMessage,
  diff,
  encodeMessage,
  toValue,
  type CrdtEffect,
  type CrdtMutation,
  type CrdtOp,
  type Dot,
} from "./index";
import type { Value } from "./bridge";

type SnapshotMsg = {
  t: "snapshot";
  id: number;
  type: string;
  rev: number;
  value: unknown;
};
type CrdtSnapshotMsg = {
  t: "crdt_snapshot";
  id: number;
  type: string;
  rev: number;
  value: unknown;
  spec: Parameters<typeof CrdtSpec.fromObject>[0];
  state: Record<string, unknown>;
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
export type PatchMsg = {
  t: "patch";
  id: number;
  patch: ModelPatch;
  proposal?: string;
};
export type CrdtMsg = {
  t: "crdt";
  id: number;
  rev: number;
  ops: CrdtOp[];
  effect?: CrdtEffect;
  proposal?: string;
};
/** An accepted proposal that produced no authoritative patch. */
export type AckMsg = { t: "ack"; id: number; rev: number; proposal: string };
/** The server refused a proposed edit; `rev` is its current revision and `error` says why (the
 * model's validation message). Sent to the proposer only, after the authoritative revert. */
export type RejectMsg = {
  t: "reject";
  id: number;
  rev: number;
  error: string;
  proposal?: string;
  crdt_ops?: CrdtOp[];
};
/** Frame metadata returned after the mirror accepts a snapshot or patch. */
export type ReceiveChange =
  | { t: "snapshot"; id: number; rev: number }
  | { t: "crdt_snapshot"; id: number; rev: number }
  | PatchMsg
  | CrdtMsg;

type ClientEffect =
  | { effect: "snapshot"; id: number; rev: number }
  | { effect: "crdt_snapshot"; id: number; rev: number }
  | {
      effect: "patch" | "stale_patch";
      id: number;
      rev: number;
      proposal?: string;
    }
  | {
      effect: "crdt";
      id: number;
      rev: number;
      proposal?: string;
    }
  | {
      effect: "acknowledgement";
      id: number;
      rev: number;
      proposal: string;
    }
  | {
      effect: "rejection";
      id: number;
      rev: number;
      error: string;
      proposal?: string;
    }
  | { effect: "ignore" }
  | { effect: "disconnect"; proposals: string[] };

interface ClientStateAdapter {
  prepare(message: object): ClientEffect;
  commit(effect: ClientEffect): void;
  proposal(id: number, ops: PatchOp[], proposal?: string): string;
  disconnect(): Extract<ClientEffect, { effect: "disconnect" }>;
  revisions(): Record<string, number>;
  pending(): string[];
  abandon(proposal: string): boolean;
}

class SharedClientState implements ClientStateAdapter {
  private inner = new WasmClientState();

  prepare(message: object): ClientEffect {
    return JSON.parse(this.inner.prepare(JSON.stringify(message)));
  }

  commit(effect: ClientEffect): void {
    this.inner.commit(JSON.stringify(effect));
  }

  proposal(id: number, ops: PatchOp[], proposal?: string): string {
    return this.inner.proposal(BigInt(id), JSON.stringify(ops), proposal);
  }

  disconnect(): Extract<ClientEffect, { effect: "disconnect" }> {
    return JSON.parse(this.inner.disconnect());
  }

  revisions(): Record<string, number> {
    return JSON.parse(this.inner.revisions());
  }

  pending(): string[] {
    return JSON.parse(this.inner.pending());
  }

  abandon(proposal: string): boolean {
    return this.inner.abandon(proposal);
  }
}

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

interface BatchMsg {
  t: "batch";
  msgs: (SnapshotMsg | CrdtSnapshotMsg | PatchMsg | CrdtMsg | RejectMsg)[];
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

function dotKey(dot: Dot): string {
  return JSON.stringify([dot.counter, dot.replica]);
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
  private crdt = new Map<number, CrdtDocument>();
  private crdtOutbox: Array<{ id: number; ops: CrdtOp[] }> = [];
  private replica: string;
  private changeListeners: Array<(change: ReceiveChange) => void> = [];
  private ackListeners: Array<(ack: PatchMsg | CrdtMsg | AckMsg) => void> = [];
  private rejectListeners: Array<(reject: RejectMsg) => void> = [];
  private abandonListeners: Array<(proposals: string[]) => void> = [];
  private connectListeners: Array<() => void> = [];
  private disconnectListeners: Array<() => void> = [];
  private state: ClientStateAdapter;
  // outbound channel of the active managed connection (set by connect()/run(), cleared on close)
  private sender: ((frame: string | Uint8Array) => void) | null = null;

  constructor(private codec: string = "json") {
    this.state = new SharedClientState();
    const random =
      globalThis.crypto?.randomUUID?.() ?? Math.random().toString(16).slice(2);
    this.replica = `client-${random}`;
  }

  /** Whether a managed connection (`connect()`/`run()`) is open right now. */
  get connected(): boolean {
    return this.sender !== null;
  }

  /** Send a frame over the active managed connection (`connect()`/`run()`).
   *
   * Returns `true` when handed to an open connection, `false` when none is active (never connected,
   * in a reconnect gap, or receive-only `connectSSE`) — the frame is dropped, matching a browser
   * WebSocket's send on a closed socket. That makes it a drop-in fire-and-forget callback for an
   * adapter — e.g. spaday's `connectStore(store, client, (f) => client.send(f), codec)` — check
   * `connected` (or the return) when delivery matters.
   */
  send(frame: string | Uint8Array): boolean {
    if (!this.sender) return false;
    this.sender(frame);
    return true;
  }

  /** Propose an edit over the active connection: `send(edit(id, value))`. Server-authoritative —
   * the mirror updates when the authoritative patch echoes back (or `onReject` fires). Returns
   * `false` (dropped) when not connected. */
  propose(id: number, value: unknown, proposal?: string): boolean {
    return this.sendProposal(this.edit(id, value, proposal));
  }

  /** Propose explicit patch operations over the active connection. */
  proposeOps(id: number, ops: PatchOp[], proposal?: string): boolean {
    return this.sendProposal(this.editOps(id, ops, proposal));
  }

  /** Apply local CRDT mutations immediately and retain their operations until the server echoes
   * them. A disconnected edit remains queued for the next managed connection. */
  proposeCrdt(id: number, mutations: CrdtMutation[]): boolean {
    const frame = this.editCrdt(id, mutations);
    return this.send(frame);
  }

  private sendProposal(frame: string | Uint8Array): boolean {
    const custom = codecFor(this.codec);
    const message = custom
      ? (custom.decode(frame) as PatchMsg)
      : JSON.parse(
          typeof frame === "string" ? frame : decodeMessage(frame, this.codec),
        );
    try {
      const sent = this.send(frame);
      if (!sent && message.proposal !== undefined)
        this.abandonProposal(message.proposal);
      return sent;
    } catch (error) {
      if (message.proposal !== undefined)
        this.abandonProposal(message.proposal);
      throw error;
    }
  }

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

  /** Register a listener fired when the server accepts a tagged proposal. Receives an authoritative
   * patch or no-op acknowledgement carrying the same opaque proposal identifier. */
  onAck(listener: (ack: PatchMsg | CrdtMsg | AckMsg) => void): () => void {
    this.ackListeners.push(listener);
    return () => {
      const i = this.ackListeners.indexOf(listener);
      if (i >= 0) this.ackListeners.splice(i, 1);
    };
  }

  /** Register a listener fired when an active managed WebSocket disconnects. */
  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.push(listener);
    return () => {
      const i = this.disconnectListeners.indexOf(listener);
      if (i >= 0) this.disconnectListeners.splice(i, 1);
    };
  }

  /** Register a listener fired when a managed WebSocket opens, including reconnects. */
  onConnect(listener: () => void): () => void {
    this.connectListeners.push(listener);
    return () => {
      const i = this.connectListeners.indexOf(listener);
      if (i >= 0) this.connectListeners.splice(i, 1);
    };
  }

  /** Register a listener fired with unsettled proposal identifiers on disconnect. */
  onAbandon(listener: (proposals: string[]) => void): () => void {
    this.abandonListeners.push(listener);
    return () => {
      const i = this.abandonListeners.indexOf(listener);
      if (i >= 0) this.abandonListeners.splice(i, 1);
    };
  }

  private opened(sender: (frame: string | Uint8Array) => void): void {
    this.sender = sender;
    this.flushCrdtOutbox();
    for (const listener of [...this.connectListeners]) listener();
  }

  private flushCrdtOutbox(): void {
    if (!this.sender) return;
    for (const pending of this.crdtOutbox)
      this.sender(
        this.encode({ t: "crdt", id: pending.id, rev: 0, ops: pending.ops }),
      );
  }

  private settleCrdtOps(id: number, ops: CrdtOp[]): void {
    const echoed = new Set(ops.map((op) => dotKey(op.dot)));
    this.crdtOutbox = this.crdtOutbox.flatMap((pending) => {
      if (pending.id !== id) return [pending];
      const remaining = pending.ops.filter((op) => !echoed.has(dotKey(op.dot)));
      return remaining.length ? [{ id, ops: remaining }] : [];
    });
  }

  private encode(message: object): string | Uint8Array {
    const custom = codecFor(this.codec);
    if (custom) return custom.encode(message);
    const json = JSON.stringify(message);
    return this.codec === "json" ? json : encodeMessage(json, this.codec);
  }

  private disconnected(): void {
    const { proposals } = this.state.disconnect();
    if (proposals.length)
      for (const listener of [...this.abandonListeners]) listener(proposals);
    for (const listener of [...this.disconnectListeners]) listener();
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

  private acknowledge(msg: PatchMsg | CrdtMsg | AckMsg): void {
    if (msg.proposal !== undefined)
      for (const listener of [...this.ackListeners]) listener(msg);
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
  recv(data: string | Uint8Array): ReceiveChange | ReceiveChange[] | undefined {
    const custom = codecFor(this.codec);
    let msg:
      | SnapshotMsg
      | CrdtSnapshotMsg
      | PatchMsg
      | CrdtMsg
      | AckMsg
      | RejectMsg
      | BatchMsg;
    if (custom) {
      msg = custom.decode(data) as
        | SnapshotMsg
        | CrdtSnapshotMsg
        | PatchMsg
        | CrdtMsg
        | AckMsg
        | RejectMsg;
    } else if (typeof data === "string") {
      msg = JSON.parse(data);
    } else {
      // binary frame: disambiguate by the connection's codec (msgpack vs cbor)
      msg = JSON.parse(decodeMessage(data, this.codec));
    }
    if (msg.t === "batch") {
      // a negotiated batch frame (the connection asked with ?batch=1): apply each message in
      // order and return the accepted changes as an array
      const accepted = msg.msgs
        .map((m) => this.apply(m))
        .filter((a): a is ReceiveChange => a !== undefined);
      return accepted.length ? accepted : undefined;
    }
    return this.apply(msg);
  }

  private apply(
    msg:
      | SnapshotMsg
      | CrdtSnapshotMsg
      | PatchMsg
      | CrdtMsg
      | AckMsg
      | RejectMsg,
  ): ReceiveChange | undefined {
    const effect = this.state.prepare(msg);
    if (effect.effect === "snapshot") {
      const snapshot = msg as SnapshotMsg;
      this.crdt.delete(snapshot.id);
      this.crdtOutbox = this.crdtOutbox.filter(
        (pending) => pending.id !== snapshot.id,
      );
      this.values.set(snapshot.id, snapshot.value);
      this.state.commit(effect);
      return this.accepted({
        t: "snapshot",
        id: snapshot.id,
        rev: snapshot.rev,
      });
    } else if (effect.effect === "crdt_snapshot") {
      const snapshot = msg as CrdtSnapshotMsg;
      const document = CrdtDocument.fromState(
        CrdtSpec.fromObject(snapshot.spec),
        snapshot.state,
        this.replica,
      );
      for (const pending of this.crdtOutbox)
        if (pending.id === snapshot.id) document.apply(pending.ops);
      this.crdt.set(snapshot.id, document);
      this.values.set(snapshot.id, toValue(document.value));
      this.state.commit(effect);
      return this.accepted({
        t: "crdt_snapshot",
        id: snapshot.id,
        rev: snapshot.rev,
      });
    } else if (effect.effect === "patch") {
      const patch = msg as PatchMsg;
      const current = this.values.get(patch.id);
      if (current === undefined)
        throw new Error(`patch received before snapshot for model ${patch.id}`);
      const value = applyPatch(current as Value, patch.patch);
      this.state.commit(effect);
      this.values.set(patch.id, value);
      const accepted = this.accepted(patch);
      this.acknowledge(patch);
      return accepted;
    } else if (effect.effect === "crdt") {
      const change = msg as CrdtMsg;
      const document = this.crdt.get(change.id);
      if (!document)
        throw new Error(
          `CRDT operations received before CRDT snapshot for model ${change.id}`,
        );
      document.apply(change.ops);
      this.values.set(change.id, toValue(document.value));
      this.state.commit(effect);
      this.settleCrdtOps(change.id, change.ops);
      const accepted = this.accepted(change);
      this.acknowledge(change);
      return accepted;
    } else if (effect.effect === "stale_patch") {
      this.state.commit(effect);
      this.acknowledge(msg as PatchMsg);
      return undefined;
    } else if (effect.effect === "acknowledgement") {
      this.state.commit(effect);
      this.acknowledge(msg as AckMsg);
      return undefined;
    } else if (effect.effect === "rejection") {
      this.state.commit(effect);
      const rejection = msg as RejectMsg;
      if (rejection.crdt_ops)
        this.settleCrdtOps(rejection.id, rejection.crdt_ops);
      for (const listener of [...this.rejectListeners]) listener(rejection);
      return undefined;
    }
    return undefined;
  }

  /** The current mirrored core `Value` of a model. */
  value(id: number): unknown {
    return this.values.get(id);
  }

  /** The canonical merge specification for a CRDT-backed model, else undefined. */
  crdtSpec(id: number): CrdtSpec | undefined {
    return this.crdt.get(id)?.spec;
  }

  ids(): number[] {
    return [...this.values.keys()];
  }

  /** Identifiers for proposals that have not settled or been abandoned. */
  pendingProposals(): string[] {
    return this.state.pending();
  }

  /** Number of local CRDT operations awaiting an authoritative echo. */
  pendingCrdtOps(id?: number): number {
    return this.crdtOutbox.reduce(
      (count, pending) =>
        count +
        (id === undefined || pending.id === id ? pending.ops.length : 0),
      0,
    );
  }

  /** Stop tracking one proposal that the caller did not send.
   *
   * Returns whether the proposal was pending. This does not call `onAbandon`; those listeners report
   * proposals abandoned by a managed connection closing.
   */
  abandonProposal(proposal: string): boolean {
    return this.state.abandon(proposal);
  }

  /** Propose an edit to a mirrored model; returns the patch frame to send (encoded in this codec).
   *
   * Server-authoritative: the local mirror updates when the server echoes the authoritative patch
   * back via `recv`, not optimistically.
   */
  edit(id: number, value: unknown, proposal?: string): string | Uint8Array {
    const patch = JSON.parse(
      diff(JSON.stringify(this.values.get(id)), JSON.stringify(value)),
    );
    return this.editOps(id, patch.ops, proposal);
  }

  /** Propose explicit patch operations. This can express a value equal to the current mirror while
   * an older optimistic proposal is pending. A client-local proposal id is assigned by default. */
  editOps(id: number, ops: PatchOp[], proposal?: string): string | Uint8Array {
    const message = this.state.proposal(id, ops, proposal);
    const decoded = JSON.parse(message) as PatchMsg;
    try {
      const custom = codecFor(this.codec);
      if (custom) return custom.encode(decoded);
      if (this.codec !== "json") return encodeMessage(message, this.codec);
      return message;
    } catch (error) {
      if (decoded.proposal !== undefined)
        this.abandonProposal(decoded.proposal);
      throw error;
    }
  }

  /** Apply local CRDT mutations and return their idempotent operation frame. */
  editCrdt(id: number, mutations: CrdtMutation[]): string | Uint8Array {
    const document = this.crdt.get(id);
    if (!document) throw new Error(`model ${id} is not CRDT-backed`);
    const change = document.mutate(mutations);
    this.values.set(id, toValue(document.value));
    if (change.ops.length) this.crdtOutbox.push({ id, ops: change.ops });
    return this.encode({ t: "crdt", id, rev: 0, ops: change.ops });
  }

  /** Connect to a transports server and mirror it. Returns the `WebSocket`.
   *
   * On a reconnect (this client already mirrors models) it appends `?since=` with its last-seen rev per
   * model, so the server replays only the delta instead of re-sending each whole model.
   */
  connect(url: string): WebSocket {
    const sep = url.includes("?") ? "&" : "?";
    let params = `codec=${this.codec}`;
    const revisions = this.state.revisions();
    if (Object.keys(revisions).length) {
      const since = encodeURIComponent(JSON.stringify(revisions));
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
    const sender = (frame: string | Uint8Array) =>
      ws.send(frame as string | Uint8Array<ArrayBuffer>);
    ws.addEventListener("open", () => {
      this.opened(sender); // arm only once open: send during CONNECTING throws in the DOM
    });
    ws.addEventListener("close", () => {
      if (this.sender === sender) {
        this.sender = null; // don't clobber a newer reconnect's channel
        this.disconnected();
      }
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
            if (!this.crdt.has(id) && !pushed.has(id) && pre.has(id)) {
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
