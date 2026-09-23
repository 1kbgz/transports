import {
  placeholder,
  diff,
  apply,
  toValue,
  fromValue,
  encodeAs,
  decodeAs,
  jsonToMsgpack,
  msgpackToJson,
  jsonToCbor,
  cborToJson,
  normalizeMessage,
  encodeMessage,
  decodeMessage,
  ClientState,
  registerCodec,
  unregisterCodec,
  Client,
  normalizeCrdtSpec,
  crdtSpecHash,
  requireCrdtSpecHash,
  CrdtSpec,
  CrdtDocument,
} from "../src/ts/index";
import { initSync } from "../dist/pkg/transports";
import fs from "fs";
import { test, expect } from "@playwright/test";

test.beforeAll(async () => {
  const buffer = fs.readFileSync("./dist/pkg/transports_bg.wasm");
  initSync({ module: buffer });
});

test("exports are defined", async () => {
  expect(placeholder).toBeDefined();
});

test("object bridge round-trips (pure JS)", async () => {
  const obj = { name: "lamp", on: false, tags: ["a"] };
  expect(fromValue(toValue(obj))).toEqual(obj);
});

test("diff/apply via the wasm core", async () => {
  const a = JSON.stringify(toValue({ on: false }));
  const b = JSON.stringify(toValue({ on: true }));
  const patch = diff(a, b);
  expect(JSON.parse(apply(a, patch))).toEqual(JSON.parse(b));
});

test("wasm binding matches the shared CRDT spec fixture", async () => {
  const fixture = JSON.parse(
    fs.readFileSync("../rust/tests/fixtures/crdt_spec.json", "utf8"),
  );

  expect(JSON.stringify(normalizeCrdtSpec(fixture.spec))).toBe(
    fixture.canonical,
  );
  expect(crdtSpecHash(fixture.spec)).toBe(fixture.hash);
  expect(() => requireCrdtSpecHash(fixture.spec, fixture.hash)).not.toThrow();
  expect(() => requireCrdtSpecHash(fixture.spec, "sha256:other")).toThrow(
    /incompatible CRDT spec/,
  );

  const spec = CrdtSpec.fromObject(fixture.spec);
  expect(spec.toJson()).toBe(fixture.canonical);
  expect(spec.toObject()).toEqual(JSON.parse(fixture.canonical));
  expect(spec.hash).toBe(fixture.hash);
  expect(spec.equals(CrdtSpec.fromJson(spec.toJson()))).toBe(true);
  expect(JSON.stringify(spec)).toBe(fixture.canonical);

  const element = { kind: "map", fields: { meta: { kind: "map" } } };
  const first = new CrdtSpec({
    kind: "set",
    keys: [["tenant"], ["meta", "id"]],
    element,
  });
  const reversed = new CrdtSpec({
    kind: "set",
    keys: [["meta", "id"], ["tenant"]],
    element,
  });
  expect(first.equals(reversed)).toBe(true);
  expect(first.hash).toBe(reversed.hash);
});

test("CRDT spec rejects unknown policies and register fields", async () => {
  expect(() => CrdtSpec.fromObject({ root: { kind: "unknown" } })).toThrow();
  expect(() =>
    CrdtSpec.fromObject({ root: { kind: "register", fields: {} } }),
  ).toThrow(/unknown field/);
});

test("wasm binding matches the shared CRDT reducer fixture", async () => {
  const fixture = JSON.parse(
    fs.readFileSync("../rust/tests/fixtures/crdt_reducer.json", "utf8"),
  );
  const spec = CrdtSpec.fromObject(fixture.spec);
  const document = new CrdtDocument(spec, { text: "", title: "draft" }, "a");
  const change = document.mutate([
    {
      kind: "register_set",
      path: [{ kind: "key", key: "title" }],
      value: "ready",
    },
    {
      kind: "sequence_insert",
      path: [{ kind: "key", key: "text" }],
      after: null,
      values: ["h", "i"],
    },
    {
      kind: "sequence_splice",
      path: [{ kind: "key", key: "text" }],
      index: 1,
      delete_count: 1,
      values: ["λ"],
    },
  ]);

  expect(document.value).toEqual({ text: "hλ", title: "ready" });
  expect(change.effect.applied).toBe(4);
  expect(change.ops.map((op) => op.dot)).toEqual([
    { counter: 1, replica: "a" },
    { counter: 2, replica: "a" },
    { counter: 3, replica: "a" },
    { counter: 4, replica: "a" },
  ]);
  const receiver = CrdtDocument.fromState(spec, document.state, "b");
  expect(receiver.value).toEqual(document.value);
  const duplicate = receiver.apply(change.ops);
  expect(duplicate.patch.ops).toEqual([]);
  expect(duplicate.applied).toBe(0);
});

test("Client retains offline CRDT operations through a reconnect snapshot", async () => {
  const spec = new CrdtSpec({
    kind: "sequence",
    materialization: "string",
  });
  const server = new CrdtDocument(spec, "", "server");
  const snapshot = {
    t: "crdt_snapshot",
    id: 9,
    type: "Document",
    rev: 0,
    value: toValue(server.value),
    spec: spec.toObject(),
    state: server.state,
  };
  const client = new Client();
  client.recv(JSON.stringify(snapshot));
  expect(client.crdtSpec(9)?.equals(spec)).toBe(true);
  expect(client.crdtSpec(999)).toBeUndefined();

  const frame = client.editCrdt(9, [
    {
      kind: "sequence_insert",
      path: [],
      after: null,
      values: [..."offline"],
    },
  ]);
  const message = JSON.parse(frame);
  expect(client.value(9)).toEqual(toValue("offline"));
  expect(client.pendingCrdtOps(9)).toBe(1);

  client.recv(JSON.stringify(snapshot));
  expect(client.value(9)).toEqual(toValue("offline"));
  server.apply(message.ops);
  client.recv(JSON.stringify({ t: "crdt", id: 9, rev: 1, ops: message.ops }));
  expect(client.pendingCrdtOps(9)).toBe(0);
  expect(client.value(9)).toEqual(toValue(server.value));
});

test("Client applies concurrent CRDT operations with out-of-order revisions", async () => {
  const spec = new CrdtSpec({
    kind: "sequence",
    materialization: "string",
  });
  const initial = new CrdtDocument(spec, "", "server");
  const client = new Client();
  client.recv(
    JSON.stringify({
      t: "crdt_snapshot",
      id: 10,
      type: "Document",
      rev: 0,
      value: toValue(initial.value),
      spec: spec.toObject(),
      state: initial.state,
    }),
  );
  const a = CrdtDocument.fromState(spec, initial.state, "a");
  const b = CrdtDocument.fromState(spec, initial.state, "b");
  const aOps = a.mutate([
    {
      kind: "sequence_insert",
      path: [],
      after: null,
      values: ["a"],
    },
  ]).ops;
  const bOps = b.mutate([
    {
      kind: "sequence_insert",
      path: [],
      after: null,
      values: ["b"],
    },
  ]).ops;
  initial.apply(aOps);
  initial.apply(bOps);

  client.recv(JSON.stringify({ t: "crdt", id: 10, rev: 2, ops: bOps }));
  client.recv(JSON.stringify({ t: "crdt", id: 10, rev: 1, ops: aOps }));

  expect(client.value(10)).toEqual(toValue(initial.value));
});

test("Client clears rejected CRDT operations before an authoritative snapshot", async () => {
  const spec = new CrdtSpec({
    kind: "sequence",
    materialization: "string",
  });
  const server = new CrdtDocument(spec, "", "server");
  const snapshot = {
    t: "crdt_snapshot",
    id: 11,
    type: "Document",
    rev: 0,
    value: toValue(server.value),
    spec: spec.toObject(),
    state: server.state,
  };
  const client = new Client();
  client.recv(JSON.stringify(snapshot));
  const edit = JSON.parse(
    client.editCrdt(11, [
      {
        kind: "sequence_insert",
        path: [],
        after: null,
        values: ["x"],
      },
    ]),
  );

  client.recv(
    JSON.stringify({
      t: "reject",
      id: 11,
      rev: 0,
      error: "read-only subscription",
      crdt_ops: edit.ops,
    }),
  );
  client.recv(JSON.stringify(snapshot));

  expect(client.pendingCrdtOps(11)).toBe(0);
  expect(client.value(11)).toEqual(toValue(""));
});

test("plain snapshot clears CRDT state and pending operations", async () => {
  const spec = new CrdtSpec({
    kind: "sequence",
    materialization: "string",
  });
  const server = new CrdtDocument(spec, "", "server");
  const client = new Client();
  client.recv(
    JSON.stringify({
      t: "crdt_snapshot",
      id: 12,
      type: "Document",
      rev: 0,
      value: toValue(server.value),
      spec: spec.toObject(),
      state: server.state,
    }),
  );
  client.editCrdt(12, [
    {
      kind: "sequence_insert",
      path: [],
      after: null,
      values: ["x"],
    },
  ]);

  client.recv(
    JSON.stringify({
      t: "snapshot",
      id: 12,
      type: "Document",
      rev: 1,
      value: toValue("plain"),
    }),
  );

  expect(client.value(12)).toEqual(toValue("plain"));
  expect(client.crdtSpec(12)).toBeUndefined();
  expect(client.pendingCrdtOps(12)).toBe(0);
  expect(() => client.editCrdt(12, [])).toThrow(/not CRDT-backed/);
  expect(
    client.recv(JSON.stringify({ t: "crdt", id: 12, rev: 2, ops: [] })),
  ).toBeUndefined();
});

test("wasm core emits and applies sequence moves", async () => {
  const a = JSON.stringify(toValue(["a", "b", "c", "d"]));
  const b = JSON.stringify(toValue(["d", "b", "a", "c"]));
  const patch = JSON.parse(diff(a, b));

  expect(patch.ops).toEqual([
    { Move: { path: [], from: 3, to: 0 } },
    { Move: { path: [], from: 2, to: 1 } },
  ]);
  expect(JSON.parse(apply(a, JSON.stringify(patch)))).toEqual(JSON.parse(b));
});

test("wasm core emits one permutation for a dense reorder", async () => {
  const old = Array.from({ length: 32 }, (_, index) => index);
  const reordered = [...old].reverse();
  const a = JSON.stringify(toValue(old));
  const b = JSON.stringify(toValue(reordered));
  const patch = JSON.parse(diff(a, b));

  expect(patch.ops).toEqual([{ Reorder: { path: [], order: reordered } }]);
  expect(JSON.parse(apply(a, JSON.stringify(patch)))).toEqual(JSON.parse(b));
});

test("msgpack round-trips via encodeAs/decodeAs", async () => {
  const v = JSON.stringify(toValue({ name: "lamp", on: true, count: 123456 }));
  const mp = encodeAs(v, "application/msgpack");
  expect(mp instanceof Uint8Array).toBe(true);
  expect(JSON.parse(decodeAs(mp, "application/msgpack"))).toEqual(
    JSON.parse(v),
  );
});

test("whole-message json<->msgpack round-trips", async () => {
  const msg = JSON.stringify({ t: "patch", id: 7, patch: { rev: 2, ops: [] } });
  const mp = jsonToMsgpack(msg);
  expect(mp instanceof Uint8Array).toBe(true);
  expect(JSON.parse(msgpackToJson(mp))).toEqual(JSON.parse(msg));
});

test("Client mirrors a binary (msgpack) snapshot then patch", async () => {
  const c = new Client("msgpack");
  c.recv(
    jsonToMsgpack(
      JSON.stringify({
        t: "snapshot",
        id: 1,
        type: "Device",
        rev: 0,
        value: { Map: { on: { Bool: false } } },
      }),
    ),
  );
  c.recv(
    jsonToMsgpack(
      JSON.stringify({
        t: "patch",
        id: 1,
        patch: {
          rev: 1,
          ops: [{ Set: { path: [{ Key: "on" }], value: { Bool: true } } }],
        },
      }),
    ),
  );
  expect(c.value(1)).toEqual({ Map: { on: { Bool: true } } });
});

test("cbor round-trips via encodeAs/decodeAs", async () => {
  const v = JSON.stringify(toValue({ name: "lamp", on: true, count: 123456 }));
  const cb = encodeAs(v, "application/cbor");
  expect(cb instanceof Uint8Array).toBe(true);
  expect(JSON.parse(decodeAs(cb, "application/cbor"))).toEqual(JSON.parse(v));
});

test("whole-message json<->cbor round-trips", async () => {
  const msg = JSON.stringify({ t: "patch", id: 7, patch: { rev: 2, ops: [] } });
  const cb = jsonToCbor(msg);
  expect(cb instanceof Uint8Array).toBe(true);
  expect(JSON.parse(cborToJson(cb))).toEqual(JSON.parse(msg));
});

test("live message model and codecs are shared with Rust", async () => {
  const msg = JSON.stringify({
    t: "ack",
    id: 7,
    rev: 3,
    proposal: "editor-1",
  });
  expect(JSON.parse(normalizeMessage(msg))).toEqual(JSON.parse(msg));
  for (const codec of ["json", "msgpack", "cbor"]) {
    expect(JSON.parse(decodeMessage(encodeMessage(msg, codec), codec))).toEqual(
      JSON.parse(msg),
    );
  }
});

test("wasm binding matches the shared client-state trace", async () => {
  const fixture = JSON.parse(
    fs.readFileSync("../rust/tests/fixtures/live_protocol.json", "utf8"),
  );
  const state = new ClientState();
  const prepare = (message) =>
    JSON.parse(state.prepare(JSON.stringify(message)));
  const commit = (effect) => state.commit(JSON.stringify(effect));

  let effect = prepare(fixture.snapshot);
  expect(effect).toEqual(fixture.effects.snapshot);
  commit(effect);
  state.proposal(7n, "[]", "editor-1");
  state.proposal(7n, "[]");

  effect = prepare(fixture.patch);
  expect(effect).toEqual(fixture.effects.patch);
  commit(effect);
  expect(JSON.parse(state.pending())).toEqual(["auto-1"]);

  effect = prepare({ ...fixture.patch, proposal: "auto-1" });
  expect(effect).toEqual(fixture.effects.stale_patch);
  commit(effect);

  for (const [proposal, message, expected] of [
    ["editor-2", fixture.ack, fixture.effects.ack],
    ["editor-3", fixture.reject, fixture.effects.reject],
  ]) {
    state.proposal(7n, "[]", proposal);
    effect = prepare(message);
    expect(effect).toEqual(expected);
    commit(effect);
  }

  effect = prepare(fixture.unknown);
  expect(effect).toEqual(fixture.effects.unknown);
  commit(effect);
  state.proposal(7n, "[]", "editor-4");
  expect(JSON.parse(state.disconnect())).toEqual(fixture.effects.disconnect);
  expect(JSON.parse(state.revisions())).toEqual({ 7: 3 });
  expect(JSON.parse(state.pending())).toEqual([]);
});

test("wasm binding matches the shared message-codec trace", async () => {
  const fixture = JSON.parse(
    fs.readFileSync("../rust/tests/fixtures/live_protocol.json", "utf8"),
  );
  for (const name of [
    "snapshot",
    "patch",
    "ack",
    "reject",
    "batch",
    "unknown",
  ]) {
    const message = JSON.stringify(fixture[name]);
    for (const codec of ["json", "msgpack", "cbor"])
      expect(
        JSON.parse(decodeMessage(encodeMessage(message, codec), codec)),
      ).toEqual(fixture[name]);
    expect([...encodeMessage(message, "msgpack")]).toEqual([
      ...jsonToMsgpack(message),
    ]);
    expect([...encodeMessage(message, "cbor")]).toEqual([
      ...jsonToCbor(message),
    ]);
  }
});

test("Client applies the shared batch fixture in order", async () => {
  const fixture = JSON.parse(
    fs.readFileSync("../rust/tests/fixtures/live_protocol.json", "utf8"),
  );
  const client = new Client();
  const accepted = client.recv(JSON.stringify(fixture.batch));
  expect(accepted.map((change) => change.t)).toEqual(["snapshot", "patch"]);
  expect(client.value(8)).toEqual({ Map: { count: { Int: 6 } } });
});

test("Client mirrors a binary (cbor) snapshot then patch", async () => {
  const c = new Client("cbor");
  c.recv(
    jsonToCbor(
      JSON.stringify({
        t: "snapshot",
        id: 1,
        type: "Device",
        rev: 0,
        value: { Map: { on: { Bool: false } } },
      }),
    ),
  );
  c.recv(
    jsonToCbor(
      JSON.stringify({
        t: "patch",
        id: 1,
        patch: {
          rev: 1,
          ops: [{ Set: { path: [{ Key: "on" }], value: { Bool: true } } }],
        },
      }),
    ),
  );
  expect(c.value(1)).toEqual({ Map: { on: { Bool: true } } });
});

test("a registered custom codec drives a Client", async () => {
  // toy custom *binary* codec: a 1-byte marker + utf-8 JSON
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  registerCodec(
    "application/x-test",
    (obj) => enc.encode("X" + JSON.stringify(obj)),
    (data) =>
      JSON.parse((typeof data === "string" ? data : dec.decode(data)).slice(1)),
  );
  try {
    const frame = enc.encode(
      "X" +
        JSON.stringify({
          t: "snapshot",
          id: 1,
          type: "Device",
          rev: 0,
          value: { Map: { on: { Bool: true } } },
        }),
    );
    const c = new Client("application/x-test");
    c.recv(frame); // decoded via the registered custom codec
    expect(c.value(1)).toEqual({ Map: { on: { Bool: true } } });
    expect(() => registerCodec("application/json", enc, dec)).toThrow();
  } finally {
    unregisterCodec("application/x-test");
  }
});

test("Client mirrors a snapshot then a patch", async () => {
  const c = new Client();
  expect(
    c.recv(
      JSON.stringify({
        t: "snapshot",
        id: 1,
        type: "Device",
        rev: 0,
        value: { Map: { on: { Bool: false } } },
      }),
    ),
  ).toEqual({ t: "snapshot", id: 1, rev: 0 });
  c.recv(
    JSON.stringify({
      t: "patch",
      id: 1,
      patch: {
        rev: 1,
        ops: [{ Set: { path: [{ Key: "on" }], value: { Bool: true } } }],
      },
    }),
  );
  expect(c.value(1)).toEqual({ Map: { on: { Bool: true } } });
});

test("Client ignores unknown message types and rejects unknown patch operations", async () => {
  const c = new Client();
  c.recv(
    JSON.stringify({
      t: "snapshot",
      id: 1,
      type: "Device",
      rev: 0,
      value: { Map: { on: { Bool: false } } },
    }),
  );
  // forward compatibility: a newer server's message types are ignored, not an error
  expect(c.recv(JSON.stringify({ t: "other", id: 1 }))).toBeUndefined();
  expect(() =>
    c.recv(
      JSON.stringify({
        t: "patch",
        id: 1,
        patch: { rev: 1, ops: [{ Other: {} }] },
      }),
    ),
  ).toThrow(/unknown patch op/);
  expect(c.value(1)).toEqual({ Map: { on: { Bool: false } } });
});

test("Client.onChange fires for accepted changes only", async () => {
  const c = new Client();
  const seen = [];
  const off = c.onChange((change) => seen.push(change));
  c.recv(
    JSON.stringify({
      t: "snapshot",
      id: 1,
      type: "Device",
      rev: 0,
      value: { Map: { on: { Bool: false } } },
    }),
  );
  const patch = {
    t: "patch",
    id: 1,
    patch: {
      rev: 1,
      ops: [{ Set: { path: [{ Key: "on" }], value: { Bool: true } } }],
    },
  };
  c.recv(JSON.stringify(patch));
  c.recv(JSON.stringify(patch)); // stale rev: ignored, no notification
  c.recv(JSON.stringify({ t: "other", id: 1 })); // unknown type: ignored, no notification
  expect(seen).toEqual([
    { t: "snapshot", id: 1, rev: 0 },
    { t: "patch", id: 1, patch: expect.objectContaining({ rev: 1 }) },
  ]);
  off();
  c.recv(
    JSON.stringify({
      t: "patch",
      id: 1,
      patch: {
        rev: 2,
        ops: [{ Set: { path: [{ Key: "on" }], value: { Bool: false } } }],
      },
    }),
  );
  expect(seen.length).toBe(2); // unsubscribed
});

test("Client.send drops when unconnected; propose sends the edit frame", async () => {
  const c = new Client();
  expect(c.connected).toBe(false);
  expect(c.send("x")).toBe(false); // dropped, not thrown: safe as a fire-and-forget callback
  expect(c.proposeOps(1, [], "dropped")).toBe(false);
  expect(c.pendingProposals()).toEqual([]);
  c.recv(
    JSON.stringify({
      t: "snapshot",
      id: 1,
      type: "Device",
      rev: 0,
      value: { Map: { on: { Bool: false } } },
    }),
  );
  const sent = [];
  c.send = (f) => sent.push(f) > 0; // stub the active-connection channel
  expect(c.propose(1, { Map: { on: { Bool: true } } }, "toggle-1")).toBe(true);
  expect(
    c.proposeOps(
      1,
      [{ Set: { path: [{ Key: "on" }], value: { Bool: false } } }],
      "toggle-2",
    ),
  ).toBe(true);
  expect(sent.length).toBe(2);
  const msg = JSON.parse(sent[0]);
  expect(msg.t).toBe("patch");
  expect(msg.proposal).toBe("toggle-1");
  expect(msg.patch.ops[0].Set.value).toEqual({ Bool: true });
  expect(JSON.parse(sent[1]).proposal).toBe("toggle-2");
});

test("Client.onReject surfaces a server rejection; the mirror is untouched", async () => {
  const c = new Client();
  const rejections = [];
  const off = c.onReject((r) => rejections.push(r));
  c.recv(
    JSON.stringify({
      t: "snapshot",
      id: 1,
      type: "Device",
      rev: 3,
      value: { Map: { brightness: { Int: 60 } } },
    }),
  );
  const result = c.recv(
    JSON.stringify({ t: "reject", id: 1, rev: 3, error: "not an int" }),
  );
  expect(result).toBeUndefined(); // a reject is not a change
  expect(rejections).toEqual([
    { t: "reject", id: 1, rev: 3, error: "not an int" },
  ]);
  expect(c.value(1)).toEqual({ Map: { brightness: { Int: 60 } } });
  off();
  c.recv(JSON.stringify({ t: "reject", id: 1, rev: 3, error: "again" }));
  expect(rejections.length).toBe(1); // unsubscribed
});

test("Client correlates accepted and rejected proposals", async () => {
  const c = new Client();
  c.recv(
    JSON.stringify({
      t: "snapshot",
      id: 1,
      type: "Device",
      rev: 3,
      value: { Map: { on: { Bool: false } } },
    }),
  );
  const acknowledgements = [];
  const rejections = [];
  c.onAck((ack) => acknowledgements.push(ack));
  c.onReject((reject) => rejections.push(reject));

  expect(
    c.recv(
      JSON.stringify({
        t: "ack",
        id: 1,
        rev: 99,
        proposal: "toggle-1",
      }),
    ),
  ).toBeUndefined();
  c.recv(
    JSON.stringify({
      t: "reject",
      id: 1,
      rev: 3,
      error: "invalid",
      proposal: "toggle-2",
    }),
  );

  expect(acknowledgements[0].proposal).toBe("toggle-1");
  expect(c.value(1)).toEqual({ Map: { on: { Bool: false } } });
  expect(rejections[0].proposal).toBe("toggle-2");
});

test("Client.editOps preserves an explicit proposal in msgpack", async () => {
  const c = new Client("msgpack");
  c.recv(
    jsonToMsgpack(
      JSON.stringify({
        t: "snapshot",
        id: 1,
        type: "Device",
        rev: 0,
        value: { Map: { on: { Bool: false } } },
      }),
    ),
  );

  const frame = c.editOps(
    1,
    [{ Set: { path: [{ Key: "on" }], value: { Bool: true } } }],
    "toggle-1",
  );
  expect(JSON.parse(msgpackToJson(frame)).proposal).toBe("toggle-1");
});

test("Client-generated proposal ids cannot collide with caller ids", async () => {
  const c = new Client();
  expect(JSON.parse(c.editOps(1, [], "1")).proposal).toBe("1");
  expect(JSON.parse(c.editOps(1, [])).proposal).toBe("auto-1");
  expect(() => c.editOps(1, [], "auto-2")).toThrow(/reserved/);
});

test("Client can abandon one unsent proposal", async () => {
  const c = new Client();
  const abandoned = [];
  c.onAbandon((proposals) => abandoned.push(proposals));
  c.editOps(1, [], "sent-elsewhere");

  expect(c.abandonProposal("missing")).toBe(false);
  expect(c.abandonProposal("sent-elsewhere")).toBe(true);
  expect(c.pendingProposals()).toEqual([]);
  expect(abandoned).toEqual([]);
});

test("Client exposes managed connection lifecycle", async () => {
  const NativeWebSocket = globalThis.WebSocket;
  class FakeSocket {
    constructor() {
      this.listeners = {};
    }
    addEventListener(name, listener) {
      this.listeners[name] = listener;
    }
    send() {}
    emit(name) {
      this.listeners[name]({});
    }
  }
  globalThis.WebSocket = FakeSocket;
  try {
    const c = new Client();
    const connections = [];
    const disconnects = [];
    const abandoned = [];
    const unsubscribe = c.onConnect(() => connections.push(c.connected));
    c.onDisconnect(() => disconnects.push(true));
    c.onAbandon((proposals) => abandoned.push(proposals));
    c.editOps(1, [], "editor-1");
    c.editOps(1, []);
    expect(c.pendingProposals()).toEqual(["auto-1", "editor-1"]);
    const socket = c.connect("ws://host/ws");
    socket.emit("open");
    expect(c.connected).toBe(true);
    expect(connections).toEqual([true]);
    socket.emit("close");
    expect(c.connected).toBe(false);
    expect(disconnects).toEqual([true]);
    expect(abandoned).toEqual([["auto-1", "editor-1"]]);
    expect(c.pendingProposals()).toEqual([]);

    const resumed = c.connect("ws://host/ws");
    resumed.emit("open");
    expect(connections).toEqual([true, true]);
    unsubscribe();
    resumed.emit("close");

    const ignored = c.connect("ws://host/ws");
    ignored.emit("open");
    expect(connections).toEqual([true, true]);
  } finally {
    globalThis.WebSocket = NativeWebSocket;
  }
});

test("Client.run reports every reconnect without waiting for a frame", async () => {
  const NativeWebSocket = globalThis.WebSocket;
  const sockets = [];
  class FakeSocket {
    constructor() {
      this.listeners = {};
      sockets.push(this);
    }
    addEventListener(name, listener) {
      (this.listeners[name] ??= []).push(listener);
    }
    send() {}
    close() {
      this.emit("close");
    }
    emit(name) {
      for (const listener of this.listeners[name] ?? []) listener({});
    }
  }
  globalThis.WebSocket = FakeSocket;
  try {
    const c = new Client();
    const connections = [];
    c.onConnect(() => connections.push(c.connected));
    const runner = c.run("ws://host/ws", { retry: 0 });

    sockets[0].emit("open");
    expect(connections).toEqual([true]);
    sockets[0].emit("close");
    await expect.poll(() => sockets.length).toBe(2);
    sockets[1].emit("open");
    expect(connections).toEqual([true, true]);

    runner.stop();
    sockets[1].emit("close");
  } finally {
    globalThis.WebSocket = NativeWebSocket;
  }
});

test("Client connection listeners ignore receive-only SSE", async () => {
  const NativeEventSource = globalThis.EventSource;
  class FakeSource {
    addEventListener() {}
  }
  globalThis.EventSource = FakeSource;
  try {
    const c = new Client();
    const connections = [];
    c.onConnect(() => connections.push(true));
    c.connectSSE("http://host/sse");
    expect(connections).toEqual([]);
    expect(c.connected).toBe(false);
  } finally {
    globalThis.EventSource = NativeEventSource;
  }
});

test("Client patch application matches the wasm core apply", async () => {
  // differential test: the mirror is maintained by the pure-TS applyPatch, not the fuzz-tested
  // core — pin the two implementations together across randomized diffs. Seeded PRNG (mulberry32)
  // so a failure reproduces.
  let seed = 0x1b6b92;
  const rand = () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const int = (n) => Math.floor(rand() * n);
  const scalar = () => {
    const pick = int(4);
    if (pick === 0) return { Int: int(100) };
    if (pick === 1) return { Str: `s${int(100)}` };
    if (pick === 2) return { Bool: rand() < 0.5 };
    return "Null";
  };
  const value = (depth) => {
    if (depth <= 0 || rand() < 0.3) return scalar();
    if (rand() < 0.5) {
      const map = {};
      for (let i = 1 + int(4); i > 0; i--) map[`k${int(6)}`] = value(depth - 1);
      return { Map: map };
    }
    return { List: Array.from({ length: int(4) }, () => value(depth - 1)) };
  };
  // perturb a copy: replace/drop/add branches so diffs mix Set/Remove/Insert/RemoveAt and
  // type changes
  const mutate = (v) => {
    if (rand() < 0.2) return value(2);
    if (v && typeof v === "object" && "Map" in v) {
      const map = {};
      for (const [k, child] of Object.entries(v.Map))
        if (rand() >= 0.15) map[k] = mutate(child);
      if (rand() < 0.3) map[`k${int(6)}`] = value(2);
      return { Map: map };
    }
    if (v && typeof v === "object" && "List" in v) {
      const list = v.List.filter(() => rand() >= 0.15).map(mutate);
      if (rand() < 0.3) list.splice(int(list.length + 1), 0, value(2));
      return { List: list };
    }
    return rand() < 0.3 ? scalar() : v;
  };
  for (let i = 0; i < 300; i++) {
    const a = { Map: { root: value(3) } };
    const b = mutate(a);
    const patch = JSON.parse(diff(JSON.stringify(a), JSON.stringify(b)));
    const viaWasm = JSON.parse(apply(JSON.stringify(a), JSON.stringify(patch)));
    const c = new Client();
    c.recv(
      JSON.stringify({ t: "snapshot", id: 1, type: "M", rev: 0, value: a }),
    );
    c.recv(JSON.stringify({ t: "patch", id: 1, patch: { ...patch, rev: 1 } }));
    expect(c.value(1)).toEqual(viaWasm);
    expect(viaWasm).toEqual(b); // the core round-trip property, from JS
  }
});

test("Client applies patches without rebuilding unchanged branches", async () => {
  const c = new Client();
  const snapshot = {
    Map: {
      profile: {
        Map: { name: { Str: "old" }, obsolete: { Bool: true } },
      },
      rows: { List: [{ Int: 1 }, { Int: 2 }] },
      untouched: { Map: { value: { Str: "same" } } },
    },
  };
  c.recv(
    JSON.stringify({
      t: "snapshot",
      id: 1,
      type: "Model",
      rev: 0,
      value: snapshot,
    }),
  );
  const before = c.value(1);
  const unchanged = before.Map.untouched;
  const change = c.recv(
    JSON.stringify({
      t: "patch",
      id: 1,
      patch: {
        rev: 1,
        ops: [
          {
            Set: {
              path: [{ Key: "profile" }, { Key: "name" }],
              value: { Str: "new" },
            },
          },
          { Remove: { path: [{ Key: "profile" }, { Key: "obsolete" }] } },
          {
            Insert: {
              path: [{ Key: "rows" }],
              index: 1,
              value: { Int: 5 },
            },
          },
          { RemoveAt: { path: [{ Key: "rows" }], index: 0 } },
        ],
      },
    }),
  );

  expect(change).toEqual({
    t: "patch",
    id: 1,
    patch: expect.objectContaining({ rev: 1 }),
  });
  expect(c.value(1)).toEqual({
    Map: {
      profile: { Map: { name: { Str: "new" } } },
      rows: { List: [{ Int: 5 }, { Int: 2 }] },
      untouched: { Map: { value: { Str: "same" } } },
    },
  });
  expect(c.value(1).Map.untouched).toBe(unchanged);
  expect(c.value(1)).not.toBe(before);
});

test("Client applies forward and backward list moves with structural sharing", async () => {
  const c = new Client();
  const rows = ["a", "b", "c", "d"].map((id) => ({
    Map: { id: { Str: id } },
  }));
  c.recv(
    JSON.stringify({
      t: "snapshot",
      id: 1,
      type: "Model",
      rev: 0,
      value: { Map: { rows: { List: rows } } },
    }),
  );
  const before = c.value(1);
  const moved = before.Map.rows.List[0];

  c.recv(
    JSON.stringify({
      t: "patch",
      id: 1,
      patch: {
        rev: 1,
        ops: [
          { Move: { path: [{ Key: "rows" }], from: 0, to: 3 } },
          { Move: { path: [{ Key: "rows" }], from: 2, to: 0 } },
        ],
      },
    }),
  );

  expect(c.value(1).Map.rows.List.map((row) => row.Map.id.Str)).toEqual([
    "d",
    "b",
    "c",
    "a",
  ]);
  expect(c.value(1).Map.rows.List[3]).toBe(moved);
  expect(c.value(1)).not.toBe(before);
});

test("Client applies and validates a complete reorder atomically", async () => {
  const c = new Client();
  const rows = ["a", "b", "c", "d"].map((id) => ({
    Map: { id: { Str: id } },
  }));
  c.recv(
    JSON.stringify({
      t: "snapshot",
      id: 1,
      type: "Model",
      rev: 0,
      value: { List: rows },
    }),
  );
  const before = c.value(1);

  for (const order of [
    [0, 1, 2],
    [0, 1, 2, 4],
    [0, 1, 1, 3],
  ]) {
    expect(() =>
      c.recv(
        JSON.stringify({
          t: "patch",
          id: 1,
          patch: {
            rev: 1,
            ops: [
              { Set: { path: [{ Index: 0 }], value: { Str: "changed" } } },
              { Reorder: { path: [], order } },
            ],
          },
        }),
      ),
    ).toThrow(/reorder/);
    expect(c.value(1)).toBe(before);
  }

  c.recv(
    JSON.stringify({
      t: "patch",
      id: 1,
      patch: {
        rev: 1,
        ops: [{ Reorder: { path: [], order: [3, 1, 0, 2] } }],
      },
    }),
  );
  expect(c.value(1).List.map((row) => row.Map.id.Str)).toEqual([
    "d",
    "b",
    "a",
    "c",
  ]);
  expect(c.value(1).List[2]).toBe(before.List[0]);
});

test("Client rejects a malformed patch atomically", async () => {
  const c = new Client();
  const value = { Map: { rows: { List: [{ Int: 1 }] } } };
  c.recv(
    JSON.stringify({
      t: "snapshot",
      id: 1,
      type: "Model",
      rev: 0,
      value,
    }),
  );
  const before = c.value(1);

  expect(() =>
    c.recv(
      JSON.stringify({
        t: "patch",
        id: 1,
        patch: {
          rev: 1,
          ops: [
            {
              Set: {
                path: [{ Key: "rows" }, { Index: 0 }],
                value: { Int: 2 },
              },
            },
            { RemoveAt: { path: [{ Key: "rows" }], index: 9 } },
          ],
        },
      }),
    ),
  ).toThrow(/out of bounds/);
  expect(c.value(1)).toBe(before);
  expect(() =>
    c.recv(
      JSON.stringify({
        t: "patch",
        id: 1,
        patch: {
          rev: 1,
          ops: [
            {
              Set: {
                path: [{ Key: "rows" }, { Index: 0 }],
                value: { Int: 2 },
              },
            },
            { Move: { path: [{ Key: "rows" }], from: 0, to: 9 } },
          ],
        },
      }),
    ),
  ).toThrow(/move destination index 9 out of bounds/);
  expect(c.value(1)).toBe(before);
  for (const op of [
    { Insert: { path: [{ Key: "rows" }], index: 0.5, value: { Int: 2 } } },
    { RemoveAt: { path: [{ Key: "rows" }], index: 0.5 } },
    { Move: { path: [{ Key: "rows" }], from: 0, to: 0.5 } },
    { Reorder: { path: [{ Key: "rows" }], order: [0.5] } },
  ]) {
    expect(() =>
      c.recv(
        JSON.stringify({ t: "patch", id: 1, patch: { rev: 1, ops: [op] } }),
      ),
    ).toThrow(/index/);
    expect(c.value(1)).toBe(before);
  }
  expect(
    c.recv(
      JSON.stringify({
        t: "patch",
        id: 1,
        patch: {
          rev: 1,
          ops: [
            {
              Set: {
                path: [{ Key: "rows" }, { Index: 0 }],
                value: { Int: 3 },
              },
            },
          ],
        },
      }),
    ),
  ).toEqual({
    t: "patch",
    id: 1,
    patch: expect.objectContaining({ rev: 1 }),
  });
  expect(c.value(1)).toEqual({ Map: { rows: { List: [{ Int: 3 }] } } });
});

test("Client.edit is send-only; mirror updates on the server echo", async () => {
  const c = new Client();
  c.recv(
    JSON.stringify({
      t: "snapshot",
      id: 1,
      type: "Device",
      rev: 0,
      value: { Map: { on: { Bool: false } } },
    }),
  );
  const frame = c.edit(1, { Map: { on: { Bool: true } } });
  const msg = JSON.parse(frame);
  expect(msg.t).toBe("patch");
  // server-authoritative: edit does not mutate the local mirror...
  expect(c.value(1)).toEqual({ Map: { on: { Bool: false } } });
  // ...the mirror updates when the server echoes the authoritative patch back. The server owns rev
  // and bumps it past the mirror's; a patch at or below the mirror's rev is ignored as already-applied.
  c.recv(
    JSON.stringify({ t: "patch", id: 1, patch: { ...msg.patch, rev: 1 } }),
  );
  expect(c.value(1)).toEqual({ Map: { on: { Bool: true } } });
});
