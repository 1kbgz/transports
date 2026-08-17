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
  registerCodec,
  unregisterCodec,
  Client,
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

test("Client.send requires a connection; propose sends the edit frame", async () => {
  const c = new Client();
  expect(() => c.send("x")).toThrow(/not connected/);
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
  c.send = (f) => sent.push(f); // stub the active-connection channel
  c.propose(1, { Map: { on: { Bool: true } } });
  expect(sent.length).toBe(1);
  const msg = JSON.parse(sent[0]);
  expect(msg.t).toBe("patch");
  expect(msg.patch.ops[0].Set.value).toEqual({ Bool: true });
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
