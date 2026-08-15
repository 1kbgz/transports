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

test("Client rejects unknown messages and patch operations", async () => {
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
  expect(() => c.recv(JSON.stringify({ t: "other", id: 1 }))).toThrow(
    /unknown message type/,
  );
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
