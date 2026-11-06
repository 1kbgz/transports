import { test, expect } from "@playwright/test";
import fs from "node:fs";
import { initSync, BufferFrame } from "../dist/pkg/transports";
import {
  Client,
  CrdtSpec,
  CrdtDocument,
  toValue,
  fromValue,
  encodeAs,
  decodeAs,
  encodeMessage,
  splitBuffers,
  joinBuffers,
} from "../src/ts/index";
import { stringify } from "../src/ts/bridge";

test.beforeAll(() =>
  initSync({ module: fs.readFileSync("./dist/pkg/transports_bg.wasm") }),
);

test("bytes preserve view offsets and use the same golden codecs as Python", () => {
  const storage = new Uint8Array([99, 0, 255, 1, 99]);
  const view = new DataView(storage.buffer, 1, 3);
  const value = toValue(view);
  const restored = fromValue(value);
  expect(restored).toEqual(new Uint8Array([0, 255, 1]));
  expect(restored.buffer).toBe(storage.buffer);
  const json = stringify(value);
  expect(json).toBe('{"Bytes":"AP8B"}');
  for (const [codec, hex] of [
    ["application/msgpack", "81a54279746573c40300ff01"],
    ["application/cbor", "a16542797465734300ff01"],
  ]) {
    const wire = encodeAs(json, codec);
    expect(Buffer.from(wire).toString("hex")).toBe(hex);
    expect(decodeAs(wire, codec)).toBe(json);
  }
});

for (const codec of ["json", "msgpack", "cbor"]) {
  test(`client round-trips binary fields over ${codec}`, () => {
    const client = new Client(codec);
    const message = {
      t: "snapshot",
      id: 1,
      type: "Sample",
      rev: 0,
      value: toValue({ data: new Uint8Array([0, 255]) }),
    };
    const json = stringify(message);
    client.recv(codec === "json" ? json : encodeMessage(json, codec));
    expect(fromValue(client.value(1)).data).toEqual(new Uint8Array([0, 255]));
    const edit = client.edit(1, toValue({ data: new Uint8Array([1, 255, 0]) }));
    const frame =
      typeof edit === "string"
        ? BufferFrame.from_json(edit)
        : BufferFrame.decode(edit, codec);
    const msg = JSON.parse(frame.to_json());
    msg.patch.rev = 1;
    const accepted = JSON.stringify(msg);
    client.recv(codec === "json" ? accepted : encodeMessage(accepted, codec));
    expect(fromValue(client.value(1)).data).toEqual(
      new Uint8Array([1, 255, 0]),
    );
    frame.free();
  });
}

test("comm buffers avoid JSON payloads and return the original view", () => {
  const data = new Uint8Array(10 * 1024 * 1024).fill(255);
  const message = {
    t: "snapshot",
    id: 1,
    type: "Sample",
    rev: 0,
    value: toValue({ data }),
  };
  const { header, buffers } = splitBuffers(message);
  expect(JSON.stringify(header).length).toBeLessThan(300);
  expect(buffers).toHaveLength(1);
  expect(buffers[0].buffer).toBe(data.buffer);
  const client = new Client();
  client.recvBuffers(header, buffers);
  const restored = fromValue(client.value(1)).data;
  expect(restored.buffer).toBe(data.buffer);
  expect(restored.byteLength).toBe(data.byteLength);
  expect(() => joinBuffers(header, [])).toThrow(/buffer count/);
  const bad = structuredClone(header);
  bad.buffer_paths.push(bad.buffer_paths[0]);
  expect(() => joinBuffers(bad, [data, data])).toThrow(/placeholder/);
});

for (const codec of ["json", "msgpack", "cbor"]) {
  test(`binary CRDT updates and snapshots over ${codec}`, () => {
    const spec = new CrdtSpec({ kind: "map" });
    const document = new CrdtDocument(
      spec,
      { data: new Uint8Array([0, 255]) },
      "server",
    );
    const client = new Client(codec);
    const snapshot = stringify({
      t: "crdt_snapshot",
      id: 9,
      type: "Sample",
      rev: 0,
      value: toValue(document.value),
      spec: spec.toObject(),
      state: document.state,
    });
    client.recv(codec === "json" ? snapshot : encodeMessage(snapshot, codec));
    expect(fromValue(client.value(9)).data).toEqual(new Uint8Array([0, 255]));
    const data = new Uint8Array(1024 * 1024).fill(255);
    const wire = client.editCrdt(9, [
      {
        kind: "register_set",
        path: [{ kind: "key", key: "data" }],
        value: data,
      },
    ]);
    const frame =
      typeof wire === "string"
        ? BufferFrame.from_json(wire)
        : BufferFrame.decode(wire, codec);
    expect(frame.count()).toBe(1);
    expect(frame.buffer(0)).toEqual(data);
    const message = JSON.parse(frame.to_json());
    message.rev = 1;
    const accepted = JSON.stringify(message);
    client.recv(codec === "json" ? accepted : encodeMessage(accepted, codec));
    expect(fromValue(client.value(9)).data).toEqual(data);
    expect(client.pendingCrdtOps(9)).toBe(0);
    frame.free();
  });
}
