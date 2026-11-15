import { test, expect } from "@playwright/test";
import fs from "node:fs";
import { initSync, FieldAuthority } from "../dist/pkg/transports";
import {
  Client,
  CrdtDocument,
  CrdtSpec,
  encodeMessage,
  toValue,
} from "../src/ts/index";

test.beforeAll(() =>
  initSync({ module: fs.readFileSync("./dist/pkg/transports_bg.wasm") }),
);

for (const codec of ["json", "msgpack", "cbor"]) {
  test(`retire models and pending state over ${codec}`, () => {
    const client = new Client(codec);
    const recv = (msg) =>
      client.recv(
        codec === "json"
          ? JSON.stringify(msg)
          : encodeMessage(JSON.stringify(msg), codec),
      );
    const snapshot = (id) => ({
      t: "snapshot",
      id,
      type: "Counter",
      rev: 0,
      value: toValue({ count: 0 }),
    });
    recv(snapshot(1));
    recv(snapshot(2));
    client.editOps(1, [], "retired-edit");
    client.editOps(2, [], "live-edit");
    const changes = [],
      abandoned = [],
      presence = [];
    client.onChange((change) => changes.push(change));
    client.onAbandon((ids) => abandoned.push(ids));
    client.onAwareness((update) => presence.push(update), { scope: "cursor" });
    recv({
      t: "scoped_awareness",
      id: 1,
      scope: "cursor",
      peer: "bob",
      state: { x: 1 },
    });
    expect(recv({ t: "batch", msgs: [{ t: "models", ids: [2] }] })).toEqual([
      { t: "remove", id: 1 },
    ]);
    expect(changes).toEqual([{ t: "remove", id: 1 }]);
    expect(abandoned).toEqual([["retired-edit"]]);
    expect(client.ids()).toEqual([2]);
    expect(client.value(1)).toBeUndefined();
    expect(client.typeName(1)).toBeUndefined();
    expect(client.pendingProposals()).toEqual(["live-edit"]);
    expect(presence.at(-1).state).toBeNull();
    expect(recv({ t: "models", ids: [2] })).toBeUndefined();
  });
}

test("retirement discards offline CRDT edits and ownership commands", () => {
  const client = new Client();
  const spec = new CrdtSpec({ kind: "sequence", materialization: "string" });
  const doc = new CrdtDocument(spec, "", "server");
  client.recv(
    JSON.stringify({
      t: "crdt_snapshot",
      id: 1,
      type: "Text",
      rev: 0,
      value: toValue(""),
      spec: spec.toObject(),
      state: doc.state,
    }),
  );
  client.editCrdt(1, [
    { kind: "sequence_insert", path: [], after: null, values: ["a"] },
  ]);
  const authority = new FieldAuthority(2n, '["status"]', 16);
  client.recv(
    JSON.stringify({
      t: "snapshot",
      id: 2,
      type: "Owner",
      rev: 0,
      value: toValue({ status: 0 }),
    }),
  );
  client.recv(authority.open("owner"));
  authority.command("status", JSON.stringify(toValue(2)));
  const command = authority.next_command();
  client.recv(command);
  client.recv(JSON.stringify({ t: "models", ids: [] }));
  expect(client.pendingCrdtOps()).toBe(0);
  expect(client.crdtSpec(1)).toBeUndefined();
  expect(client.owns(2, "status")).toBe(false);
  expect(() => client.commandAck(JSON.parse(command))).toThrow(/not pending/);
  const commands = [];
  client.onCommand((value) => commands.push(value));
  expect(commands).toEqual([]);
  authority.free();
});
