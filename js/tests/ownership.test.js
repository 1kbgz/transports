import { test, expect } from "@playwright/test";
import fs from "node:fs";
import { initSync, FieldAuthority } from "../dist/pkg/transports";
import { Client, fromValue, toValue } from "../src/ts/index";

test.beforeAll(() =>
  initSync({ module: fs.readFileSync("./dist/pkg/transports_bg.wasm") }),
);

const playing = (value) => [
  { Set: { path: [{ Key: "playing" }], value: { Bool: value } } },
];
function seed(client) {
  client.recv(
    JSON.stringify({
      t: "snapshot",
      id: 1,
      type: "Clock",
      rev: 0,
      value: toValue({ playing: false, bpm: 120 }),
    }),
  );
}

test("owned edits are immediate, observers cannot edit, and mixed authority is rejected", () => {
  const authority = new FieldAuthority(1n, '["playing"]', 16);
  const owner = new Client(),
    observer = new Client();
  seed(owner);
  seed(observer);
  owner.recv(authority.open("owner"));
  observer.recv(authority.open("observer"));
  const edit = owner.editOps(1, playing(true));
  expect(fromValue(owner.value(1)).playing).toBe(true);
  expect(owner.pendingProposals()).toEqual([]);
  expect(authority.validate("owner", edit)).toBe(true);
  expect(() => observer.editOps(1, playing(true))).toThrow(/does not own/);
  expect(() =>
    owner.editOps(1, [
      ...playing(false),
      { Set: { path: [{ Key: "bpm" }], value: { Int: 100 } } },
    ]),
  ).toThrow(/split edits/);
  expect(fromValue(owner.value(1)).playing).toBe(true);
  const ack = { t: "owned_ack", id: 1, epoch: 1, sequence: 1, rev: 1 };
  owner.recv(JSON.stringify(ack));
  // An acknowledgement advances revisions without replacing a newer local status.
  owner.editOps(1, playing(false));
  owner.recv(JSON.stringify(ack));
  expect(fromValue(owner.value(1)).playing).toBe(false);
  authority.free();
});

test("status edits coalesce in Rust and timers stop on disconnect", async () => {
  const authority = new FieldAuthority(1n, '["playing"]', 16);
  const client = new Client("json", { updateInterval: 0.02 });
  seed(client);
  client.recv(authority.open("owner"));
  const sent = [];
  const sender = (frame) => sent.push(JSON.parse(frame));
  client.attach(sender);
  for (let i = 0; i < 1000; i++) client.proposeOps(1, playing(i % 2 === 0));
  expect(sent).toEqual([]);
  expect(fromValue(client.value(1)).playing).toBe(false);
  await new Promise((resolve) => setTimeout(resolve, 35));
  expect(sent).toHaveLength(1);
  expect(sent[0].patch.ops).toEqual(playing(false));
  expect(sent[0].sequence).toBe(1000);
  client.proposeOps(1, playing(true));
  client.detach(sender);
  await new Promise((resolve) => setTimeout(resolve, 35));
  expect(sent).toHaveLength(1);
  expect(client.owns(1, "playing")).toBe(false);
  authority.free();
});

test("commands wait for a listener and remain ordered through owner handover", () => {
  const authority = new FieldAuthority(1n, '["playing"]', 16);
  authority.command("playing", '{"Bool":true}');
  authority.command("playing", '{"Bool":false}');
  const first = new Client(),
    second = new Client();
  seed(first);
  seed(second);
  first.recv(authority.open("first"));
  second.recv(authority.open("second"));
  first.recv(authority.next_command());
  const commands = [];
  first.onCommand((command) => commands.push(command));
  expect(commands).toHaveLength(1);
  const ack = JSON.parse(first.commandAck(structuredClone(commands[0])));
  authority.acknowledge("first", BigInt(ack.epoch), BigInt(ack.command));
  first.recv(authority.next_command());
  expect(commands).toHaveLength(2);
  authority.close("first");
  second.recv(authority.grant("second"));
  const other = [];
  second.onCommand((command) => other.push(command));
  second.recv(authority.next_command());
  expect(other[0].command).toBe(commands[1].command);
  expect(other[0].epoch).toBeGreaterThan(commands[1].epoch);
  expect(() => second.commandAck(commands[1])).toThrow(/not pending/);
  authority.free();
});

test("immediate edits consume pending status without sending old values later", () => {
  const client = new Client();
  seed(client);
  const authority = new FieldAuthority(1n, '["playing"]', 10);
  client.recv(authority.open("owner"));
  const sent = [];
  const sender = (frame) => sent.push(frame);
  client.attach(sender);
  client.proposeOps(1, [
    { Set: { path: [{ Key: "playing" }], value: { Bool: true } } },
  ]);
  const immediate = JSON.parse(
    client.editOps(1, [
      { Set: { path: [{ Key: "playing" }], value: { Bool: false } } },
    ]),
  );
  expect(immediate.patch.ops[0].Set.value).toEqual({ Bool: false });
  client.flushOwned();
  expect(sent).toEqual([]);
  client.detach(sender);
  authority.free();
});

test("managed command acknowledgements flush status first", () => {
  const authority = new FieldAuthority(1n, '["playing"]', 10);
  authority.command("playing", '{"Bool":true}');
  const client = new Client();
  seed(client);
  client.recv(authority.open("owner"));
  client.recv(authority.next_command());
  const commands = [],
    sent = [];
  client.onCommand((command) => commands.push(command));
  const sender = (wire) => sent.push(JSON.parse(wire).t);
  client.attach(sender);
  client.proposeOps(1, playing(true));
  client.ackCommand(commands[0]);
  expect(sent).toEqual(["owned", "command_ack"]);
  client.detach(sender);
  authority.free();
});

test("client-authority reconciliation leaves owned status to its grant", () => {
  const authority = new FieldAuthority(1n, '["playing"]', 10);
  const client = new Client();
  seed(client);
  client.recv(authority.open("owner"));
  const frame = JSON.parse(
    client.reconcile(1, toValue({ playing: true, bpm: 140 })),
  );
  expect(frame.patch.ops).toEqual([
    { Set: { path: [{ Key: "bpm" }], value: { Int: 140 } } },
  ]);
  authority.free();
});
