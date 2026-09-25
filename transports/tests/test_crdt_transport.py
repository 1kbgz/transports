from __future__ import annotations

import asyncio
import json

import pytest

from transports import WRITE, Client, CrdtDocument, CrdtSpec, Hub, RelayBroadcaster, autosync, protocol
from transports.backplane import Backplane


def text_spec() -> CrdtSpec:
    return CrdtSpec({"kind": "sequence", "materialization": "string"})


def insert(text: str) -> list[dict]:
    return [{"kind": "sequence_insert", "path": [], "after": None, "values": list(text)}]


def splice(index: int, text: str = "", delete_count: int = 0) -> list[dict]:
    return [
        {
            "kind": "sequence_splice",
            "path": [],
            "index": index,
            "delete_count": delete_count,
            "values": list(text),
        }
    ]


def crdt_hub() -> tuple[Hub, int]:
    hub = Hub(key=lambda conn: conn[0])
    sid = hub.share({"Str": ""}, "Document", crdt_spec=text_spec())
    return hub, sid


def test_hub_and_clients_exchange_concurrent_crdt_operations() -> None:
    hub, sid = crdt_hub()
    connections = [("alice", 1), ("bob", 1)]
    clients = [Client(), Client()]
    for connection, client in zip(connections, clients):
        hub.subscribe(connection[0], sid, WRITE)
        [snapshot] = hub.open(connection)
        assert json.loads(snapshot)["t"] == "crdt_snapshot"
        client.recv(snapshot)

    frames = [clients[0].edit_crdt(sid, insert("hi")), clients[1].edit_crdt(sid, insert("!"))]
    reversed_client = Client()
    reversed_client.recv(hub.open(("alice", 2))[0])
    authoritative = []
    for connection, frame in zip(connections, frames):
        output = hub.recv(connection, frame)
        authoritative.append(json.loads(output[connection][0]))
        for target, client in zip(connections, clients):
            for message in output[target]:
                client.recv(message)

    reversed_client.recv(json.dumps(authoritative[1]))
    reversed_client.recv(json.dumps(authoritative[0]))

    assert clients[0].value(sid) == clients[1].value(sid) == reversed_client.value(sid) == hub._shared[sid].value
    assert clients[0].pending_crdt_ops() == clients[1].pending_crdt_ops() == 0

    revision = hub._shared[sid].rev
    duplicate = hub.recv(connections[0], frames[0])
    assert json.loads(duplicate[connections[0]][0])["t"] == "crdt"
    assert hub._shared[sid].rev == revision


def test_crdt_client_reapplies_offline_operations_after_snapshot() -> None:
    hub, sid = crdt_hub()
    hub.subscribe("alice", sid, WRITE)
    connection = ("alice", 1)
    client = Client()
    [snapshot] = hub.open(connection)
    client.recv(snapshot)
    assert client.crdt_spec(sid) == text_spec()

    frame = client.edit_crdt(sid, insert("offline"))
    assert client.value(sid) == {"Str": "offline"}
    assert client.pending_crdt_ops(sid) == 1

    hub.recv(connection, frame)  # accepted, but the connection drops before its echo arrives
    [snapshot] = hub.open(connection)
    client.recv(snapshot)
    assert client.value(sid) == {"Str": "offline"}
    output = hub.recv(connection, frame)
    client.recv(output[connection][0])
    assert client.pending_crdt_ops(sid) == 0
    assert client.value(sid) == hub._shared[sid].value


def test_client_exposes_specs_only_for_crdt_models() -> None:
    hub, sid = crdt_hub()
    hub.subscribe("alice", sid, WRITE)
    client = Client()
    client.recv(hub.open(("alice", 1))[0])

    assert client.crdt_spec(sid) == text_spec()
    assert client.crdt_spec(999) is None


def test_managed_client_sends_and_flushes_crdt_outbox() -> None:
    async def run() -> None:
        hub, sid = crdt_hub()
        hub.subscribe("alice", sid, WRITE)
        client = Client()
        for message in hub.open(("alice", 1)):
            client.recv(message)
        sent = []
        sender = sent.append
        await client.attach(sender)

        assert await client.propose_crdt(sid, insert("live")) is True
        assert protocol.decode(sent[0])["t"] == "crdt"

        offline = Client()
        for message in hub.open(("alice", 2)):
            offline.recv(message)
        offline.edit_crdt(sid, insert("queued"))

        async def failing_sender(_frame: str | bytes) -> None:
            raise RuntimeError("channel failed")

        with pytest.raises(RuntimeError, match="channel failed"):
            await offline.attach(failing_sender)
        assert offline.connected is False
        assert offline.pending_crdt_ops(sid) == 1

        first_flush = []
        first_sender = first_flush.append
        await offline.attach(first_sender)
        assert protocol.decode(first_flush[0])["t"] == "crdt"

        assert offline.detach(first_sender) is True
        second_flush = []
        second_sender = second_flush.append
        await offline.attach(second_sender)
        assert second_flush == first_flush

    asyncio.run(run())


def test_client_keeps_other_models_queued_and_ignores_crdt_ops_for_plain_models() -> None:
    hub = Hub(key=lambda conn: conn)
    first = hub.share({"Str": ""}, "First", crdt_spec=text_spec())
    second = hub.share({"Str": ""}, "Second", crdt_spec=text_spec())
    client = Client()
    hub.subscribe("client", first, WRITE)
    hub.subscribe("client", second, WRITE)
    for message in hub.open("client"):
        client.recv(message)
    first_frame = client.edit_crdt(first, insert("a"))
    client.edit_crdt(second, insert("b"))

    for message in hub.recv("client", first_frame)["client"]:
        client.recv(message)
    assert client.pending_crdt_ops(first) == 0
    assert client.pending_crdt_ops(second) == 1

    with pytest.raises(KeyError, match="not CRDT-backed"):
        Client().edit_crdt(99, insert("x"))
    ordinary = Client()
    ordinary.recv(protocol.snapshot_msg(1, "Doc", 0, {"Str": ""}))
    assert ordinary.recv(protocol.crdt_msg(1, [])) is None


def test_plain_snapshot_clears_crdt_document_and_outbox() -> None:
    hub, sid = crdt_hub()
    hub.subscribe("client", sid, WRITE)
    client = Client()
    for message in hub.open(("client", 1)):
        client.recv(message)
    client.edit_crdt(sid, insert("pending"))

    client.recv(protocol.snapshot_msg(sid, "Document", 1, {"Str": "plain"}))

    assert client.pending_crdt_ops(sid) == 0
    assert client.value(sid) == {"Str": "plain"}
    assert client.crdt_spec(sid) is None
    with pytest.raises(KeyError, match="not CRDT-backed"):
        client.edit_crdt(sid, insert("stale"))


def test_crdt_shared_state_round_trips_through_durability_hook() -> None:
    hub, sid = crdt_hub()
    stored = {}
    hub.on_shared_write(
        lambda shared_id, type_name, value, rev, change, merge_state: stored.update(value=value, rev=rev, change=change, merge_state=merge_state)
    )
    hub.mutate_shared_crdt(sid, insert("saved"))

    restored = Hub(key=lambda conn: conn)
    restored_sid = restored.share(
        stored["value"],
        "Document",
        rev=stored["rev"],
        merge_state=stored["merge_state"],
    )
    assert restored.snapshot_shared(restored_sid) == hub.snapshot_shared(sid)
    assert stored["change"]["crdt_ops"]


def test_crdt_snapshot_checkpoint_restores_reducer_state() -> None:
    hub, sid = crdt_hub()
    hub.mutate_shared_crdt(sid, insert("saved"))
    checkpoint = hub.snapshot_shared(sid)

    restored = Hub(key=lambda conn: conn)
    restored_sid = restored.share(
        checkpoint["value"],
        checkpoint["type_name"],
        rev=checkpoint["rev"],
        merge_state=checkpoint["merge_state"],
    )
    restored.subscribe("client", restored_sid)

    assert restored._shared[restored_sid].crdt is not None
    assert json.loads(restored.open("client")[0])["t"] == "crdt_snapshot"
    assert restored.snapshot_shared(restored_sid) == checkpoint


def test_crdt_model_rejects_legacy_patch_writes() -> None:
    hub, sid = crdt_hub()
    hub.subscribe("alice", sid, WRITE)
    connection = ("alice", 1)
    hub.open(connection)

    output = hub.recv(connection, protocol.patch_msg(sid, {"rev": 0, "ops": []}))

    assert json.loads(output[connection][0])["error"] == "CRDT-backed models require CRDT operations"


def test_hub_rejects_crdt_operations_for_legacy_and_private_models() -> None:
    hub = Hub(key=lambda conn: conn)
    sid = hub.share({"Str": ""}, "Legacy")
    hub.subscribe("client", sid, WRITE)
    hub.open("client")

    shared = hub.recv("client", protocol.crdt_msg(sid, []))
    private = hub.recv("client", protocol.crdt_msg(1, []))

    assert json.loads(shared["client"][0])["error"] == "model is not CRDT-backed"
    assert json.loads(private["client"][0])["error"] == "CRDT operations require a shared model"
    crdt_sid = hub.share({"Str": ""}, "CRDT", crdt_spec=text_spec())
    with pytest.raises(ValueError, match="mutate_shared_crdt"):
        hub.set_shared(crdt_sid, {"Str": "changed"})
    with pytest.raises(ValueError, match="not CRDT-backed"):
        hub.mutate_shared_crdt(sid, [])
    with pytest.raises(ValueError, match="cannot be combined"):
        hub.share({"Str": ""}, "Invalid", crdt_spec=text_spec(), merge_state={})
    hub.apply_crdt_shared(99, [])


def test_host_crdt_mutation_flushes_and_proposals_echo_only_to_origin() -> None:
    hub, sid = crdt_hub()
    connections = [("alice", 1), ("bob", 1)]
    clients = [Client(), Client()]
    for connection, client in zip(connections, clients):
        hub.subscribe(connection[0], sid, WRITE)
        for message in hub.open(connection):
            client.recv(message)

    hub.mutate_shared_crdt(sid, insert("host"))
    flushed = hub.flush()
    assert set(flushed) == set(connections)

    message = protocol.decode(clients[0].edit_crdt(sid, insert("client")))
    message["proposal"] = "editor-1"
    output = hub.recv(connections[0], json.dumps(message))
    assert json.loads(output[connections[0]][0])["proposal"] == "editor-1"
    assert "proposal" not in json.loads(output[connections[1]][0])


def test_hubs_use_distinct_replicas_for_concurrent_host_writes() -> None:
    first, sid = crdt_hub()
    second, second_sid = crdt_hub()

    first_ops = first.mutate_shared_crdt(sid, insert("a"))
    second_ops = second.mutate_shared_crdt(second_sid, insert("b"))
    first.apply_crdt_shared(sid, second_ops, "second")
    second.apply_crdt_shared(second_sid, first_ops, "first")

    assert first_ops[0]["dot"]["replica"] != second_ops[0]["dot"]["replica"]
    assert first._shared[sid].value == second._shared[second_sid].value


def test_hub_binds_client_replica_to_its_tenant() -> None:
    hub, sid = crdt_hub()
    connections = [("alice", 1), ("bob", 1)]
    clients = [Client(), Client()]
    for connection, client in zip(connections, clients):
        hub.subscribe(connection[0], sid, WRITE)
        for message in hub.open(connection):
            client.recv(message)
    alice = protocol.decode(clients[0].edit_crdt(sid, insert("a")))
    hub.recv(connections[0], json.dumps(alice))

    rejected = hub.recv(connections[1], protocol.crdt_msg(sid, alice["ops"]))

    assert "belongs to another writer" in json.loads(rejected[connections[1]][0])["error"]


def test_hub_rejects_malformed_crdt_operations_without_raising() -> None:
    hub, sid = crdt_hub()
    hub.subscribe("alice", sid, WRITE)
    connection = ("alice", 1)
    hub.open(connection)

    rejected = hub.recv(connection, json.dumps({"t": "crdt", "id": sid, "ops": 5}))

    assert rejected == {}


def test_hub_compacts_stable_crdt_metadata_and_persists_state() -> None:
    hub = Hub(key=lambda conn: conn)
    spec = CrdtSpec({"kind": "map", "values": {"kind": "register"}})
    sid = hub.share({"Map": {"old": {"Int": 1}}}, "Map", crdt_spec=spec)
    persisted = []
    hub.on_shared_write(lambda *args: persisted.append(args))
    [op] = hub.mutate_shared_crdt(sid, [{"kind": "map_remove", "path": [], "key": "old"}])

    assert hub.compact_shared_crdt(sid, {op["dot"]["replica"]: op["dot"]["counter"]}) == 1
    assert persisted[-1][4] == {"crdt_compacted": {op["dot"]["replica"]: 1}}


def test_hub_persists_compaction_when_only_causal_metadata_changes() -> None:
    hub = Hub(key=lambda conn: conn)
    sid = hub.share({"Str": "old"}, "Str", crdt_spec=CrdtSpec({"kind": "register"}))
    persisted = []
    hub.on_shared_write(lambda *args: persisted.append(args))
    [op] = hub.mutate_shared_crdt(sid, [{"kind": "register_set", "path": [], "value": {"Str": "new"}}])

    assert hub.compact_shared_crdt(sid, {op["dot"]["replica"]: op["dot"]["counter"]}) == 0
    assert persisted[-1][4] == {"crdt_compacted": {op["dot"]["replica"]: 1}}


def test_legacy_backplane_write_cannot_desynchronize_crdt_state() -> None:
    hub, sid = crdt_hub()
    before = hub.snapshot_shared(sid)

    hub.apply_shared(sid, {"rev": 1, "ops": [{"Set": {"path": [], "value": {"Str": "legacy"}}}]}, "old-worker")

    assert hub.snapshot_shared(sid) == before


def test_rejected_crdt_edit_clears_outbox_and_restores_authoritative_value() -> None:
    hub, sid = crdt_hub()
    hub.subscribe("alice", sid)
    connection = ("alice", 1)
    client = Client()
    for message in hub.open(connection):
        client.recv(message)
    frame = client.edit_crdt(sid, insert("denied"))

    output = hub.recv(connection, frame)
    assert [json.loads(message)["t"] for message in output[connection]] == ["reject", "crdt_snapshot"]
    for message in output[connection]:
        client.recv(message)

    assert client.pending_crdt_ops(sid) == 0
    assert client.value(sid) == {"Str": ""}


class MemoryBus:
    def __init__(self) -> None:
        self.peers: list[MemoryBackplane] = []


class MemoryBackplane(Backplane):
    def __init__(self, bus: MemoryBus) -> None:
        super().__init__()
        self.bus = bus

    async def _start(self) -> None:
        self.bus.peers.append(self)

    async def publish(self, data: bytes) -> None:
        framed = self._frame(data)
        for peer in self.bus.peers:
            if peer is not self:
                peer._deliver(framed)


class RecordingBackplane(Backplane):
    def __init__(self) -> None:
        super().__init__()
        self.published: list[dict] = []

    async def _start(self) -> None:
        pass

    async def publish(self, data: bytes) -> None:
        self.published.append(json.loads(data))


def test_relay_catches_up_and_converges_crdt_state() -> None:
    async def run() -> None:
        bus = MemoryBus()
        first_hub, sid = crdt_hub()
        first = RelayBroadcaster(first_hub, MemoryBackplane(bus))
        await first.start(catch_up_timeout=0.05)
        await first.mutate_shared_crdt(sid, insert("a"))

        second_hub, second_sid = crdt_hub()
        second = RelayBroadcaster(second_hub, MemoryBackplane(bus))
        await second.start(catch_up_timeout=0.5)
        assert second_hub.snapshot_shared(second_sid) == first_hub.snapshot_shared(sid)

        await second.mutate_shared_crdt(second_sid, insert("b"))
        for _ in range(20):
            if first_hub._shared[sid].value == second_hub._shared[second_sid].value:
                break
            await asyncio.sleep(0.01)
        assert first_hub._shared[sid].value == second_hub._shared[second_sid].value
        await second.stop()
        await first.stop()

    asyncio.run(run())


def test_relay_propagates_replica_ownership_between_workers() -> None:
    async def run() -> None:
        bus = MemoryBus()
        first_hub, sid = crdt_hub()
        second_hub, second_sid = crdt_hub()
        first_hub.subscribe("alice", sid, WRITE)
        second_hub.subscribe("bob", second_sid, WRITE)
        first = RelayBroadcaster(first_hub, MemoryBackplane(bus))
        second = RelayBroadcaster(second_hub, MemoryBackplane(bus))
        await first.start(catch_up_timeout=0.05)
        await second.start(catch_up_timeout=0.2)
        client = Client()
        for message in first.open(("alice", 1)):
            client.recv(message)
        frame = client.edit_crdt(sid, insert("a"))
        first.recv(("alice", 1), frame)
        second.open(("bob", 1))
        for _ in range(20):
            if second_hub._crdt_replica_owners:
                break
            await asyncio.sleep(0)

        rejected = second.recv(("bob", 1), frame)

        assert "belongs to another writer" in json.loads(rejected[("bob", 1)][0])["error"]
        await second.stop()
        await first.stop()

    asyncio.run(run())


def test_relay_compacts_crdt_metadata_on_every_worker() -> None:
    async def run() -> None:
        bus = MemoryBus()
        first_hub = Hub(key=lambda conn: conn)
        spec = CrdtSpec({"kind": "map", "values": {"kind": "register"}})
        sid = first_hub.share({"Map": {"old": {"Int": 1}}}, "Map", crdt_spec=spec)
        first = RelayBroadcaster(first_hub, MemoryBackplane(bus))
        await first.start(catch_up_timeout=0.05)
        [op] = first_hub.mutate_shared_crdt(sid, [{"kind": "map_remove", "path": [], "key": "old"}])

        second_hub = Hub(key=lambda conn: conn)
        second_sid = second_hub.share({"Map": {"old": {"Int": 1}}}, "Map", crdt_spec=spec)
        second = RelayBroadcaster(second_hub, MemoryBackplane(bus))
        await second.start(catch_up_timeout=0.2)
        frontier = {op["dot"]["replica"]: op["dot"]["counter"]}

        assert await first.compact_shared_crdt(sid, frontier) == 1
        await asyncio.sleep(0)

        assert first_hub.snapshot_shared(sid)["crdt_state"] == second_hub.snapshot_shared(second_sid)["crdt_state"]
        await second.stop()
        await first.stop()

    asyncio.run(run())


def test_relay_does_not_publish_rejected_crdt_operations() -> None:
    async def run() -> None:
        hub, sid = crdt_hub()
        hub.subscribe("alice", sid, WRITE)
        connection = ("alice", 1)
        client = Client()
        for message in hub.open(connection):
            client.recv(message)
        valid = protocol.decode(client.edit_crdt(sid, insert("x")))
        valid["ops"][0]["kind"] = "unknown"
        backplane = RecordingBackplane()
        relay = RelayBroadcaster(hub, backplane)

        output = relay.recv(connection, json.dumps(valid))
        await asyncio.sleep(0)

        assert json.loads(output[connection][0])["t"] == "reject"
        assert backplane.published == []

    asyncio.run(run())


def test_relay_publishes_authorized_crdt_operations() -> None:
    async def run() -> None:
        hub, sid = crdt_hub()
        hub.subscribe("alice", sid, WRITE)
        connection = ("alice", 1)
        client = Client()
        for message in hub.open(connection):
            client.recv(message)
        backplane = RecordingBackplane()
        relay = RelayBroadcaster(hub, backplane)

        relay.recv(connection, client.edit_crdt(sid, insert("x")))
        await asyncio.sleep(0)

        assert backplane.published[0]["t"] == "c"
        assert backplane.published[0]["sid"] == sid

    asyncio.run(run())


def test_relay_does_not_republish_duplicate_crdt_operations() -> None:
    async def run() -> None:
        hub, sid = crdt_hub()
        hub.subscribe("alice", sid, WRITE)
        connection = ("alice", 1)
        client = Client()
        for message in hub.open(connection):
            client.recv(message)
        backplane = RecordingBackplane()
        relay = RelayBroadcaster(hub, backplane)
        frame = client.edit_crdt(sid, insert("x"))

        relay.recv(connection, frame)
        await asyncio.sleep(0)
        backplane.published.clear()
        relay.recv(connection, frame)
        await asyncio.sleep(0)

        assert backplane.published == []

    asyncio.run(run())


def test_relay_buffers_legacy_and_crdt_writes_during_catchup() -> None:
    async def run() -> None:
        hub, sid = crdt_hub()
        legacy_sid = hub.share({"Map": {"x": {"Int": 0}}}, "Legacy")
        bus = MemoryBus()
        backplane = MemoryBackplane(bus)
        relay = RelayBroadcaster(hub, backplane)
        await backplane.start()
        consumer = asyncio.create_task(relay._consume())
        remote = CrdtDocument(text_spec(), "", "remote")
        ops = remote.mutate(insert("remote"))["ops"]
        patch = {
            "rev": 1,
            "ops": [{"Set": {"path": [{"Key": "x"}], "value": {"Int": 1}}}],
        }

        async def wait_for(predicate) -> None:
            while not predicate():
                await asyncio.sleep(0)

        catchup = asyncio.create_task(relay._catch_up(0.2))
        await asyncio.wait_for(wait_for(lambda: relay._catching_up), 0.1)
        backplane._deliver(b"p" * 16 + json.dumps({"t": "x", "sid": sid + 99, "frontier": {}, "origin": "peer"}).encode())
        backplane._deliver(b"p" * 16 + json.dumps({"t": "c", "sid": sid, "ops": ops, "origin": "peer"}).encode())
        backplane._deliver(b"p" * 16 + json.dumps({"t": "w", "sid": legacy_sid, "patch": patch, "origin": "peer"}).encode())
        await asyncio.wait_for(wait_for(lambda: len(relay._buffer) == 3), 0.1)
        await catchup

        assert hub._shared[sid].value == {"Str": "remote"}
        assert hub._shared[legacy_sid].value["Map"]["x"] == {"Int": 1}
        consumer.cancel()
        await backplane.stop()

    asyncio.run(run())


def test_relay_consumer_survives_invalid_crdt_operation(caplog) -> None:
    async def run() -> None:
        hub, sid = crdt_hub()
        backplane = MemoryBackplane(MemoryBus())
        relay = RelayBroadcaster(hub, backplane)
        await backplane.start()
        consumer = asyncio.create_task(relay._consume())
        while not backplane._subscribers:
            await asyncio.sleep(0)

        poison = {"kind": "unknown", "dot": {"counter": 1, "replica": "poison"}}
        backplane._deliver(b"p" * 16 + json.dumps({"t": "c", "sid": sid, "ops": [poison], "origin": "peer"}).encode())
        await asyncio.sleep(0)
        remote = CrdtDocument(text_spec(), "", "remote")
        ops = remote.mutate(insert("valid"))["ops"]
        backplane._deliver(b"p" * 16 + json.dumps({"t": "c", "sid": sid, "ops": ops, "origin": "peer"}).encode())
        for _ in range(20):
            if hub._shared[sid].value == {"Str": "valid"}:
                break
            await asyncio.sleep(0)

        assert consumer.done() is False
        assert hub._shared[sid].value == {"Str": "valid"}
        assert "dropping invalid backplane message" in caplog.text
        consumer.cancel()
        await backplane.stop()

    asyncio.run(run())


def test_catchup_reapplies_local_crdt_writes_after_lower_revision_peer_snapshot() -> None:
    async def run() -> None:
        hub, sid = crdt_hub()
        hub.subscribe("client", sid, WRITE)
        connection = ("client", 1)
        client = Client()
        for message in hub.open(connection):
            client.recv(message)
        relay = RelayBroadcaster(hub, RecordingBackplane())
        relay._catching_up = True
        for index, value in enumerate("local"):
            relay.recv(connection, client.edit_crdt(sid, splice(index, value)))
        assert hub._shared[sid].rev == 5
        assert len(relay._buffer) == 5

        peer = CrdtDocument(text_spec(), "", "peer")
        for index, value in enumerate("old"):
            peer.mutate(splice(index, value))
        relay._apply_resp(
            {
                "sid": sid,
                "kind": "snap",
                "value": {"Str": peer.value},
                "rev": 3,
                "merge_state": {},
                "crdt_spec": text_spec().to_dict(),
                "crdt_state": peer.state,
            }
        )
        assert sid in relay._caught
        assert "old" in hub._shared[sid].value["Str"]
        relay._catching_up = False
        for kind, buffered_sid, payload, origin in relay._buffer:
            assert kind == "c"
            hub.apply_crdt_shared(buffered_sid, payload, origin)

        assert sorted(hub._shared[sid].value["Str"]) == sorted("localold")
        await asyncio.sleep(0)

    asyncio.run(run())


def test_catchup_buffers_duplicate_crdt_replays_before_snapshot_replacement() -> None:
    hub, sid = crdt_hub()
    hub.subscribe("client", sid, WRITE)
    connection = ("client", 1)
    client = Client()
    for message in hub.open(connection):
        client.recv(message)
    frame = client.edit_crdt(sid, insert("kept"))
    hub.recv(connection, frame)
    relay = RelayBroadcaster(hub, RecordingBackplane())
    relay._catching_up = True

    relay.recv(connection, frame)
    empty = CrdtDocument(text_spec(), "", "peer")
    relay._apply_resp(
        {
            "sid": sid,
            "kind": "snap",
            "value": {"Str": ""},
            "rev": 0,
            "merge_state": {},
            "crdt_spec": text_spec().to_dict(),
            "crdt_state": empty.state,
        }
    )
    for kind, buffered_sid, payload, origin in relay._buffer:
        assert kind == "c"
        hub.apply_crdt_shared(buffered_sid, payload, origin)

    assert hub._shared[sid].value == {"Str": "kept"}


def test_catchup_resnapshots_connected_clients_after_peer_state_replacement() -> None:
    async def run() -> None:
        hub, sid = crdt_hub()
        hub.subscribe("client", sid, WRITE)
        connection = ("client", 1)
        client = Client()
        for message in hub.open(connection):
            client.recv(message)
        relay = RelayBroadcaster(hub, RecordingBackplane())
        relay._catching_up = True
        output = relay.recv(connection, client.edit_crdt(sid, insert("L")))
        for message in output[connection]:
            client.recv(message)

        peer = CrdtDocument(text_spec(), "", "peer")
        peer.mutate(insert("P"))
        relay._apply_resp(
            {
                "sid": sid,
                "kind": "snap",
                "value": {"Str": "P"},
                "rev": 1,
                "merge_state": {},
                "crdt_spec": text_spec().to_dict(),
                "crdt_state": peer.state,
            }
        )
        relay._catching_up = False
        for kind, buffered_sid, payload, origin in relay._buffer:
            assert kind == "c"
            hub.apply_crdt_shared(buffered_sid, payload, origin)
        flushed = hub.flush()[connection]
        assert [protocol.decode(message)["t"] for message in flushed] == ["crdt_snapshot", "crdt"]
        for message in flushed:
            client.recv(message)

        assert client.value(sid) == hub._shared[sid].value
        await asyncio.sleep(0)

    asyncio.run(run())


def test_autosync_preserves_crdt_snapshot_before_operation_fanout() -> None:
    class Connection:
        def __init__(self) -> None:
            self.sent: list[str | bytes] = []

        async def send_text(self, message: str) -> None:
            self.sent.append(message)

        async def send_bytes(self, message: bytes) -> None:
            self.sent.append(message)

    async def run() -> None:
        hub = Hub(key=lambda conn: "client")
        sid = hub.share({"Str": ""}, "Document", crdt_spec=text_spec())
        hub.subscribe("client", sid, WRITE)
        connection = Connection()
        hub.open(connection)
        peer = CrdtDocument(text_spec(), "", "peer")
        peer.mutate(insert("P"))
        hub.apply_snapshot_shared(sid, {"Str": "P"}, 1, {}, text_spec().to_dict(), peer.state)
        local = CrdtDocument(text_spec(), "", "local")
        hub.apply_crdt_shared(sid, local.mutate(insert("L"))["ops"], "client")
        task = asyncio.create_task(autosync(hub, interval=0.001))
        try:
            for _ in range(100):
                if len(connection.sent) == 2:
                    break
                await asyncio.sleep(0.001)
            assert [protocol.decode(message)["t"] for message in connection.sent] == ["crdt_snapshot", "crdt"]
            assert connection in hub._codecs
        finally:
            task.cancel()

    asyncio.run(run())


def test_crdt_catchup_refuses_legacy_snapshots_and_deltas() -> None:
    hub, sid = crdt_hub()
    hub.mutate_shared_crdt(sid, insert("new"))
    before = hub.snapshot_shared(sid)
    relay = RelayBroadcaster(hub, RecordingBackplane())

    relay._apply_resp({"sid": sid, "kind": "snap", "value": {"Str": "legacy"}, "rev": 9, "merge_state": {}})
    relay._apply_resp({"sid": sid, "kind": "delta", "patches": [], "rev": 9, "merge_state": {}})

    assert sid not in relay._caught
    assert hub.snapshot_shared(sid) == before


def test_crdt_snapshot_adoption_uses_distinct_hub_replicas() -> None:
    source = CrdtDocument(text_spec(), "", "source")
    source.mutate(insert("base"))
    hubs = [Hub(key=lambda conn: conn), Hub(key=lambda conn: conn)]
    sids = [hub.share({"Str": ""}, "Document") for hub in hubs]
    for hub, sid in zip(hubs, sids):
        assert hub.apply_snapshot_shared(sid, {"Str": "base"}, 1, {}, text_spec().to_dict(), source.state)

    ops = [hub.mutate_shared_crdt(sid, insert(value)) for hub, sid, value in zip(hubs, sids, "ab")]

    assert ops[0][0]["dot"]["replica"] != ops[1][0]["dot"]["replica"]


def test_legacy_snapshot_still_adopts_an_older_revision_value() -> None:
    hub = Hub(key=lambda conn: conn)
    sid = hub.share({"Str": "new"}, "Legacy", rev=2)
    hub.subscribe("client", sid)
    hub.open("client")

    hub.apply_snapshot_shared(sid, {"Str": "peer"}, 1, {})

    assert hub._shared[sid].value == {"Str": "peer"}
    assert hub._shared[sid].rev == 2
    [snapshot] = hub.flush()["client"]
    assert protocol.decode(snapshot)["value"] == {"Str": "peer"}


@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
def test_crdt_protocol_frames_round_trip_every_codec(codec: str) -> None:
    spec = text_spec()
    hub, sid = crdt_hub()
    snapshot = hub.snapshot_shared(sid)
    message = protocol.crdt_snapshot_msg(
        sid,
        "Document",
        snapshot["rev"],
        snapshot["value"],
        spec.to_dict(),
        snapshot["crdt_state"],
    )
    assert protocol.decode(protocol.encode(message, codec), codec)["t"] == "crdt_snapshot"
