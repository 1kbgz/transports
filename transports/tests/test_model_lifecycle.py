import json

import pytest
from pydantic import BaseModel, Field

from transports import Client, Hub, Server, Session, protocol


class Counter(BaseModel):
    count: int = 0


def deliver(client, frames):
    for frame in frames:
        client.recv(frame)


@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
@pytest.mark.parametrize("tenant", [False, True])
def test_late_host_retirement_and_reconnect(codec, tenant):
    session = Session()
    if tenant:
        server = Hub(key=lambda conn: conn.split(":")[0])
        session = server.tenant("alice")
    else:
        server = Server(session)
    alice, bob = "alice:1", "alice:2"
    first, second = Client(codec), Client(codec)
    deliver(first, server.open(alice, codec))
    if tenant:
        other = Client(codec)
        deliver(other, server.open("other:1", codec))
    model = Counter()
    mid = session.host(model)
    model.count = 5
    deliver(first, server.flush()[alice])
    assert first.model(mid, Counter).count == 5
    deliver(second, server.open(bob, codec))
    stale = second.edit_ops(mid, [{"Set": {"path": [{"Key": "count"}], "value": {"Int": 9}}}], "old-edit")
    changes, abandoned = [], []
    first.on_change(changes.append)
    first.on_abandon(abandoned.append)
    first.edit_ops(mid, [], "pending")
    server.close(bob)
    model.count = 6
    session.flush()
    assert session.unhost(mid)
    assert not session.unhost(mid)
    model.count = 7  # old watcher must neither recreate state nor mark a missing id dirty
    assert session.drain() == []
    deliver(first, server.flush()[alice])
    assert changes == [{"t": "remove", "id": mid}]
    assert abandoned == [["pending"]]
    assert first.ids() == []
    assert first.type_name(mid) is None
    assert first._rev == {}
    assert mid not in session._log
    if tenant:
        assert other.ids() == []
    deliver(second, server.open(bob, codec, since=second._rev))
    assert second.ids() == []
    rejected = []
    second.on_reject(rejected.append)
    deliver(second, server.recv(bob, stale)[bob])
    assert rejected[-1]["proposal"] == "old-edit"
    assert "unknown" in rejected[-1]["error"]
    assert session.ids() == []
    new_mid = session.host(Counter(count=11))
    assert new_mid > mid
    frames = server.flush()
    deliver(first, frames[alice])
    deliver(second, frames[bob])
    assert first.model(new_mid, Counter).count == second.model(new_mid, Counter).count == 11
    assert server.flush() == {}


@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
def test_inventory_clears_owned_commands_and_awareness(codec):
    class Owned(BaseModel):
        status: int = Field(default=0, json_schema_extra={"owner": "client"})

    session = Session()
    server = Server(session)
    client = Client(codec)
    deliver(client, server.open("reader", codec, ownership=False))
    deliver(client, server.open("owner", codec))
    mid = session.host(Owned())
    session.command(mid, "status", 2)
    frames = server.flush()
    reader = Client(codec)
    deliver(reader, frames["reader"])
    deliver(client, frames["owner"])
    assert not reader.owns(mid, "status")
    assert client.owns(mid, "status")
    assert client._commands
    client.edit_ops(mid, [{"Set": {"path": [{"Key": "status"}], "value": {"Int": 3}}}])
    client.recv(protocol.encode(protocol.awareness_msg(mid, {"name": "Bob"}, "bob", scope="cursor"), codec))
    client._local_awareness[mid, "cursor"] = "queued"
    removed = []
    client.on_awareness(removed.append, scope="cursor")
    session.unhost(mid)
    deliver(client, server.flush()["owner"])
    assert not client.owns(mid, "status")
    assert not client._commands
    assert not client._local_awareness
    assert not client.awareness(mid, scope="cursor")
    assert removed == [{"t": "scoped_awareness", "id": mid, "scope": "cursor", "peer": "bob", "state": None}]
    assert json.loads(client._owned.drain()) == []
    assert not session._authorities


def test_inventory_releases_crdt_outbox_and_returns_batch_removals():
    from transports import CrdtSpec

    hub = Hub(key=lambda conn: conn)
    mid = hub.share({"Str": ""}, "Text", crdt_spec=CrdtSpec({"kind": "sequence", "materialization": "string"}))
    hub.subscribe("alice", mid, "write")
    client = Client()
    deliver(client, hub.open("alice"))
    # Simulate reconnecting to a host that no longer serves this model.
    frame = client.edit_crdt(mid, [{"kind": "sequence_insert", "path": [], "after": None, "values": ["a"]}])
    assert frame and client.pending_crdt_ops(mid)
    changes = client.recv(json.dumps({"t": "batch", "msgs": [{"t": "models", "ids": []}]}))
    assert changes == [{"t": "remove", "id": mid}]
    assert client.pending_crdt_ops() == 0
    assert client.crdt_spec(mid) is None
    assert client.ids() == []


@pytest.mark.parametrize("tenant", [False, True])
def test_explicit_inventory_reconciles_without_resume_token(tenant):
    session = Session()
    server = Server(session)
    if tenant:
        server = Hub(key=lambda conn: conn)
        session = server.tenant("alice")
    client = Client()
    mid = session.host(Counter())
    deliver(client, server.open("alice", inventory=True))
    assert client.revisions() == {mid: 0}
    server.close("alice")
    session.unhost(mid)
    deliver(client, server.open("alice", inventory=True))
    assert client.ids() == []
    assert client.revisions() == {}


def test_sse_reconnect_inventory_removes_retired_models():
    import asyncio

    from transports.sse import _SSEConn, sse_stream

    async def run():
        session = Session()
        server = Server(session)
        mid = session.host(Counter())
        client = Client()
        stream = sse_stream(server, _SSEConn(), inventory=True)
        client.recv(await anext(stream))  # inventory, then snapshot
        client.recv(await anext(stream))
        await stream.aclose()
        session.unhost(mid)
        stream = sse_stream(server, _SSEConn(), inventory=True)
        client.recv(await anext(stream))
        assert client.ids() == []
        await stream.aclose()

    asyncio.run(run())


def test_retired_model_releases_python_reference_and_schema():
    import gc
    import weakref

    session = Session()
    model = Counter()
    ref = weakref.ref(model)
    mid = session.host(model)
    session.unhost(mid)
    del model
    gc.collect()
    assert ref() is None
    assert session.schema("Counter") is None


def test_tenant_retirement_preserves_shared_model_state():
    hub = Hub(key=lambda conn: conn)
    session = hub.tenant("alice")
    private = session.host(Counter())
    shared = hub.share(Counter(count=9))
    hub.subscribe("alice", shared)
    client = Client()
    deliver(client, hub.open("alice", inventory=True))
    session.unhost(private)
    deliver(client, hub.flush()["alice"])
    assert client.ids() == [shared]
    assert client.model(shared, Counter).count == 9


@pytest.mark.parametrize("tenant", [False, True])
def test_bad_lifecycle_codec_does_not_block_other_connections(tenant):
    def encode(message):
        if message.get("t") == "models":
            raise ValueError("inventory encoding failed")
        return json.dumps(message)

    protocol.register_codec("test/lifecycle", encode, json.loads)
    try:
        session = Session()
        server = Server(session)
        if tenant:
            server = Hub(key=lambda conn: "tenant")
            session = server.tenant("tenant")
        server.open("bad", "test/lifecycle")
        client = Client()
        deliver(client, server.open("good"))
        mid = session.host(Counter(count=2))
        frames = server.flush()
        deliver(client, frames["good"])
        assert "bad" not in server._codecs
        assert client.model(mid, Counter).count == 2
    finally:
        protocol.unregister_codec("test/lifecycle")
