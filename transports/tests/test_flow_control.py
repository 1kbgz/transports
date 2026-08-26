"""`autosync` flow control: bounded per-connection queues and the explicit slow-consumer policy.

The flush loop never awaits a socket — each connection's messages drain through its own bounded
queue and writer task — so one backpressured client cannot stall the broadcast, and a client whose
queue overflows is disconnected (its reconnect resumes from its last revision via ``open(since=)``).
"""

from __future__ import annotations

import asyncio

from pydantic import BaseModel

import transports


class Counter(BaseModel):
    n: int = 0


class FakeConn:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_text(self, msg: str) -> None:
        self.sent.append(msg)

    async def send_bytes(self, msg: bytes) -> None:
        self.sent.append(msg)


class StuckConn(FakeConn):
    """A consumer that never drains: its first send blocks forever."""

    async def send_text(self, msg: str) -> None:
        await asyncio.Event().wait()


def test_slow_consumer_is_disconnected_without_stalling_the_broadcast():
    async def scenario() -> None:
        session = transports.Session()
        model = Counter()
        session.host(model)
        server = transports.Server(session)

        fast, stuck = FakeConn(), StuckConn()
        server.open(fast)
        server.open(stuck)

        sync_task = asyncio.get_running_loop().create_task(transports.autosync(server, interval=0.001, max_queue=4))
        try:
            # each mutation is one queued message per connection per flush; the stuck consumer's
            # writer is wedged in its first send, so its queue can only grow
            for expected in range(1, 21):
                model.n = expected
                await asyncio.sleep(0.005)

            # the fast consumer kept receiving throughout — the stuck one never blocked it
            assert len(fast.sent) >= 10

            # the slow consumer crossed max_queue and was disconnected (the explicit policy);
            # the broadcast set no longer includes it
            assert stuck not in server._codecs
            assert fast in server._codecs

            # and the flow keeps working after the disconnect
            before = len(fast.sent)
            model.n = 999
            await asyncio.sleep(0.01)
            assert len(fast.sent) > before
        finally:
            sync_task.cancel()

    asyncio.run(scenario())


def test_flush_encodes_once_per_codec():
    session = transports.Session()
    model = Counter()
    session.host(model)
    server = transports.Server(session)

    conns = [FakeConn() for _ in range(3)]
    for conn in conns:
        server.open(conn)

    model.n = 7
    out = server.flush()
    assert set(out) == set(conns)
    # same-codec connections share the identical encoded objects, not per-connection copies
    first = out[conns[0]]
    assert all(out[conn] is first for conn in conns[1:])


def test_batch_negotiated_connections_get_one_frame_per_flush():
    import json

    session = transports.Session()
    model = Counter()
    session.host(model)
    server = transports.Server(session)

    plain, batched = FakeConn(), FakeConn()
    server.open(plain)
    server.open(batched, batch=True)

    # several patches in one flush window: the negotiated connection gets one batch frame,
    # the plain connection the individual messages
    model.n = 1
    model.n = 2
    out = server.flush()
    # a single coalesced patch skips the envelope entirely
    assert len(out[plain]) == len(out[batched]) == 1
    assert json.loads(out[batched][0])["t"] == "patch"

    # two models -> two messages per flush -> exactly one enveloped frame for the batched conn
    second = Counter()
    session.host(second)
    model.n = 3
    second.n = 1
    out = server.flush()
    assert len(out[plain]) == 2
    assert len(out[batched]) == 1
    frame = json.loads(out[batched][0])
    assert frame["t"] == "batch"
    assert [m["t"] for m in frame["msgs"]] == ["patch", "patch"]

    # the python client applies a batch in order and returns the accepted changes as a list
    client = transports.Client()
    for msg in server.open(FakeConn()):  # snapshots to seed a mirror
        client.recv(msg)
    model.n = 4
    second.n = 2
    accepted = client.recv(server.flush()[batched][0])
    assert isinstance(accepted, list) and len(accepted) == 2
    assert client.model(next(iter(session.ids())), Counter).n == 4
