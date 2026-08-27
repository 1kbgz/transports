"""`autosync` flow control: per-connection coalescing and the explicit slow-consumer policy.

The flush loop never awaits a socket — each connection's undelivered messages live in a per-model
map drained by its own writer task — so one backpressured client cannot stall the broadcast. State
coalesces (a newer revision replaces the undelivered one), bounding a slow consumer's backlog by
its model count; a connection whose backlog exceeds ``max_queue`` even after coalescing is
disconnected (its reconnect resumes from its last revision via ``open(since=)``).
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


def test_stuck_state_consumer_coalesces_instead_of_disconnecting():
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
            # the stuck consumer's writer is wedged in its first send, but its undelivered state
            # coalesces to the newest revision per model — one model can never exceed the bound
            for expected in range(1, 21):
                model.n = expected
                await asyncio.sleep(0.005)

            # the fast consumer kept receiving throughout — the stuck one never blocked it
            assert len(fast.sent) >= 10

            # bounded by coalescing, the stuck consumer stays connected (state semantics: it
            # will receive the newest revision whenever it drains, not the history)
            assert stuck in server._codecs
            assert fast in server._codecs
        finally:
            sync_task.cancel()

    asyncio.run(scenario())


def test_stuck_consumer_exceeding_the_bound_is_disconnected():
    async def scenario() -> None:
        session = transports.Session()
        models = [Counter() for _ in range(6)]
        for model in models:
            session.host(model)
        server = transports.Server(session)

        fast, stuck = FakeConn(), StuckConn()
        server.open(fast)
        server.open(stuck)

        sync_task = asyncio.get_running_loop().create_task(transports.autosync(server, interval=0.001, max_queue=4))
        try:
            # six models' undelivered revisions cannot coalesce below max_queue=4: the stuck
            # consumer crosses the bound and is disconnected (the explicit policy)
            for expected in range(1, 6):
                for model in models:
                    model.n = expected
                await asyncio.sleep(0.005)

            assert stuck not in server._codecs
            assert fast in server._codecs

            # and the flow keeps working after the disconnect
            before = len(fast.sent)
            models[0].n = 999
            await asyncio.sleep(0.01)
            assert len(fast.sent) > before
        finally:
            sync_task.cancel()

    asyncio.run(scenario())


def test_coalescing_delivers_the_newest_revision():
    import json

    class GatedConn(FakeConn):
        """Delivery pauses until `gate` is set, so revisions pile up undelivered."""

        def __init__(self) -> None:
            super().__init__()
            self.gate = asyncio.Event()

        async def send_text(self, msg: str) -> None:
            await self.gate.wait()
            self.sent.append(msg)

    async def scenario() -> None:
        session = transports.Session()
        model = Counter()
        session.host(model)
        server = transports.Server(session)

        gated = GatedConn()
        server.open(gated)

        sync_task = asyncio.get_running_loop().create_task(transports.autosync(server, interval=0.001))
        try:
            # revisions land while delivery is gated; each flush replaces the undelivered wire
            for value in (1, 2, 3):
                model.n = value
                await asyncio.sleep(0.005)
            gated.gate.set()
            await asyncio.sleep(0.02)

            # the client got the newest state, not the whole history: fewer sends than revisions,
            # and the last delivered patch carries the final value
            assert 1 <= len(gated.sent) < 3
            ops = json.loads(gated.sent[-1])["patch"]["ops"]
            assert {"Set": {"path": [{"Key": "n"}], "value": {"Int": 3}}} in ops
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


def test_wedged_socket_is_cut_by_the_watchdog_and_its_shard_resumes():
    async def scenario() -> None:
        session = transports.Session()
        model = Counter()
        session.host(model)
        server = transports.Server(session)

        fast, stuck = FakeConn(), StuckConn()
        server.open(stuck)
        server.open(fast)

        # one shard: the wedged send stalls the healthy consumer too, until the watchdog
        # (probing progress every stall_timeout) cuts the wedged conn and restarts the shard
        sync_task = asyncio.get_running_loop().create_task(
            transports.autosync(server, interval=0.001, shards=1, stall_timeout=0.05)
        )
        try:
            model.n = 1
            await asyncio.sleep(0.02)
            starved = len(fast.sent)

            await asyncio.sleep(0.25)
            assert stuck not in server._codecs

            model.n = 2
            await asyncio.sleep(0.05)
            assert fast in server._codecs
            assert len(fast.sent) > starved
        finally:
            sync_task.cancel()

    asyncio.run(scenario())
