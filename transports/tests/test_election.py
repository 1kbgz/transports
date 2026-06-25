"""Leader election + singleton task over a backplane, and the backplane's multi-subscriber fan-out (so a
relay and an election can share one bus). Uses a tiny in-process backplane to drive the logic
deterministically."""

import asyncio

from transports import Election
from transports.backplane import Backplane


class _Bus:
    def __init__(self):
        self.peers = []


class MemBackplane(Backplane):
    """In-process backplane: peers share a `_Bus` and deliver to each other."""

    def __init__(self, bus):
        super().__init__()
        self.bus = bus

    async def _start(self):
        self.bus.peers.append(self)

    async def publish(self, data):
        framed = self._frame(data)
        for p in self.bus.peers:
            if p is not self:
                p._deliver(framed)


def test_backplane_fans_to_multiple_subscribers():
    async def go():
        bus = _Bus()
        a, b = MemBackplane(bus), MemBackplane(bus)
        await a.start()
        await b.start()
        got1, got2 = [], []

        async def consume(bp, out):
            async for m in bp.messages():
                out.append(m)

        t1 = asyncio.create_task(consume(a, got1))
        t2 = asyncio.create_task(consume(a, got2))  # second consumer on the SAME backplane
        await asyncio.sleep(0.05)
        await b.publish(b"hello")
        await asyncio.sleep(0.1)
        assert got1 == [b"hello"] and got2 == [b"hello"], (got1, got2)
        t1.cancel()
        t2.cancel()
        await a.stop()
        await b.stop()

    asyncio.run(go())


def test_smallest_id_leads_and_fails_over():
    async def go():
        bus = _Bus()
        es = []
        for i in ("a", "b", "c"):  # "a" < "b" < "c"
            bp = MemBackplane(bus)
            await bp.start()
            e = Election(bp, id=i, interval=0.1, timeout=0.5)
            await e.start()
            es.append(e)
        await asyncio.sleep(0.4)  # exchange beacons
        assert [e.id for e in es if e.is_leader] == ["a"]

        await es[0].stop()  # the leader dies
        await asyncio.sleep(0.9)  # > timeout: its lease expires
        assert [e.id for e in es if e.is_leader] == ["b"], "next-smallest should take over"

        for e in es[1:]:
            await e.stop()

    asyncio.run(go())


def test_run_singleton_runs_only_on_the_leader():
    async def go():
        bus = _Bus()
        ran = {"a": 0, "b": 0}

        def task_for(label):
            async def task():
                try:
                    while True:
                        ran[label] += 1
                        await asyncio.sleep(0.05)
                except asyncio.CancelledError:
                    pass

            return task

        es, runners = [], []
        for i in ("a", "b"):
            bp = MemBackplane(bus)
            await bp.start()
            e = Election(bp, id=i, interval=0.1, timeout=0.5)
            es.append(e)
            runners.append(asyncio.create_task(e.run_singleton(task_for(i))))
        await asyncio.sleep(0.4)  # let leadership settle
        ran["a"] = ran["b"] = 0  # measure the steady state (a brief startup overlap is documented + expected)
        await asyncio.sleep(0.5)
        assert ran["a"] > 0 and ran["b"] == 0, ran  # only the leader runs the singleton

        for e in es:
            await e.stop()
        for r in runners:
            r.cancel()

    asyncio.run(go())
