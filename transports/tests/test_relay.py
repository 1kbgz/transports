"""Cross-worker convergence: a client on one worker and a client on another edit the same shared model
concurrently, the writes propagate over a backplane, and both workers' replicas converge — distinct
fields both survive, a conflicting field resolves to the same winner everywhere. This is the CRDT path
(`DeepLwwCrdt`) running across processes through `RelayBroadcaster`'s publish/apply."""

import asyncio
import json
import multiprocessing as mp
import os
import random
import sys
import tempfile

import pytest

posix_only = pytest.mark.skipif(sys.platform == "win32", reason="Unix domain sockets are POSIX-only")


def _patch(rev, *kv):
    return {"rev": rev, "ops": [{"Set": {"path": [{"Key": k}], "value": {"Int": v}}} for k, v in kv]}


def _worker(path, idx, patch, result_q):
    """One worker: host the shared model, apply its own write + publish it, apply the other's, report the
    converged value. Re-publishes a few times so the result doesn't depend on connect timing (re-applying
    a write is idempotent under the CRDT — same stamp)."""

    async def go():
        from transports import DeepLwwCrdt, Hub, UnixSocketBackplane

        hub = Hub(key=lambda c: c)
        sid = hub.share({"Map": {}}, "Doc", merge=DeepLwwCrdt)  # first share on each -> same sid
        hub.subscribe(f"t{idx}", sid, "write")
        bp = UnixSocketBackplane(path)
        await bp.start()
        origin = f"t{idx}"

        async def consume():
            async for raw in bp.messages():
                m = json.loads(raw)
                hub.apply_shared(m["sid"], m["patch"], m["origin"])

        ct = asyncio.create_task(consume())
        hub.apply_shared(sid, patch, origin)  # this worker's own client edit
        payload = json.dumps({"sid": sid, "patch": patch, "origin": origin}).encode()
        await asyncio.sleep(0.3)  # let both backplanes connect
        for _ in range(4):
            await bp.publish(payload)
            await asyncio.sleep(0.3)
        ct.cancel()
        await bp.close()
        result_q.put((idx, hub._shared[sid].value))

    asyncio.run(go())


@posix_only
def test_concurrent_cross_worker_edits_converge():
    path = os.path.join(tempfile.gettempdir(), f"tbp-relay-{os.getpid()}-{random.randint(0, 9999)}.sock")
    patches = {
        0: _patch(1, ("a", 0), ("x", 10)),  # worker 0: unique key "a" + conflicting "x"
        1: _patch(1, ("b", 1), ("x", 11)),  # worker 1: unique key "b" + conflicting "x"
    }
    result_q = mp.Queue()
    procs = [mp.Process(target=_worker, args=(path, i, patches[i], result_q)) for i in (0, 1)]
    for p in procs:
        p.start()
    results = {}
    for _ in procs:
        idx, value = result_q.get(timeout=20)
        results[idx] = value
    for p in procs:
        p.join(timeout=5)
    try:
        os.unlink(path)
    except OSError:
        pass

    assert results[0] == results[1], "replicas did not converge"
    m = results[0]["Map"]
    assert m["a"] == {"Int": 0} and m["b"] == {"Int": 1}, "distinct-field edits should both survive"
    assert m["x"] == {"Int": 11}, "the conflicting field should resolve to the (1,'t1') winner on both"


# --- catch-up + durability (relay logic over a tiny in-process bus) ---

from transports import DeepLwwCrdt, Hub, RelayBroadcaster  # noqa: E402
from transports.backplane import Backplane  # noqa: E402


class _Bus:
    def __init__(self):
        self.peers = []


class MemBackplane(Backplane):
    """An in-process backplane for testing relay logic without sockets — peers share a `_Bus`."""

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


def test_late_joiner_catches_up_and_inherits_the_clock():
    async def go():
        bus = _Bus()
        hub_a = Hub(key=lambda c: c)
        sid = hub_a.share({"Map": {}}, "Doc", merge=DeepLwwCrdt)
        relay_a = RelayBroadcaster(hub_a, MemBackplane(bus))
        await relay_a.start(catch_up_timeout=0.2)  # first worker, no peer
        hub_a.apply_shared(sid, _patch(1, ("a", 0), ("x", 10)), "ca")  # A's client edits

        hub_b = Hub(key=lambda c: c)  # B joins late, empty
        hub_b.share({"Map": {}}, "Doc", merge=DeepLwwCrdt)
        relay_b = RelayBroadcaster(hub_b, MemBackplane(bus))
        await relay_b.start(catch_up_timeout=1.0)  # request-for-state -> A answers -> adopt

        assert hub_b._shared[sid].value == hub_a._shared[sid].value, "joiner did not catch up"
        # the merge clock came across too: a stale write (lower stamp) to a caught-up field is dropped
        hub_b.apply_shared(sid, _patch(1, ("x", 999)), "aa")  # (1,'aa') < (1,'ca')
        assert hub_b._shared[sid].value["Map"]["x"] == {"Int": 10}, "stale write should be dropped"

    asyncio.run(go())


def test_durable_restore_recovers_value_rev_and_clock():
    store = {}
    hub = Hub(key=lambda c: c)
    sid = hub.share({"Map": {}}, "Doc", merge=DeepLwwCrdt)
    hub.on_shared_write(lambda s, tn, value, rev, patch, ms: store.update(value=value, rev=rev, ms=ms))
    hub.apply_shared(sid, _patch(1, ("a", 1), ("x", 5)), "ca")
    hub.apply_shared(sid, _patch(2, ("x", 7)), "cb")  # x -> 7, stamp (2,'cb')

    # "full-cluster restart": a fresh hub restores from the user's store
    hub2 = Hub(key=lambda c: c)
    sid2 = hub2.share(store["value"], "Doc", merge=DeepLwwCrdt, rev=store["rev"], merge_state=store["ms"])
    assert hub2._shared[sid2].value == hub._shared[sid].value
    assert hub2._shared[sid2].rev == hub._shared[sid].rev
    hub2.apply_shared(sid2, _patch(1, ("x", 999)), "aa")  # (1,'aa') < (2,'cb') -> dropped (clock restored)
    assert hub2._shared[sid2].value["Map"]["x"] == {"Int": 7}


def test_since_shared_returns_delta_or_none():
    hub = Hub(key=lambda c: c)
    sid = hub.share({"Map": {}}, "Doc", merge=DeepLwwCrdt, replay=True)
    for r in range(1, 4):
        hub.apply_shared(sid, _patch(r, (f"k{r}", r)), f"c{r}")
    assert hub.since_shared(sid, 3) == []  # already current
    assert len(hub.since_shared(sid, 1)) == 2  # patches for rev 2 and 3
    no_replay = hub.share({"Map": {}}, "Doc2", merge=DeepLwwCrdt)  # replay=False
    assert hub.since_shared(no_replay, 0) is None


def test_late_joiner_with_checkpoint_catches_up_by_delta():
    async def go():
        bus = _Bus()
        hub_a = Hub(key=lambda c: c)
        sid = hub_a.share({"Map": {}}, "Doc", merge=DeepLwwCrdt, replay=True)
        relay_a = RelayBroadcaster(hub_a, MemBackplane(bus))
        await relay_a.start(catch_up_timeout=0.2)
        hub_a.apply_shared(sid, _patch(1, ("a", 1)), "ca")
        hub_a.apply_shared(sid, _patch(2, ("b", 2)), "cb")

        # B restored a stale checkpoint at rev 1 (value has only "a") -> should be sent the rev-2 delta
        hub_b = Hub(key=lambda c: c)
        hub_b.share({"Map": {"a": {"Int": 1}}}, "Doc", merge=DeepLwwCrdt, replay=True, rev=1)
        relay_b = RelayBroadcaster(hub_b, MemBackplane(bus))
        await relay_b.start(catch_up_timeout=1.0)

        assert hub_b._shared[sid].value == hub_a._shared[sid].value, "delta catch-up did not converge"

    asyncio.run(go())


def _alive_worker(path, result_q):
    async def go():
        from transports import DeepLwwCrdt, Hub, RelayBroadcaster, UnixSocketBackplane

        hub = Hub(key=lambda c: c)
        sid = hub.share({"Map": {}}, "Doc", merge=DeepLwwCrdt)
        relay = RelayBroadcaster(hub, UnixSocketBackplane(path))
        await relay.start(catch_up_timeout=0.3)
        hub.apply_shared(sid, _patch(1, ("a", 0), ("x", 10)), "ca")
        await asyncio.sleep(3.0)  # stay up to answer the joiner's request
        result_q.put(("a", hub._shared[sid].value))
        await relay.stop()

    asyncio.run(go())


def _joiner_worker(path, result_q):
    async def go():
        await asyncio.sleep(1.2)  # join after the first worker has edited
        from transports import DeepLwwCrdt, Hub, RelayBroadcaster, UnixSocketBackplane

        hub = Hub(key=lambda c: c)
        hub.share({"Map": {}}, "Doc", merge=DeepLwwCrdt)
        relay = RelayBroadcaster(hub, UnixSocketBackplane(path))
        await relay.start(catch_up_timeout=2.0)  # request-for-state over the real backplane
        result_q.put(("b", hub._shared[next(iter(hub._shared))].value))
        await relay.stop()

    asyncio.run(go())


@posix_only
def test_cross_process_late_joiner_catches_up_over_a_real_backplane():
    path = os.path.join(tempfile.gettempdir(), f"tbp-join-{os.getpid()}-{random.randint(0, 9999)}.sock")
    result_q = mp.Queue()
    procs = [
        mp.Process(target=_alive_worker, args=(path, result_q)),
        mp.Process(target=_joiner_worker, args=(path, result_q)),
    ]
    for p in procs:
        p.start()
    res = {}
    for _ in procs:
        k, v = result_q.get(timeout=20)
        res[k] = v
    for p in procs:
        p.join(timeout=5)
    try:
        os.unlink(path)
    except OSError:
        pass
    assert res["b"] == res["a"], "a worker joining late did not catch up over the backplane"
    assert res["b"]["Map"]["x"] == {"Int": 10}
