"""Cross-worker convergence: a client on one worker and a client on another edit the same shared model
concurrently, the writes propagate over a backplane, and both workers' replicas converge — distinct
fields both survive, a conflicting field resolves to the same winner everywhere. This is the CRDT path
(`DeepLwwCrdt`) running across processes through `RelayBroadcaster`'s publish/apply."""

import asyncio
import json
import multiprocessing as mp
import os
import random
import tempfile


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
