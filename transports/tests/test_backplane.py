"""Each backplane must relay frames between separate processes: 3 peers beacon their id and every peer
should receive the other two (never its own). Exercises QueueBackplane, UnixSocketBackplane, and
ZmqBackplane across real processes."""

import asyncio
import multiprocessing as mp
import os
import random
import tempfile
from functools import partial

import pytest

from transports.backplane import QueueBackplane, UnixSocketBackplane, ZmqBackplane


def _peer(make, my_id, result_q, secs=1.5):
    """Child process: start a backplane (from a factory, or a passed-in handle), beacon `my_id`, collect
    what others send, report the set seen."""

    async def go():
        bp = make() if callable(make) else make
        await bp.start()
        seen: set[str] = set()

        async def collect():
            async for m in bp.messages():
                seen.add(m.decode())

        loop = asyncio.get_running_loop()
        ct = asyncio.create_task(collect())
        end = loop.time() + secs
        while loop.time() < end:
            await bp.publish(my_id.encode())
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.25)
        ct.cancel()
        await bp.close()
        result_q.put((my_id, sorted(seen)))

    asyncio.run(go())


def _run(makes):
    """Spawn one peer per factory/handle; return {id: set(seen)} after all report."""
    result_q = mp.Queue()
    ids = [str(i) for i in range(len(makes))]
    procs = [mp.Process(target=_peer, args=(makes[i], ids[i], result_q)) for i in range(len(makes))]
    for p in procs:
        p.start()
    results = {}
    for _ in procs:
        mid, seen = result_q.get(timeout=20)
        results[mid] = set(seen)
    for p in procs:
        p.join(timeout=5)
    return results


def _assert_full_mesh(results, ids):
    for i in ids:
        assert results[i] == (set(ids) - {i}), f"peer {i} saw {results[i]}, expected the other peers"


def test_queue_backplane_relays_across_processes():
    broker, handles = QueueBackplane.bus(3)
    try:
        results = _run(handles)
    finally:
        broker.terminate()
    _assert_full_mesh(results, ["0", "1", "2"])


def test_unix_socket_backplane_relays_across_processes():
    path = os.path.join(tempfile.gettempdir(), f"tbp-{os.getpid()}-{random.randint(0, 9999)}.sock")
    try:
        results = _run([partial(UnixSocketBackplane, path)] * 3)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
    _assert_full_mesh(results, ["0", "1", "2"])


def test_zmq_backplane_relays_across_processes():
    pytest.importorskip("zmq")
    base = random.randint(20000, 60000)
    front, back = f"tcp://127.0.0.1:{base}", f"tcp://127.0.0.1:{base + 1}"
    results = _run([partial(ZmqBackplane, front, back)] * 3)
    _assert_full_mesh(results, ["0", "1", "2"])
