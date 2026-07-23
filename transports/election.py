"""Leader election + a singleton task across a cluster, over a `Backplane`.

`uvicorn --workers N` (or any multi-process cluster) sometimes needs a job to run on **exactly one**
worker — a ticker, a scheduler, a single writer — and to **fail over** if that worker dies. `Election`
provides that over the same backplane the cluster already shares: each member beacons its id, and the
member with the smallest id seen alive (within `timeout`) is the leader, so when the leader stops
beaconing the next-smallest takes over within `timeout`. (This decouples "who runs the job" from "who
runs the backplane broker" — unlike a backplane's own proxy election, it survives the broker moving, and
pairs with a dedicated broker, `transports.backplane.serve_zmq_broker`, to remove the bus's single point
of failure.)

It is a lease over best-effort pub/sub, not consensus: during a failover two members may both consider
themselves leader for up to `timeout`, so make the singleton task idempotent or tolerant of brief overlap.
For a hard single-writer guarantee, back it with an external coordinator (etcd / ZooKeeper / Raft).
"""

import asyncio
import json
import os
import time
from collections.abc import Awaitable, Callable

from .backplane import Backplane


class Election:
    """Leader election over a `Backplane`. Read :attr:`is_leader`, or run a coroutine on exactly the leader
    with :meth:`run_singleton`. Share the cluster's (multi-subscriber) backplane — election beacons are
    tagged so they don't collide with other traffic. The backplane must already be ``start()``ed (the relay
    or your app does that)."""

    def __init__(self, backplane: Backplane, *, id: str | None = None, interval: float = 1.0, timeout: float = 3.0) -> None:
        self.backplane = backplane
        self.id = id or os.urandom(8).hex()
        self.interval = interval
        self.timeout = timeout
        self._seen: dict = {}  # peer id -> monotonic last-seen
        self._tasks: list = []
        self._singleton: asyncio.Task | None = None
        self._stopped = False

    @property
    def is_leader(self) -> bool:
        """True when this member is running and no peer with a smaller id has beaconed within `timeout` —
        i.e. it is the smallest live member. With no peers heard, a lone member leads itself; a stopped
        member never leads."""
        if self._stopped:
            return False
        now = time.monotonic()
        return all(self.id <= pid for pid, seen in self._seen.items() if now - seen < self.timeout)

    async def start(self) -> None:
        """Begin beaconing + tracking peers (does not start a singleton task)."""
        if not self._tasks:
            self._tasks = [asyncio.create_task(self._beacon()), asyncio.create_task(self._listen())]

    async def stop(self) -> None:
        self._stopped = True
        for t in self._tasks:
            t.cancel()
        if self._singleton:
            self._singleton.cancel()
            self._singleton = None

    async def run_singleton(self, factory: Callable[[], Awaitable]) -> None:
        """Run ``factory()`` (a coroutine function) on whichever member is the leader: (re)start it on
        gaining leadership, cancel it on losing it. Runs until :meth:`stop`. Waits one beacon round first
        so ids are exchanged before the first decision (reducing startup overlap)."""
        await self.start()
        await asyncio.sleep(self.interval)
        try:
            while not self._stopped:
                if self.is_leader and self._singleton is None:
                    self._singleton = asyncio.create_task(factory())
                elif not self.is_leader and self._singleton is not None:
                    self._singleton.cancel()
                    self._singleton = None
                await asyncio.sleep(self.interval / 2)
        finally:
            if self._singleton:
                self._singleton.cancel()
                self._singleton = None

    async def _beacon(self) -> None:
        try:
            while not self._stopped:
                await self.backplane.publish(json.dumps({"election": self.id}).encode())
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            pass

    async def _listen(self) -> None:
        try:
            async for raw in self.backplane.messages():
                try:
                    m = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                if isinstance(m, dict) and "election" in m:
                    self._seen[m["election"]] = time.monotonic()
        except asyncio.CancelledError:
            pass
