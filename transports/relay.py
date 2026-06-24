"""Cluster a :class:`~transports.Hub` across worker processes with a :class:`~transports.Backplane`.

`uvicorn --workers N` spreads the connection I/O across cores, but each worker then has its own in-memory
hub. :class:`RelayBroadcaster` bridges a local hub to a backplane so the cluster serves the *same* shared
models. It satisfies the same broadcaster contract as `Hub`/`Server` (``open``/``recv``/``flush``/
``close`` + ``default_codec``), so ``ws_endpoint(relay)`` and ``autosync(relay)`` work unchanged.

- A **local** write to a shared model is applied + fanned to this worker's clients (by the hub) **and**
  published to the backplane as ``{sid, patch, origin}``.
- A write from **another** worker is merged in through the model's `MergeStrategy` and fanned to this
  worker's clients on the next flush.

Because the merge is a CRDT (e.g. :class:`~transports.DeepLwwCrdt`), concurrent writes from clients on
different workers **converge**: every worker applies the same set of ``(patch, origin)`` writes, and the
stamp on each write — not its arrival order — decides the outcome, so all replicas reach one value.
"""

import asyncio
import json
from typing import Any, Dict, List, Optional

from . import protocol
from .backplane import Backplane
from .hub import SHARED_ID_BASE, Hub
from .server import Wire
from .transports import diff as _diff


class RelayBroadcaster:
    """Make a `Hub` part of a multi-worker cluster over a `Backplane`. Use it wherever a `Hub`/`Server`
    goes: ``ws_endpoint(relay)`` for connections, ``autosync(relay)`` to drain. Call :meth:`start` once
    (it starts the backplane + the consumer) and :meth:`stop` on shutdown."""

    def __init__(self, hub: Hub, backplane: Backplane) -> None:
        self.hub = hub
        self.backplane = backplane
        self.default_codec = hub.default_codec
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        await self.backplane.start()
        self._task = asyncio.create_task(self._consume())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        await self.backplane.close()

    async def _consume(self) -> None:
        """Apply writes arriving from other workers; their fans go out on the next `flush`/`autosync`."""
        try:
            async for raw in self.backplane.messages():
                m = json.loads(raw)
                self.hub.apply_shared(m["sid"], m["patch"], m["origin"])
        except asyncio.CancelledError:
            pass

    # --- broadcaster contract: delegate to the hub; recv also publishes shared writes to the cluster ---
    def open(self, conn: Any, codec: Optional[str] = None, since: Optional[Dict[int, int]] = None) -> List[Wire]:
        return self.hub.open(conn, codec, since)

    def recv(self, conn: Any, data: Wire) -> Dict[Any, List[Wire]]:
        out = self.hub.recv(conn, data)  # apply + fan to this worker's clients
        msg = protocol.decode(data, self.hub._codecs.get(conn))
        if msg.get("t") == "patch" and msg.get("id", 0) >= SHARED_ID_BASE:
            origin = self.hub._conn_key.get(conn)
            payload = json.dumps({"sid": msg["id"], "patch": msg["patch"], "origin": origin}).encode()
            asyncio.create_task(self.backplane.publish(payload))  # broadcast the raw write to the others
        return out

    async def write_shared(self, sid: int, value: Any) -> None:
        """Write to a shared model from the host side (e.g. a ticker) and propagate it to the cluster:
        apply locally (fanned to this worker's clients on the next flush) and publish the patch so every
        other worker applies it too. `value` is a core `Value` dict (or a model to convert upstream)."""
        sh = self.hub._shared.get(sid)
        if sh is None:
            return
        patch = json.loads(_diff(json.dumps(sh.value), json.dumps(value)))
        if not patch.get("ops"):
            return
        self.hub.apply_shared(sid, patch, origin="<host>")
        await self.backplane.publish(json.dumps({"sid": sid, "patch": patch, "origin": "<host>"}).encode())

    def flush(self) -> Dict[Any, List[Wire]]:
        return self.hub.flush()

    def close(self, conn: Any) -> None:
        self.hub.close(conn)
