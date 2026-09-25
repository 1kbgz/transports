"""Cluster a :class:`~transports.Hub` across worker processes with a :class:`~transports.Backplane`.

`uvicorn --workers N` spreads the connection I/O across cores, but each worker then has its own in-memory
hub. :class:`RelayBroadcaster` bridges a local hub to a backplane so the cluster serves the *same* shared
models. It satisfies the same broadcaster contract as `Hub`/`Server` (``open``/``recv``/``flush``/
``close`` + ``default_codec``), so ``ws_endpoint(relay)`` and ``autosync(relay)`` work unchanged.

- A **local** write to a shared model is applied + fanned to this worker's clients **and** published to
  the backplane.
- A write from **another** worker is merged in through the model's `MergeStrategy` and fanned to this
  worker's clients. Because the merge is a CRDT, concurrent writes from clients on different workers
  **converge**.

**Joining late.** When a worker starts it issues a *request-for-state* on the backplane and a peer
answers — either a full **snapshot** (value + rev + the merge clock) or, when the model is shared with
``replay=True`` and the joiner already holds a recent checkpoint, a **delta** of patches since that rev
(``resync`` vs ``replay``, chosen by what the joiner reports it ``have``s). Live writes that arrive while
catching up are buffered and applied afterward (idempotent under the CRDT). :meth:`start` blocks until
catch-up settles, so the worker only serves clients once consistent.

**Durability.** transports stores nothing durably. Register :meth:`Hub.on_shared_write` to persist each
authoritative change to *your* store, and restore with ``Hub.share(value=…, rev=…, merge_state=…)`` on
startup. Then: a 1+ worker failure is covered by the surviving replicas + a rejoining worker's catch-up;
a full-cluster failure is covered by your checkpoint. Gate persistence on :attr:`is_leader` for a single
writer.
"""

import asyncio
import json
import logging
import os
from typing import Any

from . import protocol
from .backplane import Backplane
from .hub import Hub
from .server import Wire
from .transports import diff as _diff

logger = logging.getLogger(__name__)


class RelayBroadcaster:
    """Make a `Hub` part of a multi-worker cluster over a `Backplane`. Use it wherever a `Hub`/`Server`
    goes: ``ws_endpoint(relay)`` for connections, ``autosync(relay)`` to drain. Call :meth:`start` once
    (it starts the backplane, the consumer, and a catch-up) and :meth:`stop` on shutdown."""

    def __init__(self, hub: Hub, backplane: Backplane) -> None:
        self.hub = hub
        self.backplane = backplane
        self.default_codec = hub.default_codec
        self._id = os.urandom(8).hex()
        self._task: asyncio.Task | None = None
        self._catching_up = False
        self._buffer: list[tuple] = []
        self._caught: set = set()

    @property
    def is_leader(self) -> bool:
        """True on the one worker that runs the backplane broker/proxy (a natural single writer for
        durability). Always False for `QueueBackplane` (no election) — coordinate persistence yourself."""
        return bool(getattr(self.backplane, "runs_proxy", getattr(self.backplane, "runs_broker", False)))

    async def start(self, catch_up_timeout: float = 1.5) -> None:
        await self.backplane.start()
        self._task = asyncio.create_task(self._consume())
        await self._catch_up(catch_up_timeout)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        await self.backplane.stop()

    async def _catch_up(self, timeout: float) -> None:
        """Ask peers for the current state of our shared models; adopt the first answer per model. Buffer
        live writes meanwhile and apply them after, so nothing is lost in the join window."""
        sids = list(self.hub._shared)
        if not sids:
            return
        self._catching_up, self._buffer, self._caught = True, [], set()
        have = {str(sid): self.hub._shared[sid].rev for sid in sids}
        await self.backplane.publish(json.dumps({"t": "req", "frm": self._id, "have": have}).encode())
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        while len(self._caught) < len(sids) and loop.time() - t0 < timeout:
            await asyncio.sleep(0.05)
        self._catching_up = False
        for kind, sid, payload, origin in self._buffer:
            try:
                if kind == "c":
                    self.hub.apply_crdt_shared(sid, payload, origin)
                elif kind == "x":
                    self.hub.compact_shared_crdt(sid, payload)
                else:
                    self.hub.apply_shared(sid, payload, origin)
            except (KeyError, TypeError, ValueError):
                logger.exception("dropping invalid buffered backplane message")
        self._buffer = []

    async def _consume(self) -> None:
        try:
            async for raw in self.backplane.messages():
                try:
                    m = json.loads(raw)
                    t = m.get("t", "w")
                    if t == "w":
                        if self._catching_up:
                            self._buffer.append(("w", m["sid"], m["patch"], m["origin"]))
                        else:
                            self.hub.apply_shared(m["sid"], m["patch"], m["origin"])
                    elif t == "c":
                        if self._catching_up:
                            self._buffer.append(("c", m["sid"], m["ops"], m["origin"]))
                        else:
                            self.hub.apply_crdt_shared(m["sid"], m["ops"], m["origin"])
                    elif t == "x":
                        if self._catching_up:
                            self._buffer.append(("x", m["sid"], m["frontier"], m.get("origin")))
                        else:
                            self.hub.compact_shared_crdt(m["sid"], m["frontier"])
                    elif t == "req":
                        await self._respond(m)
                    elif t == "resp" and m.get("to") == self._id:
                        self._apply_resp(m)
                except Exception:
                    logger.exception("dropping invalid backplane message")
        except asyncio.CancelledError:
            pass

    async def _respond(self, m: dict) -> None:
        """Answer a peer's request-for-state, per shared model: a delta when the model keeps a replay log
        and the peer's `have` rev is still in it, else a full snapshot."""
        if m.get("frm") == self._id:
            return
        have = m.get("have", {})
        for sid in list(self.hub._shared):
            want = have.get(str(sid), 0)
            snap = self.hub.snapshot_shared(sid)
            if "crdt_state" in snap:
                resp = {
                    "t": "resp",
                    "to": m["frm"],
                    "sid": sid,
                    "kind": "snap",
                    "value": snap["value"],
                    "rev": snap["rev"],
                    "merge_state": snap["merge_state"],
                    "crdt_spec": snap["crdt_spec"],
                    "crdt_state": snap["crdt_state"],
                }
                await self.backplane.publish(json.dumps(resp).encode())
                continue
            delta = self.hub.since_shared(sid, want) if want > 0 else None
            if delta is not None:
                resp = {
                    "t": "resp",
                    "to": m["frm"],
                    "sid": sid,
                    "kind": "delta",
                    "patches": delta,
                    "rev": snap["rev"],
                    "merge_state": snap["merge_state"],
                }
            else:
                resp = {
                    "t": "resp",
                    "to": m["frm"],
                    "sid": sid,
                    "kind": "snap",
                    "value": snap["value"],
                    "rev": snap["rev"],
                    "merge_state": snap["merge_state"],
                }
            await self.backplane.publish(json.dumps(resp).encode())

    def _apply_resp(self, m: dict) -> None:
        sid = m["sid"]
        if sid in self._caught:
            return  # already caught up for this model from an earlier responder
        if m["kind"] == "snap":
            applied = self.hub.apply_snapshot_shared(
                sid,
                m["value"],
                m["rev"],
                m.get("merge_state"),
                m.get("crdt_spec"),
                m.get("crdt_state"),
            )
        else:
            applied = self.hub.apply_delta_shared(sid, m["patches"], m["rev"], m.get("merge_state"))
        if applied:
            self._caught.add(sid)

    # --- broadcaster contract: delegate to the hub; recv also publishes shared writes to the cluster ---
    def open(self, conn: Any, codec: str | None = None, since: dict[int, int] | None = None, batch: bool = False) -> list[Wire]:
        return self.hub.open(conn, codec, since, batch=batch)

    def recv(self, conn: Any, data: Wire) -> dict[Any, list[Wire]]:
        codec = self.hub._codecs.get(conn)
        if codec is None:
            return {}
        try:
            msg = protocol.decode(data, codec)
        except (TypeError, ValueError):
            return {}
        origin = self.hub._conn_key.get(conn)
        sid = msg.get("id", 0)
        sh = self.hub._shared.get(sid)
        publish = (
            msg.get("t") in ("patch", "crdt")
            and self.hub._shared_write_allowed(origin, sid)
            and not (msg["t"] == "patch" and sh is not None and sh.crdt is not None)
        )
        out = self.hub.recv(conn, data)  # apply + fan to this worker's clients
        if publish and msg["t"] == "crdt":
            status = self.hub._last_crdt_write
            if self._catching_up and status in ((conn, sid, "applied"), (conn, sid, "duplicate")):
                self._buffer.append(("c", sid, msg["ops"], origin))
            publish = status == (conn, sid, "applied")
        elif publish and self._catching_up:
            self._buffer.append(("w", sid, msg["patch"], origin))
        if publish:
            if msg["t"] == "crdt":
                payload = json.dumps({"t": "c", "sid": sid, "ops": msg["ops"], "origin": origin}).encode()
            else:
                payload = json.dumps({"t": "w", "sid": sid, "patch": msg["patch"], "origin": origin}).encode()
            asyncio.create_task(self.backplane.publish(payload))  # broadcast the raw write to the others
        return out

    async def set_shared(self, sid: int, value: Any) -> None:
        """Write to a shared model from the host side (e.g. a ticker) and propagate it to the cluster:
        apply locally (fanned on the next flush) and publish so every other worker applies it too. The
        cluster-aware counterpart of `Hub.set_shared`."""
        sh = self.hub._shared.get(sid)
        if sh is None:
            return
        if sh.crdt is not None:
            raise ValueError("CRDT-backed models require mutate_shared_crdt")
        patch = json.loads(_diff(json.dumps(sh.value), json.dumps(value)))
        if not patch.get("ops"):
            return
        self.hub.apply_shared(sid, patch, origin="<host>")
        if self._catching_up:
            self._buffer.append(("w", sid, patch, "<host>"))
        await self.backplane.publish(json.dumps({"t": "w", "sid": sid, "patch": patch, "origin": "<host>"}).encode())

    async def mutate_shared_crdt(self, sid: int, mutations: list[dict]) -> None:
        """Apply host-side CRDT mutations locally and publish their operations to every worker."""
        ops = self.hub.mutate_shared_crdt(sid, mutations)
        if ops:
            if self._catching_up:
                self._buffer.append(("c", sid, ops, "<host>"))
            payload = json.dumps({"t": "c", "sid": sid, "ops": ops, "origin": "<host>"}).encode()
            await self.backplane.publish(payload)

    async def compact_shared_crdt(self, sid: int, frontier: dict[str, int]) -> int:
        """Compact a stable CRDT frontier locally and on every worker."""
        compacted = self.hub.compact_shared_crdt(sid, frontier)
        if self._catching_up:
            self._buffer.append(("x", sid, dict(frontier), "<host>"))
        payload = json.dumps({"t": "x", "sid": sid, "frontier": frontier, "origin": "<host>"}).encode()
        await self.backplane.publish(payload)
        return compacted

    def flush(self) -> dict[Any, list[Wire]]:
        return self.hub.flush()

    @property
    def _codecs(self) -> dict[Any, str]:
        return self.hub._codecs

    def _flush_tagged(self) -> dict[Any, list[tuple[int | None, Wire]]]:
        return self.hub._flush_tagged()

    def close(self, conn: Any) -> dict[Any, list[Wire]]:
        return self.hub.close(conn)
