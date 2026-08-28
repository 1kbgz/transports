"""Serve a reactive `Session` over connections (WebSocket, ...).

`Server` holds the transport-agnostic logic — register connections, send snapshots on open, relay
inbound patches, broadcast outbound patches — as plain synchronous methods that *return* the messages
to send, keyed by connection. The actual async I/O lives in a thin adapter (`ws_endpoint` /
`autosync`), so the protocol is testable without a network.

A connection handle is any hashable object (the Starlette `WebSocket`, a test sentinel, ...) that the
I/O adapter knows how to send on. Each connection negotiates a codec (`"json"` or `"msgpack"`); the
server encodes every outbound message in *that connection's* codec, so JSON and MessagePack clients
can share one server. A wire message is a `str` (JSON text frame) or `bytes` (MessagePack binary).
"""

import asyncio
import itertools
import json
from typing import Any, Protocol

from . import protocol
from .session import Session

Wire = str | bytes


class Broadcaster(Protocol):
    """The structural contract the I/O adapters drive — satisfied by both `Server` and `Hub`."""

    #: the codec a connection gets when it doesn't request one (the I/O adapters read this)
    default_codec: str

    def open(self, conn: Any, codec: str = ..., since: dict[int, int] | None = ..., batch: bool = ...) -> list[Wire]: ...

    def recv(self, conn: Any, data: Wire) -> dict[Any, list[Wire]]: ...

    def flush(self) -> dict[Any, list[Wire]]: ...

    def _flush_tagged(self) -> dict[Any, list[tuple[int | None, "Wire"]]]: ...

    def close(self, conn: Any) -> None: ...


class Server:
    """Serves a `Session` to connected clients: sends a snapshot on connect, broadcasts patches, and
    relays a client's patches to the other clients (a hub). Transport-agnostic — its methods return
    the messages to send; an adapter such as `ws_endpoint` performs the I/O.

    Each connection has its own negotiated codec, so outbound messages are encoded per connection."""

    def __init__(self, session: Session, *, default_codec: str = protocol.JSON) -> None:
        self._session = session
        self._codecs: dict[Any, str] = {}
        self._batched: set[Any] = set()
        self.default_codec = protocol.normalize_codec(default_codec)

    def _encode_for(self, conn: Any, msg_json: str) -> Wire:
        return protocol.encode(msg_json, self._codecs.get(conn, self.default_codec))

    def open(self, conn: Any, codec: str | None = None, since: dict[int, int] | None = None, batch: bool = False) -> list[Wire]:
        """Register a connection; return the messages that bring it up to date.

        Fresh connect (``since=None``) → a snapshot per model. Resume (``since={mid: last_rev}``) → only
        the patches each model emitted after ``last_rev``, falling back to a snapshot for any model whose
        replay log can't bridge the gap. So a reconnecting client replays the delta, not the whole model.
        """
        self._codecs[conn] = protocol.normalize_codec(codec or self.default_codec)
        if batch:
            self._batched.add(conn)
        out: list[Wire] = []
        for mid in self._session.ids():
            client_rev = since.get(mid) if since else None
            delta = self._session.since(mid, client_rev) if client_rev is not None else None
            if delta is not None:
                for patch in delta:
                    out.append(self._encode_for(conn, protocol.patch_msg(mid, patch)))
            else:
                snap = self._session.snapshot(mid)
                out.append(self._encode_for(conn, protocol.snapshot_msg(mid, snap["type_name"], snap["rev"], snap["value"])))
        return out

    def recv(self, conn: Any, data: Wire) -> dict[Any, list[Wire]]:
        """Handle an inbound message (text or binary frame); returns messages to send, keyed by conn.

        A client patch is a *proposal*: the server applies it, bumps its own authoritative `rev`, and
        echoes the resulting patch to **every** connection (including the origin), each in that
        connection's codec. Models are server-authoritative — a client's mirror updates when this echo
        arrives, not optimistically.
        """
        msg = protocol.decode(data, self._codecs.get(conn))
        if msg.get("t") == "patch":
            authoritative = self._session.submit(msg["id"], msg["patch"])
            if authoritative is None:
                # Rejected (invalid edit, or a malformed patch): re-send the authoritative state to the
                # proposer alone, so its optimistic UI reverts to the last good value, followed by a typed
                # `reject` frame saying why (the model's validation message). The server stays up and other
                # connections are untouched — a round-trip validation failure self-corrects.
                error = self._session.reject_reason or "rejected"
                snap = self._session.snapshot(msg["id"])
                if snap is None:
                    reject = protocol.reject_msg(msg["id"], 0, error)
                    return {conn: [self._encode_for(conn, reject)]}
                revert = protocol.snapshot_msg(msg["id"], snap["type_name"], snap["rev"], snap["value"])
                reject = protocol.reject_msg(msg["id"], snap["rev"], error)
                return {conn: [self._encode_for(conn, revert), self._encode_for(conn, reject)]}
            relay = protocol.patch_msg(msg["id"], authoritative)
            encoded: dict[str, list[Wire]] = {}
            out: dict[Any, list[Wire]] = {}
            for c, codec in self._codecs.items():
                if codec not in encoded:
                    encoded[codec] = [protocol.encode(relay, codec)]
                out[c] = encoded[codec]
            return out
        return {}

    def flush(self) -> dict[Any, list[Wire]]:
        """Drain the session and return the patch messages to broadcast, encoded once per codec.

        Encoding depends only on the codec, so a broadcast to N same-codec connections shares one
        encoded copy instead of re-encoding per connection — the fan-out cost is O(messages x
        distinct codecs), not O(messages x connections)."""
        stripped: dict[int, list[Wire]] = {}  # same-codec connections keep sharing one list
        return {conn: stripped.setdefault(id(tagged), [wire for _, wire in tagged]) for conn, tagged in self._flush_tagged().items()}

    def _flush_tagged(self) -> dict[Any, list[tuple[int | None, Wire]]]:
        """`flush`, with each message tagged by its model id (``None`` for a batch envelope) so
        `autosync` can coalesce a connection's undelivered state to the newest revision per model."""
        tagged = [(mid, protocol.patch_msg(mid, patch)) for mid, patch in self._session.drain()]
        if not tagged or not self._codecs:
            return {}
        # a batch-negotiated connection gets the whole flush as one frame (one send instead of
        # one per message); a single-message flush skips the envelope either way
        batched = protocol.batch_msg([m for _, m in tagged]) if len(tagged) > 1 else None
        encoded: dict[tuple[str, bool], list[tuple[int | None, Wire]]] = {}
        out: dict[Any, list[tuple[int | None, Wire]]] = {}
        for conn, codec in self._codecs.items():
            wants_batch = conn in self._batched
            key = (codec, wants_batch)
            if key not in encoded:
                if wants_batch and batched is not None:
                    encoded[key] = [(None, protocol.encode(batched, codec))]
                else:
                    encoded[key] = [(mid, protocol.encode(m, codec)) for mid, m in tagged]
            out[conn] = encoded[key]
        return out

    def close(self, conn: Any) -> None:
        self._codecs.pop(conn, None)
        self._batched.discard(conn)


async def _send(conn: Any, msg: Wire) -> None:
    if isinstance(msg, (bytes, bytearray)):
        await conn.send_bytes(msg)
    else:
        await conn.send_text(msg)


def ws_endpoint(server: Broadcaster):
    """Build a Starlette WebSocket endpoint that serves `server` (a `Server` or `Hub`).

    The connection's codec is read from a ``?codec=`` query param, falling back to the broadcaster's
    `default_codec`. Wire it into an app, e.g. ``WebSocketRoute("/ws", ws_endpoint(server))``, and run
    `autosync(server)` as a background task to stream server-side model changes to clients.
    """

    async def endpoint(websocket: Any) -> None:
        from starlette.websockets import WebSocketDisconnect

        codec = websocket.query_params.get("codec", server.default_codec)
        since_param = websocket.query_params.get("since")  # resume token: {mid: last_rev} JSON
        since = {int(k): int(v) for k, v in json.loads(since_param).items()} if since_param else None
        batch = websocket.query_params.get("batch") in ("1", "true")
        await websocket.accept()
        for msg in server.open(websocket, codec, since, batch=batch):
            await _send(websocket, msg)
        try:
            while True:
                frame = await websocket.receive()
                if frame.get("type") == "websocket.disconnect":
                    break
                data = frame.get("text")
                if data is None:
                    data = frame.get("bytes")
                if data is None:
                    continue
                for conn, msgs in server.recv(websocket, data).items():
                    for msg in msgs:
                        await _send(conn, msg)
        except WebSocketDisconnect:
            pass
        finally:
            server.close(websocket)

    return endpoint


async def autosync(
    server: Broadcaster,
    interval: float = 0.01,
    *,
    max_queue: int = 1024,
    max_interval: float = 0.25,
    shards: int = 32,
    stall_timeout: float = 0.5,
) -> None:
    """Background task: periodically flush and broadcast patches to all connections.

    Run exactly one of these per `Server`/`Hub` (not per connection), so a single drain feeds every
    client. The async counterpart of `sync` — use this for socket backends (WebSocket/SSE) driven by an
    event loop, and `sync` for the synchronous ones (Jupyter comm/anywidget).

    Each connection's undelivered messages live in a per-model map, and **state coalesces**: a
    newer revision of a model *replaces* that connection's undelivered one (state semantics —
    clients need the newest revision, not the history), so a slow consumer's backlog is bounded
    by its model count and it always receives fresh data. Non-coalescible messages (batch
    envelopes) accumulate under unique keys instead.

    Delivery runs on a fixed pool of ``shards`` writer tasks, each serially draining its share of
    connections — serial-loop economics (per-connection writer tasks were measured as a multiple-x
    CPU regression at 1000 connections: task-scheduling churn, and no drain-rate feedback). The
    flush loop itself never awaits a socket. Two slow-consumer policies bound the pathological
    cases: a connection still holding more than ``max_queue`` undelivered messages from previous
    flushes — even after coalescing — is disconnected, and a connection whose send makes no
    progress for ``stall_timeout`` seconds (a wedged socket, which would stall its shard) is cut
    by a watchdog. A disconnected client's reconnect resumes from its last revision via
    ``open(since=...)``.
    """
    pending: dict[Any, dict[Any, Wire]] = {}  # per conn: model id (or unique key) -> newest undelivered wire
    nonce = itertools.count()  # keys for non-coalescible messages

    class _Shard:
        __slots__ = ("busy", "conns", "count", "current", "idle_at", "task", "wake")

        def __init__(self) -> None:
            self.conns: set[Any] = set()
            self.wake = asyncio.Event()
            self.task: asyncio.Task | None = None
            self.current: Any = None  # the conn a send is in flight to (watchdog progress probe)
            self.count = 0  # sends completed (watchdog progress probe)
            self.busy = False  # woken with work and not yet drained (the self-clocking signal)
            self.idle_at = 0.0  # when this shard last finished draining (cadence measurement)

    pool = [_Shard() for _ in range(max(1, shards))]
    shard_of: dict[Any, _Shard] = {}

    def drop(conn: Any) -> None:
        server.close(conn)
        pending.pop(conn, None)
        shard = shard_of.pop(conn, None)
        if shard is not None:
            shard.conns.discard(conn)

    async def write(shard: _Shard) -> None:
        while True:
            await shard.wake.wait()
            shard.wake.clear()
            progressed = True
            while progressed:
                progressed = False
                for conn in list(shard.conns):
                    undelivered = pending.get(conn)
                    try:
                        while undelivered:
                            key = next(iter(undelivered))
                            # pop before the send: a revision arriving mid-send re-keys and
                            # is delivered on the next pass, preserving per-model ordering
                            wire = undelivered.pop(key)
                            shard.current = conn
                            await _send(conn, wire)
                            shard.count += 1
                            progressed = True
                    except asyncio.CancelledError:
                        raise
                    except Exception:  # noqa: BLE001
                        drop(conn)
                    finally:
                        shard.current = None
            shard.busy = False
            shard.idle_at = asyncio.get_running_loop().time()

    async def watchdog() -> None:
        # a wedged socket suspends its shard mid-send indefinitely; if a shard is still on the
        # same conn with no completed sends after a full stall_timeout, cut that conn and
        # restart the shard so its healthy connections resume
        seen: dict[int, tuple[Any, int]] = {}
        while True:
            await asyncio.sleep(stall_timeout)
            for index, shard in enumerate(pool):
                probe = (shard.current, shard.count)
                if probe[0] is not None and seen.get(index) == probe:
                    stuck = shard.current
                    if shard.task is not None:
                        shard.task.cancel()
                    drop(stuck)
                    shard.current = None
                    shard.task = asyncio.get_running_loop().create_task(write(shard))
                    shard.wake.set()
                seen[index] = (shard.current, shard.count)

    watchdog_task = asyncio.get_running_loop().create_task(watchdog())
    loop_time = asyncio.get_running_loop().time
    sleep_for = interval
    fanout_started: float | None = None
    try:
        while True:
            await asyncio.sleep(sleep_for)
            # wait for the previous fan-out to mostly finish (>=90% of shards idle); the
            # quorum and deadline keep a few wedged sockets (the watchdog's job) from
            # freezing the cadence for everyone else
            deadline = loop_time() + stall_timeout
            while sum(1 for shard in pool if shard.busy) * 10 > len(pool):
                if loop_time() >= deadline:
                    break
                await asyncio.sleep(interval / 4)
            # self-clocking cadence: the next drain waits as long as the last fan-out took to
            # deliver (a ~50% duty cycle, clamped to [interval, max_interval]), so under load
            # revisions coalesce in the session (state keeps only the newest) instead of
            # shipping at full granularity — draining on a fixed clock regardless of send
            # capacity was measured as a multiple-x CPU regression that also delayed the
            # *final* revision. Light fleets drain instantly and keep the fast cadence.
            if fanout_started is not None:
                drained_at = max((shard.idle_at for shard in pool), default=fanout_started)
                sleep_for = min(max(interval, 4 * (drained_at - fanout_started)), max_interval)
            for conn, tagged in server._flush_tagged().items():
                undelivered = pending.get(conn)
                if undelivered is None:
                    undelivered = pending[conn] = {}
                    shard = min(pool, key=lambda s: len(s.conns))
                    shard.conns.add(conn)
                    shard_of[conn] = shard
                    if shard.task is None:
                        shard.task = asyncio.get_running_loop().create_task(write(shard))
                if len(undelivered) > max_queue:
                    # the explicit slow-consumer policy: a client still holding more than
                    # ``max_queue`` undelivered messages from *previous* flushes — even after
                    # coalescing — is disconnected rather than buffered without bound.
                    # (Checked before the merge, so a wide burst never drops a healthy
                    # consumer that drains promptly.)
                    drop(conn)
                    continue
                for mid, wire in tagged:
                    # keyed by model id: an undelivered older revision is replaced in place
                    # (coalescing), keeping a slow consumer's backlog bounded by model count
                    undelivered[mid if mid is not None else (None, next(nonce))] = wire
                shard = shard_of[conn]
                shard.busy = True
                shard.wake.set()
            fanout_started = loop_time()
            for conn in [c for c in pending if c not in server._codecs]:
                drop(conn)
    finally:
        watchdog_task.cancel()
        for shard in pool:
            if shard.task is not None:
                shard.task.cancel()


def sync(server: Broadcaster) -> None:
    """Drain host-side changes and deliver the patches over every connection, synchronously.

    The manual counterpart of `autosync`, for backends driven by a synchronous loop (a Jupyter comm or
    anywidget): call it after mutating hosted models — e.g. at the end of a cell, or from a kernel
    timer. Each connection handle exposes `send(wire)`.
    """
    for conn, msgs in server.flush().items():
        for msg in msgs:
            conn.send(msg)
