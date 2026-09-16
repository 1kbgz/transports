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
from collections.abc import Callable
from typing import Any, Protocol

from . import protocol
from .session import Session

Wire = str | bytes
# `ws_endpoint` uses the active autosync queue for direct replies, so one writer orders every frame
# for a connection. A relay and its hub share one queue owner and one destructive flush loop.
_AUTOSYNC_ENQUEUE: dict[int, Callable[[dict[Any, list[Wire]]], None]] = {}


def _enqueue_if_autosync(server: "Broadcaster", messages: dict[Any, list[Wire]]) -> bool:
    enqueue = _AUTOSYNC_ENQUEUE.get(id(getattr(server, "hub", server)))
    if enqueue is None:
        return False
    enqueue(messages)
    return True


class Broadcaster(Protocol):
    """The structural contract the I/O adapters drive — satisfied by both `Server` and `Hub`."""

    #: the codec a connection gets when it doesn't request one (the I/O adapters read this)
    default_codec: str

    @property
    def _codecs(self) -> dict[Any, str]: ...

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
        arrives, not optimistically. Pending host patches are returned before the proposal reply.
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
                try:
                    snap = self._session.snapshot(msg["id"])
                except KeyError:
                    reject = protocol.reject_msg(msg["id"], 0, error)
                    direct = {conn: [self._encode_for(conn, reject)]}
                else:
                    revert = protocol.snapshot_msg(msg["id"], snap["type_name"], snap["rev"], snap["value"])
                    reject = protocol.reject_msg(msg["id"], snap["rev"], error)
                    direct = {conn: [self._encode_for(conn, revert), self._encode_for(conn, reject)]}
            else:
                relay = protocol.patch_msg(msg["id"], authoritative)
                encoded: dict[str, list[Wire]] = {}
                direct = {}
                for c, codec in self._codecs.items():
                    if codec not in encoded:
                        encoded[codec] = [protocol.encode(relay, codec)]
                    direct[c] = encoded[codec]
            out = {target: [wire for _, wire in tagged] for target, tagged in self._flush_tagged().items()}
            for target, wires in direct.items():
                out.setdefault(target, []).extend(wires)
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
                replies = server.recv(websocket, data)
                if _enqueue_if_autosync(server, replies):
                    continue
                for conn, msgs in replies.items():
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

    Each connection's undelivered state patches live in a per-model map, and **state coalesces**:
    a newer revision is composed with that model's undelivered patch into one frame, so no delta is
    lost while a slow consumer's backlog stays bounded by its model count. Non-coalescible messages
    (batch envelopes and direct replies) accumulate under unique keys instead.

    Delivery runs on a fixed pool of ``shards`` writer tasks, each serially draining its share of
    connections — serial-loop economics (per-connection writer tasks were measured as a multiple-x
    CPU regression at 1000 connections: task-scheduling churn, and no drain-rate feedback). The
    flush loop itself never awaits a socket. Direct replies from `ws_endpoint` enter the same queue,
    preserving revision order without making the receive loop wait on every recipient. Two
    slow-consumer policies bound the pathological cases: a connection still holding more than
    ``max_queue`` undelivered messages from previous flushes — even after coalescing — is disconnected,
    and a connection whose send makes no progress for ``stall_timeout`` seconds (a wedged socket, which
    would stall its shard) is cut by a watchdog. A disconnected client's reconnect resumes from its
    last revision via ``open(since=...)``.
    """
    pending: dict[Any, dict[Any, Wire]] = {}  # per conn: model id (or unique key) -> newest undelivered wire
    epochs: dict[Any, int] = {}  # barriers keep later coalesced state behind direct or batch frames
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
        undelivered = pending.pop(conn, None)
        if undelivered is not None:
            undelivered.clear()
        epochs.pop(conn, None)
        shard = shard_of.pop(conn, None)
        if shard is not None:
            shard.conns.discard(conn)

    def enqueue(conn: Any, tagged: list[tuple[int | None, Wire]], *, coalesce: bool = True) -> None:
        undelivered = pending.get(conn)
        if undelivered is None:
            undelivered = pending[conn] = {}
            shard = min(pool, key=lambda candidate: len(candidate.conns))
            shard.conns.add(conn)
            shard_of[conn] = shard
            if shard.task is None:
                shard.task = asyncio.get_running_loop().create_task(write(shard))
        epoch = epochs.get(conn, 0)
        for mid, wire in tagged:
            if coalesce and mid is not None:
                key = (epoch, mid)
                previous = undelivered.get(key)
                if previous is not None:
                    try:
                        codec = server._codecs.get(conn, server.default_codec)
                        older = protocol.decode(previous, codec)
                        newer = protocol.decode(wire, codec)
                        patch = dict(newer["patch"])
                        patch["ops"] = [*older["patch"]["ops"], *patch["ops"]]
                        wire = protocol.encode(protocol.patch_msg(mid, patch), codec)
                    except Exception:  # noqa: BLE001
                        drop(conn)
                        return
            else:
                key = (None, next(nonce))
                epoch += 1
            undelivered[key] = wire
        epochs[conn] = epoch
        shard = shard_of[conn]
        shard.busy = True
        shard.wake.set()

    def enqueue_direct(messages: dict[Any, list[Wire]]) -> None:
        for conn, wires in messages.items():
            if conn in server._codecs:
                enqueue(conn, [(None, wire) for wire in wires], coalesce=False)

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

    queue_key = id(getattr(server, "hub", server))
    if queue_key in _AUTOSYNC_ENQUEUE:
        raise RuntimeError("autosync is already running for this server")
    _AUTOSYNC_ENQUEUE[queue_key] = enqueue_direct
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
            # Enforce the bound on work left from the previous tick, after healthy writers had
            # a chance to drain. A single wide flush or an interleaved direct reply must not drop
            # a consumer before its writer gets scheduled.
            for conn in [c for c, undelivered in pending.items() if len(undelivered) > max_queue]:
                drop(conn)
            for conn, tagged in server._flush_tagged().items():
                enqueue(conn, tagged)
            fanout_started = loop_time()
            for conn in [c for c in pending if c not in server._codecs]:
                drop(conn)
    finally:
        if _AUTOSYNC_ENQUEUE.get(queue_key) is enqueue_direct:
            _AUTOSYNC_ENQUEUE.pop(queue_key)
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
