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
import json
from typing import Any, Protocol

from . import protocol
from .session import Session

Wire = str | bytes


class Broadcaster(Protocol):
    """The structural contract the I/O adapters drive — satisfied by both `Server` and `Hub`."""

    #: the codec a connection gets when it doesn't request one (the I/O adapters read this)
    default_codec: str

    def open(self, conn: Any, codec: str = ..., since: dict[int, int] | None = ...) -> list[Wire]: ...

    def recv(self, conn: Any, data: Wire) -> dict[Any, list[Wire]]: ...

    def flush(self) -> dict[Any, list[Wire]]: ...

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
        msgs = [protocol.patch_msg(mid, patch) for mid, patch in self._session.drain()]
        if not msgs or not self._codecs:
            return {}
        # a batch-negotiated connection gets the whole flush as one frame (one send instead of
        # one per message); a single-message flush skips the envelope either way
        batched = [protocol.batch_msg(msgs)] if len(msgs) > 1 else msgs
        encoded: dict[tuple[str, bool], list[Wire]] = {}
        out: dict[Any, list[Wire]] = {}
        for conn, codec in self._codecs.items():
            wants_batch = conn in self._batched
            key = (codec, wants_batch)
            if key not in encoded:
                encoded[key] = [protocol.encode(m, codec) for m in (batched if wants_batch else msgs)]
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


async def autosync(server: Broadcaster, interval: float = 0.01, *, max_queue: int = 1024) -> None:
    """Background task: periodically flush and broadcast patches to all connections.

    Run exactly one of these per `Server`/`Hub` (not per connection), so a single drain feeds every
    client. The async counterpart of `sync` — use this for socket backends (WebSocket/SSE) driven by an
    event loop, and `sync` for the synchronous ones (Jupyter comm/anywidget).

    Each connection gets a bounded send queue drained by its own writer task, so the flush loop
    never blocks on a socket and one backpressured client cannot stall the broadcast. A client
    whose queue exceeds ``max_queue`` pending messages is disconnected (the explicit
    slow-consumer policy) — its reconnect resumes from its last revision via ``open(since=...)``.
    """
    queues: dict[Any, asyncio.Queue] = {}
    writers: dict[Any, asyncio.Task] = {}

    def drop(conn: Any) -> None:
        server.close(conn)
        queues.pop(conn, None)
        writer = writers.pop(conn, None)
        if writer is not None and writer is not asyncio.current_task():
            writer.cancel()

    async def write(conn: Any, queue: asyncio.Queue) -> None:
        try:
            while True:
                await _send(conn, await queue.get())
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            drop(conn)

    while True:
        await asyncio.sleep(interval)
        for conn, msgs in server.flush().items():
            queue = queues.get(conn)
            if queue is None:
                # per-connection writer: the flush loop never awaits a socket, so one
                # backpressured client cannot stall the broadcast to everyone else
                queue = queues[conn] = asyncio.Queue()
                writers[conn] = asyncio.get_running_loop().create_task(write(conn, queue))
            if queue.qsize() + len(msgs) > max_queue:
                # the explicit slow-consumer policy: a client that cannot keep up is
                # disconnected rather than buffered without bound; on reconnect the
                # resume protocol (`open(since=...)`) replays what it missed
                drop(conn)
                continue
            for msg in msgs:
                queue.put_nowait(msg)
        for conn in [c for c in queues if c not in server._codecs]:
            drop(conn)


def sync(server: Broadcaster) -> None:
    """Drain host-side changes and deliver the patches over every connection, synchronously.

    The manual counterpart of `autosync`, for backends driven by a synchronous loop (a Jupyter comm or
    anywidget): call it after mutating hosted models — e.g. at the end of a cell, or from a kernel
    timer. Each connection handle exposes `send(wire)`.
    """
    for conn, msgs in server.flush().items():
        for msg in msgs:
            conn.send(msg)
