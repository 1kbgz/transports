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
from typing import Any, Dict, List, Optional, Protocol, Union

from . import protocol
from .session import Session

Wire = Union[str, bytes]


class Broadcaster(Protocol):
    """The structural contract the I/O adapters drive — satisfied by both `Server` and `Hub`."""

    #: the codec a connection gets when it doesn't request one (the I/O adapters read this)
    default_codec: str

    def open(self, conn: Any, codec: str = ...) -> List[Wire]: ...

    def recv(self, conn: Any, data: Wire) -> Dict[Any, List[Wire]]: ...

    def flush(self) -> Dict[Any, List[Wire]]: ...

    def close(self, conn: Any) -> None: ...


class Server:
    """Serves a `Session` to connected clients: sends a snapshot on connect, broadcasts patches, and
    relays a client's patches to the other clients (a hub). Transport-agnostic — its methods return
    the messages to send; an adapter such as `ws_endpoint` performs the I/O.

    Each connection has its own negotiated codec, so outbound messages are encoded per connection."""

    def __init__(self, session: Session, *, default_codec: str = protocol.JSON) -> None:
        self._session = session
        self._codecs: Dict[Any, str] = {}
        self.default_codec = protocol.normalize_codec(default_codec)

    def _encode_for(self, conn: Any, msg_json: str) -> Wire:
        return protocol.encode(msg_json, self._codecs.get(conn, self.default_codec))

    def open(self, conn: Any, codec: Optional[str] = None) -> List[Wire]:
        """Register a connection with its codec; returns the snapshot messages to send it."""
        self._codecs[conn] = protocol.normalize_codec(codec or self.default_codec)
        out: List[Wire] = []
        for mid in self._session.ids():
            snap = self._session.snapshot(mid)
            msg = protocol.snapshot_msg(mid, snap["type_name"], snap["rev"], snap["value"])
            out.append(self._encode_for(conn, msg))
        return out

    def recv(self, conn: Any, data: Wire) -> Dict[Any, List[Wire]]:
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
                return {}
            relay = protocol.patch_msg(msg["id"], authoritative)
            return {c: [self._encode_for(c, relay)] for c in self._codecs}
        return {}

    def flush(self) -> Dict[Any, List[Wire]]:
        """Drain the session and return the patch messages to broadcast, encoded per connection."""
        msgs = [protocol.patch_msg(mid, patch) for mid, patch in self._session.drain()]
        if not msgs or not self._codecs:
            return {}
        return {c: [self._encode_for(c, m) for m in msgs] for c in self._codecs}

    def close(self, conn: Any) -> None:
        self._codecs.pop(conn, None)


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
        await websocket.accept()
        for msg in server.open(websocket, codec):
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


async def autosync(server: Broadcaster, interval: float = 0.01) -> None:
    """Background task: periodically flush and broadcast patches to all connections.

    Run exactly one of these per `Server`/`Hub` (not per connection), so a single drain feeds every
    client. The async counterpart of `sync` — use this for socket backends (WebSocket/SSE) driven by an
    event loop, and `sync` for the synchronous ones (Jupyter comm/anywidget).
    """
    while True:
        await asyncio.sleep(interval)
        for conn, msgs in server.flush().items():
            for msg in msgs:
                try:
                    await _send(conn, msg)
                except Exception:
                    server.close(conn)


def sync(server: Broadcaster) -> None:
    """Drain host-side changes and deliver the patches over every connection, synchronously.

    The manual counterpart of `autosync`, for backends driven by a synchronous loop (a Jupyter comm or
    anywidget): call it after mutating hosted models — e.g. at the end of a cell, or from a kernel
    timer. Each connection handle exposes `send(wire)`.
    """
    for conn, msgs in server.flush().items():
        for msg in msgs:
            conn.send(msg)
