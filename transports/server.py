"""Serve a reactive `Session` over connections (WebSocket, ...).

`Server` holds the transport-agnostic logic — register connections, send snapshots on open, relay
inbound patches, broadcast outbound patches — as plain synchronous methods that *return* the messages
to send, keyed by connection. The actual async I/O lives in a thin adapter (`starlette_endpoint` /
`autoflush`), so the protocol is testable without a network.

A connection handle is any hashable object (the Starlette `WebSocket`, a test sentinel, ...) that the
I/O adapter knows how to send on. Each connection negotiates a codec (`"json"` or `"msgpack"`); the
server encodes every outbound message in *that connection's* codec, so JSON and MessagePack clients
can share one server. A wire message is a `str` (JSON text frame) or `bytes` (MessagePack binary).
"""

import asyncio
from typing import Any, Dict, List, Protocol, Union

from . import protocol
from .session import Session

Wire = Union[str, bytes]


class Broadcaster(Protocol):
    """The structural contract `autoflush` drives — satisfied by both `Server` and `Hub`."""

    def flush(self) -> Dict[Any, List[Wire]]: ...

    def close(self, conn: Any) -> None: ...


class Server:
    """Serves a `Session` to connected clients: sends a snapshot on connect, broadcasts patches, and
    relays a client's patches to the other clients (a hub). Transport-agnostic — its methods return
    the messages to send; an adapter such as `starlette_endpoint` performs the I/O.

    Each connection has its own negotiated codec, so outbound messages are encoded per connection."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._codecs: Dict[Any, str] = {}

    def _encode_for(self, conn: Any, msg_json: str) -> Wire:
        return protocol.encode(msg_json, self._codecs.get(conn, protocol.JSON))

    def open(self, conn: Any, codec: str = protocol.JSON) -> List[Wire]:
        """Register a connection with its codec; returns the snapshot messages to send it."""
        self._codecs[conn] = protocol.normalize_codec(codec)
        out: List[Wire] = []
        for mid in self._session.ids():
            snap = self._session.snapshot(mid)
            msg = protocol.snapshot_msg(mid, snap["type_name"], snap["rev"], snap["value"])
            out.append(self._encode_for(conn, msg))
        return out

    def recv(self, conn: Any, data: Wire) -> Dict[Any, List[Wire]]:
        """Handle an inbound message (text or binary frame); returns messages to send, keyed by conn.

        A client patch is applied to the hosted model's value and relayed to the *other* connections,
        so the server acts as a hub. Each relay is encoded in the target connection's codec.
        """
        msg = protocol.decode(data)
        if msg.get("t") == "patch":
            self._session.apply_patch(msg["id"], msg["patch"])
            relay = protocol.patch_msg(msg["id"], msg["patch"])
            return {c: [self._encode_for(c, relay)] for c in self._codecs if c is not conn}
        return {}

    def flush(self) -> Dict[Any, List[Wire]]:
        """Drain the session and return the patch messages to broadcast, encoded per connection."""
        msgs = [protocol.patch_msg(mid, patch) for mid, patch in self._session.drain()]
        if not msgs or not self._codecs:
            return {}
        return {c: [self._encode_for(c, m) for m in msgs] for c in self._codecs}

    def close(self, conn: Any) -> None:
        self._codecs.pop(conn, None)


# --- async I/O adapters --------------------------------------------------------------------------


async def _send(conn: Any, msg: Wire) -> None:
    if isinstance(msg, (bytes, bytearray)):
        await conn.send_bytes(msg)
    else:
        await conn.send_text(msg)


def starlette_endpoint(server: Server):
    """Build a Starlette WebSocket endpoint that serves `server`.

    The connection's codec is read from a ``?codec=`` query param (default JSON). Wire it into an
    app, e.g. ``WebSocketRoute("/ws", starlette_endpoint(server))``, and run `autoflush(server)` as a
    background task to stream server-side model changes to clients.
    """

    async def endpoint(websocket: Any) -> None:
        from starlette.websockets import WebSocketDisconnect

        codec = websocket.query_params.get("codec", protocol.JSON)
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


async def autoflush(server: Broadcaster, interval: float = 0.01) -> None:
    """Background task: periodically flush and broadcast patches to all connections.

    Run exactly one of these per `Server`/`Hub` (not per connection), so a single drain feeds every
    client.
    """
    while True:
        await asyncio.sleep(interval)
        for conn, msgs in server.flush().items():
            for msg in msgs:
                try:
                    await _send(conn, msg)
                except Exception:
                    server.close(conn)
