"""Serve a reactive `Session` over connections (WebSocket, ...).

`Server` holds the transport-agnostic logic — register connections, send snapshots on open, relay
inbound patches, broadcast outbound patches — as plain synchronous methods that *return* the messages
to send, keyed by connection. The actual async I/O lives in a thin adapter (`starlette_endpoint` /
`autoflush`), so the protocol is testable without a network.

A connection handle is any hashable object (the Starlette `WebSocket`, a test sentinel, ...) that the
I/O adapter knows how to send on.
"""

import asyncio
from typing import Any, Dict, List

from . import protocol
from .session import Session


class Server:
    """Serves a `Session` to connected clients: sends a snapshot on connect, broadcasts patches, and
    relays a client's patches to the other clients (a hub). Transport-agnostic — its methods return
    the messages to send; an adapter such as `starlette_endpoint` performs the I/O."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._conns: set = set()

    def open(self, conn: Any) -> List[str]:
        """Register a connection; returns the snapshot messages to send it (current model state)."""
        self._conns.add(conn)
        out: List[str] = []
        for mid in self._session.ids():
            snap = self._session.snapshot(mid)
            out.append(protocol.snapshot_msg(mid, snap["type_name"], snap["rev"], snap["value"]))
        return out

    def recv(self, conn: Any, text: str) -> Dict[Any, List[str]]:
        """Handle an inbound message; returns messages to send, keyed by connection.

        A client patch is applied to the hosted model's value and relayed to the *other* connections,
        so the server acts as a hub.
        """
        msg = protocol.parse(text)
        if msg.get("t") == "patch":
            self._session.apply_patch(msg["id"], msg["patch"])
            relay = protocol.patch_msg(msg["id"], msg["patch"])
            return {c: [relay] for c in self._conns if c is not conn}
        return {}

    def flush(self) -> Dict[Any, List[str]]:
        """Drain the session and return the patch messages to broadcast to every connection."""
        msgs = [protocol.patch_msg(mid, patch) for mid, patch in self._session.drain()]
        if not msgs or not self._conns:
            return {}
        return {c: list(msgs) for c in self._conns}

    def close(self, conn: Any) -> None:
        self._conns.discard(conn)


# --- async I/O adapters --------------------------------------------------------------------------


def starlette_endpoint(server: Server):
    """Build a Starlette WebSocket endpoint that serves `server`.

    Wire it into an app, e.g. ``WebSocketRoute("/ws", starlette_endpoint(server))``, and run
    `autoflush(server)` as a background task to stream server-side model changes to clients.
    """

    async def endpoint(websocket: Any) -> None:
        from starlette.websockets import WebSocketDisconnect

        await websocket.accept()
        for msg in server.open(websocket):
            await websocket.send_text(msg)
        try:
            while True:
                text = await websocket.receive_text()
                for conn, msgs in server.recv(websocket, text).items():
                    for msg in msgs:
                        await conn.send_text(msg)
        except WebSocketDisconnect:
            pass
        finally:
            server.close(websocket)

    return endpoint


async def autoflush(server: Server, interval: float = 0.01) -> None:
    """Background task: periodically flush the session and broadcast patches to all connections.

    Run exactly one of these per `Server` (not per connection), so a single drain feeds every client.
    """
    while True:
        await asyncio.sleep(interval)
        for conn, msgs in server.flush().items():
            for msg in msgs:
                try:
                    await conn.send_text(msg)
                except Exception:
                    server.close(conn)
