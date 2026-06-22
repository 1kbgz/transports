"""Serve a `Session`/`Hub` over Server-Sent Events (receive-only).

SSE is a one-way server→client stream (the browser `EventSource`), well suited to receive-mostly UIs
like dashboards. A client receives the opening snapshots and then a live stream of patches; it does
not send edits back over SSE (use the WebSocket adapter for bidirectional sync).

The adapter reuses the existing `autosync` driver: each SSE connection is a queue-backed handle
whose `send_text` enqueues a message, and the streaming response drains that queue. SSE is a text
channel, so the JSON codec is used.

```python
from starlette.routing import Route
import transports

app = Starlette(
    routes=[Route("/sse", transports.sse_endpoint(server))],
    on_startup=[lambda: asyncio.create_task(transports.autosync(server))],
)
```
"""

import asyncio
from typing import Any

from . import protocol
from .server import Broadcaster


class _SSEConn:
    """A connection handle that buffers outbound messages for an SSE stream to drain."""

    def __init__(self) -> None:
        self.queue: asyncio.Queue = asyncio.Queue()

    async def send_text(self, msg: str) -> None:
        await self.queue.put(msg)

    async def send_bytes(self, data: bytes) -> None:  # SSE is text; decode for the rare binary frame
        await self.queue.put(data.decode())


async def sse_stream(server: Broadcaster, conn: _SSEConn):
    """Async generator of wire messages for one SSE connection: the opening snapshots, then patches.

    Patches arrive on the connection's queue via `autosync` (which calls `send_text`). Closes the
    connection when the consumer stops iterating.
    """
    for wire in server.open(conn, protocol.JSON):  # snapshots first
        await conn.queue.put(wire)
    try:
        while True:
            yield await conn.queue.get()
    finally:
        server.close(conn)


def sse_endpoint(server: Broadcaster):
    """Build a Starlette endpoint that streams a `Server`/`Hub` to a client over SSE.

    Wire it into an app as a normal route (``Route("/sse", sse_endpoint(server))``) and run
    `autosync(server)` as a background task to stream server-side model changes to connected clients.
    """
    from sse_starlette import EventSourceResponse

    async def endpoint(request: Any):
        return EventSourceResponse(sse_stream(server, _SSEConn()))

    return endpoint
