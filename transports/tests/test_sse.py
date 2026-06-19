import asyncio

from pydantic import BaseModel

from transports import Client, Server, Session
from transports.sse import _SSEConn, sse_stream


class Device(BaseModel):
    name: str
    on: bool = False


def test_sse_stream_yields_snapshot_then_patch():
    """Drive the SSE stream generator directly (the `EventSourceResponse` wrapper is a thin shim).

    Exercises the real `_SSEConn`, `server.open`, and the autoflush delivery path (`_send` ->
    `send_text` -> queue) without an in-process HTTP server (httpx's ASGITransport can't stream an
    unbounded SSE body).
    """

    async def run():
        session = Session()
        server = Server(session)
        d = Device(name="lamp")
        mid = session.host(d)
        conn = _SSEConn()
        stream = sse_stream(server, conn)
        client = Client()

        snap = await asyncio.wait_for(stream.__anext__(), 1)  # snapshot streams first
        client.recv(snap)
        assert client.model(mid, Device) == d

        d.on = True  # host-side mutation -> the autoflush driver delivers via send_text
        for c, msgs in server.flush().items():
            for m in msgs:
                await c.send_text(m)

        patch = await asyncio.wait_for(stream.__anext__(), 1)
        client.recv(patch)
        assert client.model(mid, Device).on is True

        await stream.aclose()

    asyncio.run(run())
