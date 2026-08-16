import asyncio

from pydantic import BaseModel

from transports import Client, Server, Session
from transports.sse import _SSEConn, sse_stream


class Device(BaseModel):
    name: str
    on: bool = False


def test_sse_stream_yields_snapshot_then_patch():
    """Drive the SSE stream generator directly (the `EventSourceResponse` wrapper is a thin shim).

    Exercises the real `_SSEConn`, `server.open`, and the autosync delivery path (`_send` ->
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

        d.on = True  # host-side mutation -> the autosync driver delivers via send_text
        for c, msgs in server.flush().items():
            for m in msgs:
                await c.send_text(m)

        patch = await asyncio.wait_for(stream.__anext__(), 1)
        client.recv(patch)
        assert client.model(mid, Device).on is True

        await stream.aclose()

    asyncio.run(run())


def test_browser_sse_path_mirrors_events():
    """`Client._connect_sse_browser` (the Pyodide path) rides the browser EventSource via the js FFI.
    Fake `js` / `pyodide.ffi` in sys.modules to drive the wiring off-browser: message events update
    the mirror, an error resolves the task, and the source is closed on the way out."""
    import sys
    import types
    import typing

    class FakeEvent:
        def __init__(self, data):
            self.data = data

    class FakeProxy:
        def __init__(self, fn):
            self._fn = fn

        def __call__(self, *args):
            return self._fn(*args)

        def destroy(self) -> None:
            pass

    class FakeSource:
        instances: typing.ClassVar[list] = []

        def __init__(self, url: str):
            self.url = url
            self.listeners = {}
            self.closed = False
            FakeSource.instances.append(self)

        def addEventListener(self, name: str, cb) -> None:
            self.listeners[name] = cb

        def close(self) -> None:
            self.closed = True

    js_mod = types.ModuleType("js")
    js_mod.EventSource = types.SimpleNamespace(new=FakeSource)
    pyodide_mod = types.ModuleType("pyodide")
    ffi_mod = types.ModuleType("pyodide.ffi")
    ffi_mod.create_proxy = FakeProxy
    pyodide_mod.ffi = ffi_mod
    saved = {name: sys.modules.get(name) for name in ("js", "pyodide", "pyodide.ffi")}
    sys.modules.update({"js": js_mod, "pyodide": pyodide_mod, "pyodide.ffi": ffi_mod})
    try:

        async def run():
            session = Session()
            d = Device(name="lamp")
            mid = session.host(d)
            server = Server(session)

            client = Client()
            task = asyncio.ensure_future(client._connect_sse_browser("http://host/sse"))
            await asyncio.sleep(0)  # let the task build the source + register listeners
            source = FakeSource.instances[-1]

            for wire in server.open("conn"):  # SSE delivers text frames
                source.listeners["message"](FakeEvent(wire))
            d.on = True
            for msgs in server.flush().values():
                for wire in msgs:
                    source.listeners["message"](FakeEvent(wire))
            source.listeners["error"](FakeEvent(None))
            await asyncio.wait_for(task, 1)
            assert client.model(mid, Device).on is True
            assert source.closed

        asyncio.run(run())
    finally:
        for name, mod in saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod
