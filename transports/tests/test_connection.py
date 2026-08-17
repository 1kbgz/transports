import json

from pydantic import BaseModel

from transports import Client, Server, Session, protocol, to_value, ws_endpoint


class Device(BaseModel):
    name: str
    on: bool = False


def test_snapshot_then_patch_mirrors_server():
    session = Session()
    server = Server(session)
    client = Client()
    d = Device(name="lamp")
    mid = session.host(d)

    for m in server.open("c1"):  # connect: snapshots flow to the client
        client.recv(m)
    assert client.value(mid) == json.loads(json.dumps(to_value(d)))
    assert client.model(mid, Device) == d

    d.on = True  # server-side mutation -> flush -> client mirrors
    for m in server.flush()["c1"]:
        client.recv(m)
    assert client.model(mid, Device).on is True


def test_flush_broadcasts_to_every_connection():
    session = Session()
    server = Server(session)
    a, b = Client(), Client()
    d = Device(name="lamp")
    mid = session.host(d)
    for m in server.open("a"):
        a.recv(m)
    for m in server.open("b"):
        b.recv(m)

    d.name = "desk"
    out = server.flush()
    for m in out["a"]:
        a.recv(m)
    for m in out["b"]:
        b.recv(m)
    assert a.model(mid, Device).name == "desk"
    assert b.model(mid, Device).name == "desk"


def test_client_edit_relays_to_other_clients():
    session = Session()
    server = Server(session)
    a, b = Client(), Client()
    d = Device(name="lamp")
    mid = session.host(d)
    for m in server.open("a"):
        a.recv(m)
    for m in server.open("b"):
        b.recv(m)

    msg = a.edit(mid, to_value(Device(name="lamp", on=True)))  # client a proposes an edit
    out = server.recv("a", msg)
    assert set(out) == {"a", "b"}  # server-authoritative: echoed to everyone, incl. the origin
    for m in out["a"]:
        a.recv(m)
    for m in out["b"]:
        b.recv(m)
    assert a.model(mid, Device).on is True  # origin's mirror updates on the echo, not optimistically
    assert b.model(mid, Device).on is True


def test_flush_without_connections_is_empty():
    session = Session()
    server = Server(session)
    d = Device(name="lamp")
    session.host(d)
    d.on = True
    assert server.flush() == {}


def test_inbound_edit_updates_host_object_without_echo_loop():
    session = Session()
    server = Server(session)
    d = Device(name="lamp")
    mid = session.host(d)
    a = Client()
    for m in server.open("a"):
        a.recv(m)

    server.recv("a", a.edit(mid, to_value(Device(name="lamp", on=True))))
    assert d.on is True  # the server's hosted Python object reflects the edit (no staleness)
    assert session.snapshot(mid)["rev"] == 1  # server owns the rev
    assert session.drain() == []  # the in-place refresh did not re-trigger observation (no echo loop)


def test_two_clients_converge_on_server_rev():
    session = Session()
    server = Server(session)
    d = Device(name="lamp")
    mid = session.host(d)
    a, b = Client(), Client()
    for m in server.open("a"):
        a.recv(m)
    for m in server.open("b"):
        b.recv(m)

    for _conn, msgs in server.recv("a", a.edit(mid, to_value(Device(name="lamp", on=True)))).items():
        for m in msgs:
            (a if _conn == "a" else b).recv(m)
    assert a._rev[mid] == b._rev[mid] == 1
    assert a.value(mid) == b.value(mid)


def test_msgpack_connection_mirrors_with_binary_frames():
    session = Session()
    server = Server(session)
    client = Client(codec="msgpack")
    d = Device(name="lamp")
    mid = session.host(d)

    snaps = server.open("c1", codec="msgpack")
    assert all(isinstance(m, bytes) for m in snaps)  # binary frames over the wire
    for m in snaps:
        client.recv(m)
    assert client.model(mid, Device) == d

    d.on = True
    out = server.flush()["c1"]
    assert all(isinstance(m, bytes) for m in out)
    for m in out:
        client.recv(m)
    assert client.model(mid, Device).on is True


def test_cbor_connection_mirrors_with_binary_frames():
    session = Session()
    server = Server(session)
    client = Client(codec="cbor")
    d = Device(name="lamp")
    mid = session.host(d)

    snaps = server.open("c1", codec="cbor")
    assert all(isinstance(m, bytes) for m in snaps)  # CBOR frames are binary
    for m in snaps:
        client.recv(m)
    assert client.model(mid, Device) == d

    d.on = True
    out = server.flush()["c1"]
    assert all(isinstance(m, bytes) for m in out)
    for m in out:
        client.recv(m)
    assert client.model(mid, Device).on is True


def test_mixed_codecs_per_connection():
    session = Session()
    server = Server(session)
    j, m = Client(), Client(codec="msgpack")
    d = Device(name="lamp")
    mid = session.host(d)
    for msg in server.open("j"):
        j.recv(msg)
    for msg in server.open("m", codec="msgpack"):
        m.recv(msg)

    d.name = "desk"
    out = server.flush()
    assert all(isinstance(x, str) for x in out["j"])  # JSON client gets text
    assert all(isinstance(x, bytes) for x in out["m"])  # msgpack client gets binary
    for msg in out["j"]:
        j.recv(msg)
    for msg in out["m"]:
        m.recv(msg)
    assert j.model(mid, Device).name == "desk"
    assert m.model(mid, Device).name == "desk"


def test_msgpack_client_edit_relays_to_json_client():
    session = Session()
    server = Server(session)
    j, m = Client(), Client(codec="msgpack")
    d = Device(name="lamp")
    mid = session.host(d)
    for msg in server.open("j"):
        j.recv(msg)
    for msg in server.open("m", codec="msgpack"):
        m.recv(msg)

    edit = m.edit(mid, to_value(Device(name="lamp", on=True)))  # msgpack client edits (binary frame)
    assert isinstance(edit, bytes)
    out = server.recv("m", edit)
    assert set(out) == {"j", "m"}  # echoed to all, each in its own codec
    assert all(isinstance(x, bytes) for x in out["m"])  # msgpack origin gets binary
    for msg in out["j"]:  # the JSON client gets it re-encoded as text
        assert isinstance(msg, str)
        j.recv(msg)
    assert j.model(mid, Device).on is True


def test_browser_websocket_path_mirrors_frames():
    """`Client._connect_browser` (the Pyodide path) rides the browser WebSocket through the js FFI.
    Fake `js` / `pyodide.ffi` in sys.modules to drive the wiring off-browser: the codec rides the
    query string, text and binary (ArrayBuffer -> bytes) frames update the mirror, close resolves."""
    import asyncio
    import sys
    import types
    import typing

    class FakeBuffer:
        def __init__(self, data: bytes):
            self._data = data

        def to_bytes(self) -> bytes:
            return self._data

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

    class FakeSocket:
        instances: typing.ClassVar[list] = []

        def __init__(self, url: str):
            self.url = url
            self.listeners = {}
            FakeSocket.instances.append(self)

        def addEventListener(self, name: str, cb) -> None:
            self.listeners[name] = cb

    js_mod = types.ModuleType("js")
    js_mod.WebSocket = types.SimpleNamespace(new=FakeSocket)
    pyodide_mod = types.ModuleType("pyodide")
    ffi_mod = types.ModuleType("pyodide.ffi")
    ffi_mod.create_proxy = FakeProxy
    pyodide_mod.ffi = ffi_mod
    saved = {name: sys.modules.get(name) for name in ("js", "pyodide", "pyodide.ffi")}
    sys.modules.update({"js": js_mod, "pyodide": pyodide_mod, "pyodide.ffi": ffi_mod})
    try:

        async def run():
            sess = Session()
            d = Device(name="lamp")
            mid = sess.host(d)
            server = Server(sess)

            client = Client(codec="msgpack")
            task = asyncio.ensure_future(client._connect_browser("ws://host/ws"))
            await asyncio.sleep(0)  # let the task build the socket + register listeners
            sock = FakeSocket.instances[-1]
            assert "codec=msgpack" in sock.url

            for wire in server.open(("conn"), "msgpack"):  # binary frames, as the browser delivers them
                sock.listeners["message"](FakeEvent(FakeBuffer(wire)))
            d.name = "beacon"
            for msgs in server.flush().values():
                for wire in msgs:
                    sock.listeners["message"](FakeEvent(FakeBuffer(wire)))
            sock.listeners["close"](FakeEvent(None))
            await asyncio.wait_for(task, 1)
            assert client.model(mid, Device).name == "beacon"

        asyncio.run(run())
    finally:
        for name, mod in saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod


def test_client_send_awaits_native_senders_and_raises_when_unconnected():
    """`send` raises without an active connection and awaits an async (native websockets) sender;
    `propose` is `send(edit(...))`."""
    import asyncio

    async def run():
        client = Client()
        try:
            await client.send("x")
            raise AssertionError("expected RuntimeError")
        except RuntimeError:
            pass

        snap = to_value(Device(name="lamp"))
        client.recv(protocol.snapshot_msg(1, "Device", 0, snap))
        sent = []

        async def sender(frame):  # the native path's ws.send coroutine
            sent.append(frame)

        client._sender = sender
        await client.propose(1, to_value(Device(name="beacon")))
        assert len(sent) == 1
        msg = json.loads(sent[0])
        assert msg["t"] == "patch"
        assert msg["patch"]["ops"][0]["Set"]["value"] == {"Str": "beacon"}

    asyncio.run(run())


def test_browser_run_reconnects_with_resume_and_client_authority():
    """`Client._run_browser` (the Pyodide `run`) reconnects with `?since=` resume and, under client
    authority, pushes the pre-drop state back once the server has (re)snapshotted."""
    import asyncio
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

    class FakeSocket:
        instances: typing.ClassVar[list] = []

        def __init__(self, url: str):
            self.url = url
            self.listeners = {}
            self.sent = []
            FakeSocket.instances.append(self)

        def addEventListener(self, name: str, cb) -> None:
            self.listeners[name] = cb

        def send(self, frame) -> None:
            self.sent.append(frame)

    js_mod = types.ModuleType("js")
    js_mod.WebSocket = types.SimpleNamespace(new=FakeSocket)
    pyodide_mod = types.ModuleType("pyodide")
    ffi_mod = types.ModuleType("pyodide.ffi")
    ffi_mod.create_proxy = FakeProxy
    ffi_mod.to_js = lambda x: x
    pyodide_mod.ffi = ffi_mod
    saved = {name: sys.modules.get(name) for name in ("js", "pyodide", "pyodide.ffi")}
    sys.modules.update({"js": js_mod, "pyodide": pyodide_mod, "pyodide.ffi": ffi_mod})
    try:

        async def run():
            sess = Session()
            d = Device(name="lamp")
            mid = sess.host(d)
            server = Server(sess)

            client = Client()
            task = asyncio.ensure_future(client._run_browser("ws://host/ws", authority="client", retry=0.01))
            await asyncio.sleep(0)
            first = FakeSocket.instances[-1]
            for wire in server.open("c1"):
                first.listeners["message"](FakeEvent(wire))
            assert client.model(mid, Device).name == "lamp"

            first.listeners["close"](FakeEvent(None))  # drop; the loop retries
            for _ in range(100):
                await asyncio.sleep(0.01)
                if FakeSocket.instances[-1] is not first:
                    break
            second = FakeSocket.instances[-1]
            assert second is not first
            assert "since=" in second.url  # resume: the server replays only the delta

            for wire in server.open("c2"):
                second.listeners["message"](FakeEvent(wire))
            # client authority: once the server (re)snapshots, the pre-drop state is pushed back
            assert second.sent and json.loads(second.sent[0])["t"] == "patch"

            # the managed connection exposes an outbound channel: propose without owning the socket
            await client.propose(mid, {"Map": {"name": {"Str": "manual"}}})
            assert json.loads(second.sent[-1])["patch"]["ops"][0]["Set"]["value"] == {"Str": "manual"}

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            try:
                await client.send("x")  # the drop cleared the channel
                raise AssertionError("expected RuntimeError")
            except RuntimeError:
                pass

            # binary frames go through pyodide.ffi.to_js on the way out
            Client._send_browser(second, b"\x01")
            assert second.sent[-1] == b"\x01"

        asyncio.run(run())
    finally:
        for name, mod in saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod


def test_starlette_msgpack_connection():
    from starlette.applications import Starlette
    from starlette.routing import WebSocketRoute
    from starlette.testclient import TestClient

    session = Session()
    server = Server(session)
    d = Device(name="lamp")
    mid = session.host(d)
    app = Starlette(routes=[WebSocketRoute("/ws", ws_endpoint(server))])

    with TestClient(app) as tc, tc.websocket_connect("/ws?codec=msgpack") as ws:
        client = Client(codec="msgpack")
        client.recv(ws.receive_bytes())  # snapshot as a binary frame
        assert client.model(mid, Device) == d

        # client edits and sends a binary frame; server applies it
        edit = client.edit(mid, to_value(Device(name="lamp", on=True)))
        assert isinstance(edit, bytes)
        ws.send_bytes(edit)


def test_starlette_connect_snapshot_and_relay():
    from starlette.applications import Starlette
    from starlette.routing import WebSocketRoute
    from starlette.testclient import TestClient

    session = Session()
    server = Server(session)
    d = Device(name="lamp")
    mid = session.host(d)
    app = Starlette(routes=[WebSocketRoute("/ws", ws_endpoint(server))])

    with TestClient(app) as tc, tc.websocket_connect("/ws") as ws1, tc.websocket_connect("/ws") as ws2:
        snap1 = json.loads(ws1.receive_text())
        snap2 = json.loads(ws2.receive_text())
        assert snap1["t"] == "snapshot" and snap1["id"] == mid and snap1["value"] == json.loads(json.dumps(to_value(d)))
        assert snap2["id"] == mid

        # ws1 -> server -> relayed to ws2
        ws1.send_text(protocol.patch_msg(mid, {"rev": 1, "ops": [{"Set": {"path": [{"Key": "on"}], "value": {"Bool": True}}}]}))
        relayed = json.loads(ws2.receive_text())
        assert relayed["t"] == "patch" and relayed["id"] == mid

        client = Client()
        client.recv(json.dumps(snap2))
        client.recv(json.dumps(relayed))
        assert client.model(mid, Device).on is True
