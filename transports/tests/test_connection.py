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
