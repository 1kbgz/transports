import json

from pydantic import BaseModel

from transports import Client, Server, Session, protocol, starlette_endpoint, to_value


class Device(BaseModel):
    name: str
    on: bool = False


# --- transport-agnostic logic (no network) -------------------------------------------------------


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

    msg = a.edit(mid, to_value(Device(name="lamp", on=True)))  # client a edits, sends to server
    out = server.recv("a", msg)
    assert set(out) == {"b"}  # relayed to b, not back to a
    for m in out["b"]:
        b.recv(m)
    assert b.model(mid, Device).on is True


def test_flush_without_connections_is_empty():
    session = Session()
    server = Server(session)
    d = Device(name="lamp")
    session.host(d)
    d.on = True
    assert server.flush() == {}


# --- real Starlette WebSocket I/O ----------------------------------------------------------------


def test_starlette_connect_snapshot_and_relay():
    from starlette.applications import Starlette
    from starlette.routing import WebSocketRoute
    from starlette.testclient import TestClient

    session = Session()
    server = Server(session)
    d = Device(name="lamp")
    mid = session.host(d)
    app = Starlette(routes=[WebSocketRoute("/ws", starlette_endpoint(server))])

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
