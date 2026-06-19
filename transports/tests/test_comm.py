from typing import Any

from pydantic import BaseModel

from transports import Client, Hub, Server, Session, pump_comms, serve_comm


class Device(BaseModel):
    name: str
    on: bool = False


class FakeComm:
    """A duck-typed Jupyter comm: records sent data, lets a test fire an inbound message."""

    def __init__(self) -> None:
        self.sent: list = []
        self._on_msg: Any = None
        self.closed = False

    def send(self, data=None, metadata=None, buffers=None) -> None:
        self.sent.append(data)

    def on_msg(self, cb) -> None:
        self._on_msg = cb

    def on_close(self, cb) -> None:
        self._on_close = cb

    def fire(self, data) -> None:
        self._on_msg({"content": {"data": data}})


def test_serve_comm_sends_opening_snapshot():
    session = Session()
    server = Server(session)
    d = Device(name="lamp")
    mid = session.host(d)
    comm = FakeComm()

    serve_comm(server, comm)
    assert len(comm.sent) == 1
    client = Client()
    client.recv(comm.sent[0])
    assert client.model(mid, Device) == d


def test_comm_inbound_edit_is_applied_and_echoed():
    session = Session()
    server = Server(session)
    d = Device(name="lamp")
    mid = session.host(d)
    a, b = FakeComm(), FakeComm()
    serve_comm(server, a)
    serve_comm(server, b)

    client = Client()
    client.recv(a.sent[0])
    a.sent.clear()
    b.sent.clear()

    a.fire(client.edit(mid, {"Map": {"name": {"Str": "lamp"}, "on": {"Bool": True}}}))
    # server-authoritative: both comms (incl. origin) receive the echo, host object updates
    assert len(a.sent) == 1 and len(b.sent) == 1
    client.recv(b.sent[0])
    assert client.model(mid, Device).on is True
    assert d.on is True


def test_pump_comms_delivers_host_side_changes():
    session = Session()
    server = Server(session)
    d = Device(name="lamp")
    session.host(d)
    comm = FakeComm()
    serve_comm(server, comm)
    comm.sent.clear()

    d.name = "desk"
    pump_comms(server)
    assert len(comm.sent) == 1


def test_serve_comm_works_with_hub():
    hub = Hub(key=lambda c: "t")
    d = Device(name="lamp")
    sid = hub.share(d)
    hub.subscribe("t", sid)
    comm = FakeComm()
    serve_comm(hub, comm)
    assert len(comm.sent) == 1  # the shared model's snapshot
    client = Client()
    client.recv(comm.sent[0])
    assert client.model(sid, Device) == d
