import json

from pydantic import BaseModel

import transports


class FakeWidget:
    """Duck-typed anywidget: records sent messages, delivers fired ones to on_msg handlers."""

    def __init__(self):
        self._handlers = []
        self.sent = []

    def on_msg(self, cb):
        self._handlers.append(cb)

    def send(self, content, buffers=None):
        self.sent.append(content)

    def fire(self, content):
        for cb in self._handlers:
            cb(self, content, [])


class Model(BaseModel):
    x: int = 0


def _wires(widget):
    return [json.loads(m["wire"]) for m in widget.sent]


def test_ready_sends_snapshot_then_flush_sends_patch():
    model = Model()
    session = transports.Session()
    session.host(model)
    server = transports.Server(session)

    widget = FakeWidget()
    transports.serve_anywidget(server, widget)

    # nothing sent until the frontend is ready
    assert widget.sent == []

    widget.fire({"ready": True})
    wires = _wires(widget)
    assert len(wires) == 1 and wires[0]["t"] == "snapshot"

    model.x = 5
    transports.sync(server)
    wires = _wires(widget)
    assert wires[-1]["t"] == "patch"


def test_inbound_edit_is_relayed_back_authoritatively():
    model = Model()
    session = transports.Session()
    mid = session.host(model)
    server = transports.Server(session)

    widget = FakeWidget()
    transports.serve_anywidget(server, widget)
    widget.fire({"ready": True})

    # a client-proposed patch comes back over the wire; the server echoes the authoritative patch
    proposal = {"rev": 0, "ops": [{"Set": {"path": [{"Key": "x"}], "value": {"Int": 9}}}]}
    widget.fire({"wire": json.dumps({"t": "patch", "id": mid, "patch": proposal})})
    assert _wires(widget)[-1]["t"] == "patch"
    assert model.x == 9  # the hosted model was refreshed from the authoritative apply
