"""Serve a `Session`/`Hub` over a Jupyter comm channel.

A Jupyter **comm** is a bidirectional message channel between a kernel and a frontend (the transport
behind ipywidgets). It carries JSON-able data natively, so transports rides it by sending the wire
*string* as the comm's ``data`` — no extra framing, and the same `Client.recv`/`edit` work unchanged.

This adapter never imports `comm`/`ipykernel`: you pass in a comm (created by your widget, or
`comm.create_comm(...)`), keeping the Jupyter dependency optional. It is therefore also testable with
a duck-typed fake comm exposing ``send`` / ``on_msg`` / ``on_close``.

```python
import transports

session = transports.Session()
session.host(model)
server = transports.Server(session)

transports.serve_comm(server, my_comm)   # sends snapshots, wires inbound messages
# ... mutate model(s) ...
transports.sync(server)                  # push host-side changes to every connection
```
"""

from typing import Any

from . import protocol
from .server import Broadcaster


class _CommConn:
    """A Jupyter comm as a transports connection handle: a wire is sent as the comm's ``data``.

    Wrapping the comm gives every connection a uniform ``send(wire)``, so `sync`/`autosync` deliver to
    comms, anywidgets, and sockets the same way."""

    __slots__ = ("comm",)

    def __init__(self, comm: Any) -> None:
        self.comm = comm

    def send(self, wire: Any) -> None:
        self.comm.send(data=wire)


def serve_comm(server: Broadcaster, comm: Any, codec: str = protocol.JSON) -> _CommConn:
    """Wire a comm to a `Server`/`Hub`: send the opening snapshots and relay inbound messages.

    The comm is registered as a connection; its messages (``msg["content"]["data"]``) are fed to
    ``server.recv`` and any resulting messages are sent back over the relevant connections. Returns the
    connection handle. Call `sync(server)` after host-side mutations to push changes.
    """
    if protocol.normalize_codec(codec) != protocol.JSON:
        raise ValueError(f"the comm transport carries JSON-able data only; codec {codec!r} is not supported")
    conn = _CommConn(comm)
    for wire in server.open(conn, codec):
        conn.send(wire)

    def _on_msg(msg: dict) -> None:
        for target, msgs in server.recv(conn, msg["content"]["data"]).items():
            for m in msgs:
                target.send(m)

    comm.on_msg(_on_msg)
    comm.on_close(lambda _msg: server.close(conn))
    return conn
