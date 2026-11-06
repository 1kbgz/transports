"""Serve a `Session`/`Hub` over a Jupyter comm channel.

A Jupyter **comm** is a bidirectional message channel between a kernel and a frontend (the transport
behind ipywidgets). It carries JSON-able data natively, so transports rides it by sending the wire
string as the comm's ``data`` for text-only messages. Binary values use a small JSON header
and separate comm buffers; the same `Client.recv`/`edit` work after buffer reassembly.

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
from ._buffers import join_buffers, split_buffers
from .server import Broadcaster


class _CommConn:
    """A Jupyter comm as a transports connection handle: a wire is sent as the comm's ``data``.

    Wrapping the comm gives every connection a uniform ``send(wire)``, so `sync`/`autosync` deliver to
    comms, anywidgets, and sockets the same way."""

    __slots__ = ("codec", "comm")

    def __init__(self, comm: Any, codec: str) -> None:
        self.comm = comm
        self.codec = codec

    def send(self, wire: Any) -> None:
        data, buffers = split_buffers(wire, self.codec)
        if buffers:
            self.comm.send(data=data, buffers=buffers)
        else:
            self.comm.send(data=data)


def serve_comm(server: Broadcaster, comm: Any, codec: str = protocol.JSON, *, ownership: bool = False) -> _CommConn:
    """Wire a comm to a `Server`/`Hub`: send the opening snapshots and relay inbound messages.

    The comm is registered as a connection; its messages (``msg["content"]["data"]``) are fed to
    ``server.recv`` and any resulting messages are sent back over the relevant connections. Returns the
    connection handle. Call `sync(server)` after host-side mutations to push changes.
    """
    codec = protocol.normalize_codec(codec)
    if codec not in (protocol.JSON, protocol.MSGPACK, protocol.CBOR):
        raise ValueError("comm buffers require a built-in codec")
    conn = _CommConn(comm, codec)
    for wire in server.open(conn, codec, ownership=ownership):
        conn.send(wire)

    def _on_msg(msg: dict) -> None:
        for target, msgs in server.recv(conn, join_buffers(msg["content"]["data"], msg.get("buffers"), codec)).items():
            for m in msgs:
                target.send(m)

    comm.on_msg(_on_msg)

    def _on_close(_msg: Any) -> None:
        for target, messages in server.close(conn).items():
            for message in messages:
                target.send(message)

    comm.on_close(_on_close)
    return conn
