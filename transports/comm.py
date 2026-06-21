"""Serve a `Session`/`Hub` over a Jupyter comm channel.

A Jupyter **comm** is a bidirectional message channel between a kernel and a frontend (the transport
behind ipywidgets). It carries JSON-able data natively, so transports rides it by sending the wire
*string* as the comm's ``data`` — no extra framing, and the same `Client.recv`/`edit` work unchanged.
The comm object itself is the connection handle.

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
transports.pump_comms(server)            # push host-side changes to every comm
```
"""

from typing import Any

from . import protocol
from .server import Broadcaster


def serve_comm(server: Broadcaster, comm: Any, codec: str = protocol.JSON) -> Any:
    """Wire a comm to a `Server`/`Hub`: send the opening snapshots and relay inbound messages.

    The comm is registered as a connection; its messages (``msg["content"]["data"]``) are fed to
    ``server.recv`` and any resulting messages are sent back over the relevant comms. Returns the comm
    (the connection handle), so the caller can `pump_comms(server)` after host-side mutations.
    """
    if protocol.normalize_codec(codec) != protocol.JSON:
        raise ValueError(f"the comm transport carries JSON-able data only; codec {codec!r} is not supported")
    for wire in server.open(comm, codec):
        comm.send(data=wire)

    def _on_msg(msg: dict) -> None:
        for target, msgs in server.recv(comm, msg["content"]["data"]).items():
            for m in msgs:
                target.send(data=m)

    comm.on_msg(_on_msg)
    comm.on_close(lambda _msg: server.close(comm))
    return comm


def pump_comms(server: Broadcaster) -> None:
    """Flush host-side changes and push the resulting patches to every connected comm.

    Call after mutating hosted models (e.g. at the end of a cell, or from a timer). Each connection
    handle is a comm, so the patches are delivered with `comm.send`.
    """
    for comm, msgs in server.flush().items():
        for m in msgs:
            comm.send(data=m)
