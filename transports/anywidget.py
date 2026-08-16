"""Serve a `Server`/`Hub` over an anywidget (ipywidgets `DOMWidget`) — the widget *is* the connection.

Like `serve_comm`, but for an anywidget's custom-message channel rather than a raw Jupyter comm: the
wire rides ``widget.send({"wire": <wire>})`` outbound and the widget's ``on_msg`` inbound. The frontend
signals ``{"ready": true}`` once its view exists; only then are the opening snapshots sent (a view that
isn't listening yet would miss them). The same `Client.recv`/`edit` run unchanged on the frontend.

This never imports `anywidget`/`ipywidgets`: it duck-types the widget (``send`` / ``on_msg``), so it is
testable with a fake widget and keeps the Jupyter dependency on the caller.

```python
import transports

session = transports.Session(); session.host(model)
server = transports.Server(session)

transports.serve_anywidget(server, my_widget)   # wires ready->snapshots + inbound
# ... mutate model(s) ...
transports.sync(server)                         # push host-side changes to every connection
```
"""

from typing import Any

from . import protocol
from .server import Broadcaster


class _WidgetConn:
    """An anywidget as a transports connection handle: a wire becomes ``widget.send({"wire": wire})``."""

    __slots__ = ("widget",)

    def __init__(self, widget: Any) -> None:
        self.widget = widget

    def send(self, wire: Any) -> None:
        self.widget.send({"wire": wire})


def widget(server: Broadcaster) -> Any:
    """A turnkey anywidget serving ``server``: display it and every hosted model mirrors live.

    Builds an ``anywidget.AnyWidget`` whose frontend (shipped inside the wheel at
    ``transports/extension/cdn/widget.js``) opens the wire, renders a live view of each mirrored
    model, bubbles ``transports-change`` / ``transports-reject`` DOM events, and exposes
    ``el.transports = {client, edit}`` for custom frontends. Mirroring and edits are pure TS — the
    frontend never fetches wasm. Push kernel-side changes with ``transports.sync(server)``.

    Lazy-imports ``anywidget`` (``pip install anywidget``); works under Jupyter and JupyterLite.
    """
    import pathlib

    import anywidget as _anywidget

    class TransportsWidget(_anywidget.AnyWidget):
        _esm = pathlib.Path(__file__).parent / "extension" / "cdn" / "widget.js"

    instance = TransportsWidget()
    serve_anywidget(server, instance)
    return instance


def serve_anywidget(server: Broadcaster, widget: Any, codec: str = "json") -> _WidgetConn:
    """Wire a widget to a `Server`/`Hub`. Returns the connection handle; call `sync(server)` to push.

    The widget's custom messages drive the protocol: ``{"ready": true}`` triggers the opening snapshots,
    and any ``{"wire": <wire>}`` is relayed to ``server.recv`` (a client's edits), with results sent back
    over the relevant widgets.
    """
    if protocol.normalize_codec(codec) != protocol.JSON:
        raise ValueError(f"the anywidget transport carries JSON-able data only; codec {codec!r} is not supported")
    conn = _WidgetConn(widget)
    opened = []

    def _on_msg(_widget: Any, content: Any, _buffers: Any = None) -> None:
        if not isinstance(content, dict):
            return
        if content.get("ready") and not opened:
            opened.append(True)
            for wire in server.open(conn, codec):
                conn.send(wire)
        elif "wire" in content:
            for target, msgs in server.recv(conn, content["wire"]).items():
                for msg in msgs:
                    target.send(msg)

    widget.on_msg(_on_msg)
    return conn
