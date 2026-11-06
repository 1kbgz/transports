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

import asyncio
import math
from typing import Any

from . import protocol
from ._buffers import join_buffers, split_buffers
from .server import Broadcaster


class _WidgetConn:
    """An anywidget as a transports connection handle: a wire becomes ``widget.send({"wire": wire})``."""

    __slots__ = ("codec", "view", "widget")

    def __init__(self, widget: Any, codec: str, view: str | None = None) -> None:
        self.widget = widget
        self.codec = codec
        self.view = view

    def send(self, wire: Any) -> None:
        data, buffers = split_buffers(wire, self.codec)
        content = {"wire": data}
        if self.view is not None:
            content["view"] = self.view
        if buffers:
            self.widget.send(content, buffers=buffers)
        else:
            self.widget.send(content)


def widget(server: Broadcaster) -> Any:
    """A turnkey anywidget serving ``server``: display it and every hosted model mirrors live.

    Builds an ``anywidget.AnyWidget`` whose frontend (shipped inside the wheel at
    ``transports/extension/cdn/widget.js``) opens the wire, renders a live view of each mirrored
    model, bubbles ``transports-change`` / ``transports-reject`` DOM events, and exposes
    ``el.transports = {client, edit}`` for custom frontends. Its self-contained bundle embeds the
    shared WASM client state. Push kernel-side changes with ``transports.sync(server)``.

    Lazy-imports ``anywidget`` (``pip install anywidget``); works under Jupyter and JupyterLite.
    """
    import pathlib

    import anywidget as _anywidget

    class TransportsWidget(_anywidget.AnyWidget):
        _esm = pathlib.Path(__file__).parent / "extension" / "cdn" / "widget.js"

    instance = TransportsWidget()
    serve_anywidget(server, instance)
    return instance


def serve_anywidget(server: Broadcaster, widget: Any, codec: str = "json", *, view_timeout: float = 180) -> _WidgetConn:
    """Wire a widget to a `Server`/`Hub`. Returns the connection handle; call `sync(server)` to push.

    The widget's custom messages drive the protocol: ``{"ready": true}`` triggers the opening snapshots,
    and any ``{"wire": <wire>}`` is relayed to ``server.recv`` (a client's edits), with results sent back
    over the relevant widgets.
    """
    if not math.isfinite(view_timeout) or view_timeout <= 0:
        raise ValueError("view_timeout must be positive and finite")
    codec = protocol.normalize_codec(codec)
    if codec not in (protocol.JSON, protocol.MSGPACK, protocol.CBOR):
        raise ValueError("anywidget buffers require a built-in codec")
    conn = _WidgetConn(widget, codec)
    connections: dict[str | None, _WidgetConn] = {}
    leases: dict[str, asyncio.TimerHandle] = {}

    def close(view: str | None) -> None:
        if view in leases:
            leases.pop(view).cancel()
        target_conn = connections.pop(view)
        for target, messages in server.close(target_conn).items():
            for message in messages:
                target.send(message)

    def expire(view: str) -> None:
        close(view)
        widget.send({"expired": True, "view": view})

    def renew(view: str) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if view in leases:
            leases[view].cancel()
        leases[view] = loop.call_later(view_timeout, expire, view)
        widget.send({"heartbeat": view_timeout / 3, "view": view})

    def _on_msg(_widget: Any, content: Any, _buffers: Any = None) -> None:
        if not isinstance(content, dict):
            return
        view = content.get("view")
        if view is not None and (not isinstance(view, str) or not view):
            return
        target_conn = connections.get(view)
        if content.get("ready") and target_conn is None:
            target_conn = conn if view is None else _WidgetConn(widget, codec, view)
            opening = server.open(target_conn, codec, ownership=bool(content.get("ownership")))
            connections[view] = target_conn
            if view is not None and content.get("heartbeat"):
                renew(view)
            for wire in opening:
                target_conn.send(wire)
        elif content.get("closed") and target_conn is not None:
            close(view)
        elif content.get("heartbeat") and view is not None:
            if view in leases:
                renew(view)
            elif target_conn is None:
                widget.send({"expired": True, "view": view})
        elif "wire" in content and target_conn is not None:
            for target, messages in server.recv(target_conn, join_buffers(content["wire"], _buffers, codec)).items():
                for message in messages:
                    target.send(message)

    widget.on_msg(_on_msg)
    return conn
