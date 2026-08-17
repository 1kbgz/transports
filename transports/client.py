"""Mirror a remote `Session` from connection messages.

`Client.recv(text)` applies snapshot/patch messages to a local mirror using the core `apply`, so the
client tracks each remote model's value without hosting it. `connect(url)` runs a real WebSocket
client loop for live use; the rest of the class is sync and transport-agnostic (testable without a
network).
"""

import contextlib
import inspect
import json
import sys
import urllib.parse
from collections.abc import Callable
from typing import Any

from . import protocol
from ._bridge import M, from_value
from .transports import apply as _apply, diff as _diff


class Client:
    """Mirrors a remote `Session` — applies snapshot/patch messages to a local copy of each model.

    Read values with `value(id)` or materialize them with `model(id, cls)`. Drive it with a live
    connection via `connect(url)`, or feed it messages directly with `recv(data)`. The `codec`
    (`"json"`, `"msgpack"`, or `"cbor"`) frames outbound edits and decodes inbound frames."""

    def __init__(self, codec: str = protocol.JSON) -> None:
        self._values: dict[int, Any] = {}
        self._rev: dict[int, int] = {}
        self._type: dict[int, str] = {}
        self._codec = protocol.normalize_codec(codec)
        self._change_cbs: list[Callable[[dict], None]] = []
        self._reject_cbs: list[Callable[[dict], None]] = []
        #: outbound channel of the active managed connection (set by `connect`/`run`, cleared on drop)
        self._sender: Callable[[str | bytes], Any] | None = None

    @property
    def connected(self) -> bool:
        """Whether a managed connection (`connect` / `run`) is open right now."""
        return self._sender is not None

    async def send(self, frame: str | bytes) -> bool:
        """Send a frame over the active managed connection (`connect` / `run`).

        Returns ``True`` when handed to an open connection, ``False`` when none is active (never
        connected, in a reconnect gap, or receive-only `connect_sse`) — the frame is dropped, like a
        browser WebSocket's send on a closed socket, so an adapter can pass ``client.send`` as a
        fire-and-forget callback; check `connected` (or the return) when it matters."""
        if self._sender is None:
            return False
        result = self._sender(frame)
        if inspect.isawaitable(result):
            await result
        return True

    async def propose(self, mid: int, new_value: Any) -> bool:
        """Propose an edit over the active connection: ``send(edit(mid, new_value))``.

        Server-authoritative — the mirror updates when the authoritative patch echoes back (or
        `on_reject` fires with why it was refused). Returns ``False`` (dropped) when not connected."""
        return await self.send(self.edit(mid, new_value))

    def on_change(self, callback: Callable[[dict], None]) -> Callable[[], None]:
        """Register a callback fired after each accepted snapshot or patch — the same change dict
        `recv` returns — so `connect`/`run`/`connect_sse` consumers get path-level changes without
        managing the socket themselves. Not fired for ignored frames (stale revision, unknown message
        type). Returns an unsubscribe function."""
        self._change_cbs.append(callback)
        return lambda: self._change_cbs.remove(callback)

    def on_reject(self, callback: Callable[[dict], None]) -> Callable[[], None]:
        """Register a callback fired when the server refuses a proposed edit — with the decoded
        ``reject`` message (``{"t": "reject", "id", "rev", "error"}``). The mirror itself reverts via
        the authoritative snapshot the server sends alongside. Returns an unsubscribe function."""
        self._reject_cbs.append(callback)
        return lambda: self._reject_cbs.remove(callback)

    def _accepted(self, change: dict) -> dict:
        for callback in list(self._change_cbs):
            callback(change)
        return change

    def recv(self, data: str | bytes) -> dict | None:
        """Apply an inbound snapshot or patch message (text or binary frame) to the local mirror.

        Returns the accepted change so reactive consumers can update only its paths —
        ``{"t": "snapshot", "id", "rev"}`` for a snapshot, the decoded patch message for a patch — or
        ``None`` for a patch whose revision was already applied, a ``reject`` (dispatched to
        `on_reject`), and an unrecognized message type, which is ignored so a newer server can add
        message types without breaking older clients. Invalid frames raise without changing the
        mirror or its accepted revision."""
        msg = protocol.decode(data, self._codec)
        t = msg.get("t")
        if t == "snapshot":
            mid: int = msg["id"]
            self._values[mid] = msg["value"]
            self._type[mid] = msg["type"]
            self._rev[mid] = msg["rev"]
            return self._accepted({"t": "snapshot", "id": mid, "rev": msg["rev"]})
        elif t == "patch":
            mid = msg["id"]
            rev = msg["patch"]["rev"]
            # rev is the model's sequence number; ignore a patch already reflected in the mirror (e.g. a
            # patch the opening snapshot already captured, which the server then also broadcasts).
            if mid in self._rev and rev <= self._rev[mid]:
                return None
            if mid not in self._values:
                raise ValueError(f"patch received before snapshot for model {mid}")
            self._values[mid] = json.loads(_apply(json.dumps(self._values[mid]), json.dumps(msg["patch"])))
            self._rev[mid] = rev
            return self._accepted(msg)
        elif t == "reject":
            for callback in list(self._reject_cbs):
                callback(msg)
            return None
        # an unrecognized message type is ignored (not an error): the server may be newer than this client
        return None

    def value(self, mid: int) -> Any:
        """The current mirrored core `Value` of a model."""
        return self._values[mid]

    def model(self, mid: int, cls: type[M]) -> M:
        """Materialize the mirrored model as an instance of `cls`."""
        return from_value(self._values[mid], cls)

    def ids(self) -> list[int]:
        return list(self._values)

    def edit(self, mid: int, new_value: Any) -> str | bytes:
        """Propose an edit to a mirrored model; returns the patch frame to send (encoded in this codec).

        Models are server-authoritative: the edit is a proposal, and the local mirror updates only
        when the server echoes the authoritative patch back (via `recv`), not optimistically. This
        keeps `rev` owned by the server and avoids client/server `rev` divergence.
        """
        patch = json.loads(_diff(json.dumps(self._values[mid]), json.dumps(new_value)))
        return protocol.encode(protocol.patch_msg(mid, patch), self._codec)

    def _connect_url(self, url: str) -> str:
        """``url`` + ``?codec=``, plus ``?since=`` (last-seen rev per model) when this client already
        mirrors models, so a reconnect resumes from the delta instead of re-sending each whole model."""
        sep = "&" if "?" in url else "?"
        params = f"codec={self._codec}"
        if self._rev:
            params += "&since=" + urllib.parse.quote(json.dumps(self._rev))
        return f"{url}{sep}{params}"

    async def connect(self, url: str) -> None:
        """Connect to a transports server and mirror it until the connection closes (one connection).

        Under Pyodide (``sys.platform == "emscripten"``) the browser gives Python no raw sockets, so
        this rides the browser's native ``WebSocket`` through the ``js`` FFI instead of the
        ``websockets`` library — same API, same wire."""
        if sys.platform == "emscripten":  # pragma: no cover - exercised by the in-Pyodide suite
            await self._connect_browser(url)
            return
        import websockets

        async with websockets.connect(self._connect_url(url)) as ws:
            self._sender = ws.send
            try:
                async for frame in ws:
                    self.recv(frame)
            finally:
                self._sender = None

    async def _run_browser(self, url: str, *, authority: str = "server", retry: float = 1.0) -> None:
        """`run` over the browser's native `WebSocket` (the Pyodide path): reconnect forever with
        ``?since=`` resume, same ``authority`` semantics as the native loop."""
        import asyncio

        while True:
            pre = dict(self._values) if authority == "client" else None
            pushed: set = set()

            def _rectify(ws: Any, pre: dict | None = pre, pushed: set = pushed) -> None:
                if not pre:  # server-authoritative (or nothing mirrored before the drop)
                    return
                for mid in list(self._values):
                    if mid not in pushed and mid in pre:
                        self._send_browser(ws, self.edit(mid, pre[mid]))
                        pushed.add(mid)

            # construction/FFI errors fall through to the retry, like the native loop's dropped socket
            with contextlib.suppress(Exception):
                await self._connect_browser(url, on_frame=_rectify)
            await asyncio.sleep(retry)

    @staticmethod
    def _send_browser(ws: Any, frame: str | bytes) -> None:
        """Send a frame over a browser `WebSocket`: text as-is, binary converted for the FFI."""
        if isinstance(frame, str):
            ws.send(frame)
        else:
            from pyodide.ffi import to_js

            ws.send(to_js(frame))

    async def _connect_browser(self, url: str, on_frame: Callable[[Any], None] | None = None) -> None:
        """Mirror over the browser's native `WebSocket` (the Pyodide path) until it closes.

        ``on_frame(ws)`` fires after each applied frame — `_run_browser` uses it to rectify under
        client authority. Split out (and importing ``js`` / ``pyodide.ffi`` lazily) so the wiring is
        testable with fake modules injected into ``sys.modules`` off-browser."""
        import asyncio

        from pyodide.ffi import create_proxy

        from js import WebSocket  # the browser global via the Pyodide FFI

        ws = WebSocket.new(self._connect_url(url))
        ws.binaryType = "arraybuffer"
        closed: asyncio.Future = asyncio.get_running_loop().create_future()

        def _on_message(event: Any) -> None:
            data = event.data
            # a text frame arrives as str; a binary frame as an ArrayBuffer proxy -> bytes
            self.recv(data if isinstance(data, str) else bytes(data.to_bytes()))
            if on_frame is not None:
                on_frame(ws)

        def _on_close(_event: Any) -> None:
            if not closed.done():
                closed.set_result(None)

        def _on_open(_event: Any) -> None:
            # arm the outbound channel only once the socket is open (send during CONNECTING throws)
            self._sender = lambda frame: self._send_browser(ws, frame)

        proxies = [create_proxy(_on_message), create_proxy(_on_open), create_proxy(_on_close), create_proxy(_on_close)]
        for name, proxy in zip(("message", "open", "close", "error"), proxies):
            ws.addEventListener(name, proxy)
        try:
            await closed
        finally:
            self._sender = None
            for proxy in proxies:
                proxy.destroy()

    async def run(self, url: str, *, authority: str = "server", retry: float = 1.0) -> None:
        """Connect and mirror, **reconnecting** whenever the connection drops — so the client survives a
        server restart or a network blip. ``authority`` decides reconciliation on each (re)connect:

        - ``"server"`` (default): the server is canonical; the client adopts its state (resuming from
          ``?since=`` when it can, else a fresh snapshot). This is the "refetch on refresh" behavior.
        - ``"client"``: the client is canonical; after the server's snapshot it **pushes its last-known
          state back** as an edit, so a server that came back stale or empty is rectified from the client.
          With a CRDT model the push merges (newer stamps win); otherwise it overwrites.

        Runs until cancelled. The choice of *where the authoritative state lives* is yours — pair this
        with the server-side durability hooks (`Hub.on_shared_write`) as your use case needs. Under
        Pyodide this rides the browser's native ``WebSocket`` (like `connect`), same semantics.
        """
        import asyncio

        if authority not in ("server", "client"):
            raise ValueError(f"authority must be 'server' or 'client', not {authority!r}")
        if sys.platform == "emscripten":  # pragma: no cover - exercised by the in-Pyodide suite
            await self._run_browser(url, authority=authority, retry=retry)
            return

        import websockets

        while True:
            pre = dict(self._values) if authority == "client" else None
            pushed: set = set()
            try:
                async with websockets.connect(self._connect_url(url)) as ws:
                    self._sender = ws.send
                    try:
                        async for frame in ws:
                            self.recv(frame)
                            if pre:  # rectify: once the server has (re)snapshotted a model, push our copy back
                                for mid in list(self._values):
                                    if mid not in pushed and mid in pre:
                                        await ws.send(self.edit(mid, pre[mid]))
                                        pushed.add(mid)
                    finally:
                        self._sender = None
            except (websockets.ConnectionClosed, OSError):
                pass  # dropped — fall through to retry
            await asyncio.sleep(retry)

    async def connect_sse(self, url: str) -> None:
        """Mirror a transports server over Server-Sent Events (receive-only) until the stream closes.

        SSE is a one-way server→client channel, so this only receives snapshots and patches; use
        `connect()` (WebSocket) when the client also needs to send edits. Under Pyodide this rides
        the browser's native ``EventSource`` (no raw sockets for `httpx`).
        """
        if sys.platform == "emscripten":  # pragma: no cover - exercised by the in-Pyodide suite
            await self._connect_sse_browser(url)
            return
        import httpx
        from httpx_sse import aconnect_sse

        async with httpx.AsyncClient() as http, aconnect_sse(http, "GET", url) as source:
            async for event in source.aiter_sse():
                self.recv(event.data)

    async def _connect_sse_browser(self, url: str) -> None:
        """Mirror over the browser's native `EventSource` (the Pyodide path) until it errors/closes."""
        import asyncio

        from pyodide.ffi import create_proxy

        from js import EventSource  # the browser global via the Pyodide FFI

        source = EventSource.new(url)
        closed: asyncio.Future = asyncio.get_running_loop().create_future()

        def _on_message(event: Any) -> None:
            self.recv(event.data)  # SSE is text-only

        def _on_error(_event: Any) -> None:
            if not closed.done():
                closed.set_result(None)

        on_message, on_error = create_proxy(_on_message), create_proxy(_on_error)
        source.addEventListener("message", on_message)
        source.addEventListener("error", on_error)
        try:
            await closed
        finally:
            source.close()
            on_message.destroy()
            on_error.destroy()
