"""Mirror a remote `Session` from connection messages.

`Client.recv(text)` applies snapshot/patch messages to a local mirror using the core `apply`, so the
client tracks each remote model's value without hosting it. `connect(url)` runs a real WebSocket
client loop for live use; the rest of the class is sync and transport-agnostic (testable without a
network).
"""

import json
import urllib.parse
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

    def recv(self, data: str | bytes) -> None:
        """Apply an inbound snapshot or patch message (text or binary frame) to the local mirror."""
        msg = protocol.decode(data, self._codec)
        mid = msg["id"]
        if msg["t"] == "snapshot":
            self._values[mid] = msg["value"]
            self._type[mid] = msg["type"]
            self._rev[mid] = msg["rev"]
        elif msg["t"] == "patch":
            rev = msg["patch"]["rev"]
            # rev is the model's sequence number; ignore a patch already reflected in the mirror (e.g. a
            # patch the opening snapshot already captured, which the server then also broadcasts).
            if mid in self._rev and rev <= self._rev[mid]:
                return
            self._values[mid] = json.loads(_apply(json.dumps(self._values[mid]), json.dumps(msg["patch"])))
            self._rev[mid] = rev

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
        """Connect to a transports server and mirror it until the connection closes (one connection)."""
        import websockets

        async with websockets.connect(self._connect_url(url)) as ws:
            async for frame in ws:
                self.recv(frame)

    async def run(self, url: str, *, authority: str = "server", retry: float = 1.0) -> None:
        """Connect and mirror, **reconnecting** whenever the connection drops — so the client survives a
        server restart or a network blip. ``authority`` decides reconciliation on each (re)connect:

        - ``"server"`` (default): the server is canonical; the client adopts its state (resuming from
          ``?since=`` when it can, else a fresh snapshot). This is the "refetch on refresh" behavior.
        - ``"client"``: the client is canonical; after the server's snapshot it **pushes its last-known
          state back** as an edit, so a server that came back stale or empty is rectified from the client.
          With a CRDT model the push merges (newer stamps win); otherwise it overwrites.

        Runs until cancelled. The choice of *where the authoritative state lives* is yours — pair this
        with the server-side durability hooks (`Hub.on_shared_write`) as your use case needs.
        """
        import asyncio

        import websockets

        if authority not in ("server", "client"):
            raise ValueError(f"authority must be 'server' or 'client', not {authority!r}")
        while True:
            pre = dict(self._values) if authority == "client" else None
            pushed: set = set()
            try:
                async with websockets.connect(self._connect_url(url)) as ws:
                    async for frame in ws:
                        self.recv(frame)
                        if pre:  # rectify: once the server has (re)snapshotted a model, push our copy back
                            for mid in list(self._values):
                                if mid not in pushed and mid in pre:
                                    await ws.send(self.edit(mid, pre[mid]))
                                    pushed.add(mid)
            except (websockets.ConnectionClosed, OSError):
                pass  # dropped — fall through to retry
            await asyncio.sleep(retry)

    async def connect_sse(self, url: str) -> None:
        """Mirror a transports server over Server-Sent Events (receive-only) until the stream closes.

        SSE is a one-way server→client channel, so this only receives snapshots and patches; use
        `connect()` (WebSocket) when the client also needs to send edits.
        """
        import httpx
        from httpx_sse import aconnect_sse

        async with httpx.AsyncClient() as http, aconnect_sse(http, "GET", url) as source:
            async for event in source.aiter_sse():
                self.recv(event.data)
