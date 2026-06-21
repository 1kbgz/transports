"""Mirror a remote `Session` from connection messages.

`Client.recv(text)` applies snapshot/patch messages to a local mirror using the core `apply`, so the
client tracks each remote model's value without hosting it. `connect(url)` runs a real WebSocket
client loop for live use; the rest of the class is sync and transport-agnostic (testable without a
network).
"""

import json
from typing import Any, Dict, List, Type, Union

from . import protocol
from ._bridge import M, from_value
from .transports import apply as _apply, diff as _diff


class Client:
    """Mirrors a remote `Session` — applies snapshot/patch messages to a local copy of each model.

    Read values with `value(id)` or materialize them with `model(id, cls)`. Drive it with a live
    connection via `connect(url)`, or feed it messages directly with `recv(data)`. The `codec`
    (`"json"` or `"msgpack"`) controls how outbound edits are framed; inbound frames are decoded
    automatically from their type (text=JSON, binary=msgpack)."""

    def __init__(self, codec: str = protocol.JSON) -> None:
        self._values: Dict[int, Any] = {}
        self._rev: Dict[int, int] = {}
        self._type: Dict[int, str] = {}
        self._codec = protocol.normalize_codec(codec)

    def recv(self, data: Union[str, bytes]) -> None:
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

    def model(self, mid: int, cls: Type[M]) -> M:
        """Materialize the mirrored model as an instance of `cls`."""
        return from_value(self._values[mid], cls)

    def ids(self) -> List[int]:
        return list(self._values)

    def edit(self, mid: int, new_value: Any) -> Union[str, bytes]:
        """Propose an edit to a mirrored model; returns the patch frame to send (encoded in this codec).

        Models are server-authoritative: the edit is a proposal, and the local mirror updates only
        when the server echoes the authoritative patch back (via `recv`), not optimistically. This
        keeps `rev` owned by the server and avoids client/server `rev` divergence.
        """
        patch = json.loads(_diff(json.dumps(self._values[mid]), json.dumps(new_value)))
        return protocol.encode(protocol.patch_msg(mid, patch), self._codec)

    async def connect(self, url: str) -> None:
        """Connect to a transports server and mirror it until the connection closes.

        Appends ``?codec=`` for the client's codec so the server frames messages to match.
        """
        import websockets

        sep = "&" if "?" in url else "?"
        async with websockets.connect(f"{url}{sep}codec={self._codec}") as ws:
            async for frame in ws:
                self.recv(frame)

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
