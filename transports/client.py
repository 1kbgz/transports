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
            self._values[mid] = json.loads(_apply(json.dumps(self._values[mid]), json.dumps(msg["patch"])))
            self._rev[mid] = msg["patch"]["rev"]

    def value(self, mid: int) -> Any:
        """The current mirrored core `Value` of a model."""
        return self._values[mid]

    def model(self, mid: int, cls: Type[M]) -> M:
        """Materialize the mirrored model as an instance of `cls`."""
        return from_value(self._values[mid], cls)

    def ids(self) -> List[int]:
        return list(self._values)

    def edit(self, mid: int, new_value: Any) -> Union[str, bytes]:
        """Locally edit a mirrored model and return the patch frame to send (encoded in this codec)."""
        patch = json.loads(_diff(json.dumps(self._values[mid]), json.dumps(new_value)))
        patch["rev"] = self._rev[mid] + 1
        self._values[mid] = new_value
        self._rev[mid] = patch["rev"]
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
