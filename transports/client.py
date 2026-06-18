"""Mirror a remote `Session` from connection messages.

`Client.recv(text)` applies snapshot/patch messages to a local mirror using the core `apply`, so the
client tracks each remote model's value without hosting it. `connect(url)` runs a real WebSocket
client loop for live use; the rest of the class is sync and transport-agnostic (testable without a
network).
"""

import json
from typing import Any, Dict, List, Type

from . import protocol
from ._bridge import M, from_value
from .transports import apply as _apply, diff as _diff


class Client:
    def __init__(self) -> None:
        self._values: Dict[int, Any] = {}
        self._rev: Dict[int, int] = {}
        self._type: Dict[int, str] = {}

    def recv(self, text: str) -> None:
        """Apply an inbound snapshot or patch message to the local mirror."""
        msg = protocol.parse(text)
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

    def edit(self, mid: int, new_value: Any) -> str:
        """Locally edit a mirrored model and return the patch message to send to the server."""
        patch = json.loads(_diff(json.dumps(self._values[mid]), json.dumps(new_value)))
        patch["rev"] = self._rev[mid] + 1
        self._values[mid] = new_value
        self._rev[mid] = patch["rev"]
        return protocol.patch_msg(mid, patch)

    async def connect(self, url: str) -> None:
        """Connect to a transports server and mirror it until the connection closes."""
        import websockets

        async with websockets.connect(url) as ws:
            async for text in ws:
                self.recv(text if isinstance(text, str) else text.decode())
