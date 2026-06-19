"""The connection wire protocol: one logical frame per message, as JSON.

WebSocket messages (and Jupyter comm messages) are self-delimiting, so the binary `Frame` envelope
in the Rust core — which exists for byte-stream transports like TCP — isn't needed here. A small
JSON envelope carries the routing metadata around a model snapshot or a patch.

Two message kinds:

- ``{"t": "snapshot", "id": <int>, "type": <str>, "rev": <int>, "value": <Value>}``
- ``{"t": "patch", "id": <int>, "patch": {"rev": <int>, "ops": [...]}}``
"""

import json
from typing import Any, Union

from .transports import json_to_msgpack as _json_to_msgpack, msgpack_to_json as _msgpack_to_json

#: Canonical codec names. A connection negotiates one of these (e.g. via a ``?codec=`` query param);
#: JSON travels as text frames, MessagePack as binary frames.
JSON = "json"
MSGPACK = "msgpack"


def normalize_codec(name: Union[str, None]) -> str:
    """Map a codec name or content-type to a canonical :data:`JSON` / :data:`MSGPACK`."""
    if name in (None, "", "json", "application/json"):
        return JSON
    if name in ("msgpack", "application/msgpack", "x-msgpack", "application/x-msgpack"):
        return MSGPACK
    raise ValueError(f"unknown codec: {name}")


def snapshot_msg(model_id: int, type_name: str, rev: int, value: Any) -> str:
    return json.dumps({"t": "snapshot", "id": model_id, "type": type_name, "rev": rev, "value": value})


def patch_msg(model_id: int, patch: dict) -> str:
    return json.dumps({"t": "patch", "id": model_id, "patch": patch})


def encode(msg_json: str, codec: str = JSON) -> Union[str, bytes]:
    """Encode a JSON message string into the wire form for ``codec``.

    Returns the string unchanged for JSON, or MessagePack ``bytes`` for the msgpack codec — so the
    caller sends a text frame or a binary frame accordingly.
    """
    if normalize_codec(codec) == MSGPACK:
        return _json_to_msgpack(msg_json)
    return msg_json


def decode(data: Union[str, bytes]) -> dict:
    """Parse an inbound frame to a message dict, dispatching on frame type (str=JSON, bytes=msgpack)."""
    if isinstance(data, (bytes, bytearray)):
        return json.loads(_msgpack_to_json(bytes(data)))
    return json.loads(data)


def parse(text: str) -> dict:
    return json.loads(text)
