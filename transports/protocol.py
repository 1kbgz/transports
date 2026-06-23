"""The connection wire protocol: one logical frame per message, as JSON.

WebSocket messages (and Jupyter comm messages) are self-delimiting, so the binary `Frame` envelope
in the Rust core — which exists for byte-stream transports like TCP — isn't needed here. A small
JSON envelope carries the routing metadata around a model snapshot or a patch.

Two message kinds:

- ``{"t": "snapshot", "id": <int>, "type": <str>, "rev": <int>, "value": <Value>}``
- ``{"t": "patch", "id": <int>, "patch": {"rev": <int>, "ops": [...]}}``
"""

import json
from typing import Any, Callable, Dict, Tuple, Union

from .transports import (
    cbor_to_json as _cbor_to_json,
    decode_as as _core_decode_as,
    encode_as as _core_encode_as,
    json_to_cbor as _json_to_cbor,
    json_to_msgpack as _json_to_msgpack,
    msgpack_to_json as _msgpack_to_json,
)

#: Canonical codec names. A connection negotiates one of these (e.g. via a ``?codec=`` query param);
#: JSON travels as text frames, MessagePack and CBOR as binary frames.
JSON = "json"
MSGPACK = "msgpack"
CBOR = "cbor"

_BUILTIN = {"", "json", "application/json", "msgpack", "application/msgpack", "x-msgpack", "application/x-msgpack", "cbor", "application/cbor"}

#: Registered custom codecs: ``content_type -> (encode, decode)``. ``encode`` maps a JSON-able object
#: (a protocol message, or a model ``Value``) to wire bytes/str; ``decode`` is its inverse.
_CODECS: Dict[str, Tuple[Callable[[Any], Union[str, bytes]], Callable[[Union[str, bytes]], Any]]] = {}

Codec = Tuple[Callable[[Any], Union[str, bytes]], Callable[[Union[str, bytes]], Any]]


def register_codec(content_type: str, encode: Callable[[Any], Union[str, bytes]], decode: Callable[[Union[str, bytes]], Any]) -> None:
    """Register a custom wire codec under ``content_type``.

    ``encode`` turns a JSON-able object (a protocol message or a model ``Value``) into wire bytes (or
    a str); ``decode`` is its inverse. Once registered, ``content_type`` works anywhere a codec name
    is accepted — ``Client(codec=content_type)``, a ``?codec=`` query param, or ``encode_as`` /
    ``decode_as``. Register a matching implementation in every binding that needs it (the JS binding
    has its own ``registerCodec``). The built-in ``json`` / ``msgpack`` codecs cannot be overridden.
    """
    if content_type in _BUILTIN:
        raise ValueError(f"cannot override built-in codec: {content_type}")
    _CODECS[content_type] = (encode, decode)


def unregister_codec(content_type: str) -> None:
    """Remove a previously registered custom codec."""
    _CODECS.pop(content_type, None)


def registered_codecs() -> Tuple[str, ...]:
    """The content types of the currently registered custom codecs."""
    return tuple(_CODECS)


def normalize_codec(name: Union[str, None]) -> str:
    """Map a codec name or content-type to a canonical name (:data:`JSON`, :data:`MSGPACK`,
    :data:`CBOR`, or a registered custom content type)."""
    if name in (None, "", "json", "application/json"):
        return JSON
    if name in ("msgpack", "application/msgpack", "x-msgpack", "application/x-msgpack"):
        return MSGPACK
    if name in ("cbor", "application/cbor"):
        return CBOR
    if name in _CODECS:
        return name  # a registered content type is its own canonical name
    raise ValueError(f"unknown codec: {name}")


def snapshot_msg(model_id: int, type_name: str, rev: int, value: Any) -> str:
    return json.dumps({"t": "snapshot", "id": model_id, "type": type_name, "rev": rev, "value": value})


def patch_msg(model_id: int, patch: dict) -> str:
    return json.dumps({"t": "patch", "id": model_id, "patch": patch})


def encode(msg_json: str, codec: str = JSON) -> Union[str, bytes]:
    """Encode a JSON message string into the wire form for ``codec``.

    Returns the string unchanged for JSON, MessagePack ``bytes`` for the msgpack codec, or whatever a
    registered custom codec produces — so the caller sends a text or binary frame accordingly.
    """
    c = normalize_codec(codec)
    if c in _CODECS:
        return _CODECS[c][0](json.loads(msg_json))
    if c == MSGPACK:
        return _json_to_msgpack(msg_json)
    if c == CBOR:
        return _json_to_cbor(msg_json)
    return msg_json


def decode(data: Union[str, bytes], codec: Union[str, None] = None) -> dict:
    """Parse an inbound frame to a message dict.

    Pass the connection's ``codec`` to select the decoder (required for custom codecs). With no
    ``codec`` the built-ins are inferred from the frame type (str=JSON, bytes=msgpack).
    """
    if codec is not None:
        c = normalize_codec(codec)
        if c in _CODECS:
            return _CODECS[c][1](data)
        if c == JSON:
            return json.loads(data)
        if c == CBOR:
            return json.loads(_cbor_to_json(bytes(data)))
    if isinstance(data, (bytes, bytearray)):
        return json.loads(_msgpack_to_json(bytes(data)))
    return json.loads(data)


def encode_as(value_json: str, content_type: str) -> bytes:
    """Encode a model ``Value`` (JSON string) to bytes with the named codec (built-in or registered)."""
    if content_type in _CODECS:
        out = _CODECS[content_type][0](json.loads(value_json))
        return out.encode() if isinstance(out, str) else out
    return _core_encode_as(value_json, content_type)


def decode_as(data: Union[str, bytes], content_type: str) -> str:
    """Decode bytes back to a model ``Value`` (JSON string) with the named codec (built-in or registered)."""
    if content_type in _CODECS:
        return json.dumps(_CODECS[content_type][1](data))
    raw = bytes(data) if isinstance(data, (bytes, bytearray)) else data.encode()
    return _core_decode_as(raw, content_type)


def parse(text: str) -> dict:
    return json.loads(text)
