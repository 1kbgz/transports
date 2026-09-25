"""The connection wire protocol: one logical frame per message, as JSON.

WebSocket messages (and Jupyter comm messages) are self-delimiting, so the binary `Frame` envelope
in the Rust core — which exists for byte-stream transports like TCP — isn't needed here. A small
JSON envelope carries the routing metadata around a model snapshot or a patch.

Live message kinds:

- ``{"t": "snapshot", "id": <int>, "type": <str>, "rev": <int>, "value": <Value>}``
- ``{"t": "patch", "id": <int>, "patch": {"rev": <int>, "ops": [...]}, "proposal": <str>?}``
- ``{"t": "ack", "id": <int>, "rev": <int>, "proposal": <str>}`` — an accepted proposal that
  produced no authoritative patch; sent only to the origin and never changes its mirror.
- ``{"t": "reject", "id": <int>, "rev": <int>, "error": <str>, "proposal": <str>?,
  "crdt_ops": [...]?}`` — a proposed edit was refused; sent to the proposing connection only,
  alongside the authoritative revert. ``rev`` is the server's current revision for the model.
  ``proposal`` is an optional opaque identifier returned only to the origin on acceptance or
  rejection. ``crdt_ops`` lets an optimistic CRDT client settle the refused causal dots before the
  revert. Clients that predate a field or message kind ignore it.
- ``{"t": "crdt_snapshot", ..., "spec": <CrdtSpec>, "state": <CrdtState>}`` — a materialized
  value plus reducer state for a joining or reconnecting replica.
- ``{"t": "crdt", "id": <int>, "rev": <int>, "ops": [...], "effect": {...}?}`` — causally
  identified CRDT operations. Client proposals use revision zero; authoritative echoes carry the
  shared model revision. Senders may include the optional materialized effect as a cache.
- ``{"t": "awareness", "id": <int>, "state": <json|null>, "peer": <str>?}`` — ephemeral state
  scoped to a shared model. Clients omit ``peer``; the Hub assigns it when relaying an update.
  ``null`` removes the peer's state.
"""

import json
from collections.abc import Callable
from typing import Any

from .transports import (
    decode_as as _core_decode_as,
    decode_message as _decode_message,
    encode_as as _core_encode_as,
    encode_message as _encode_message,
    normalize_message as _normalize_message,
)

#: Canonical codec names. A connection negotiates one of these (e.g. via a ``?codec=`` query param);
#: JSON travels as text frames, MessagePack and CBOR as binary frames.
JSON = "json"
MSGPACK = "msgpack"
CBOR = "cbor"

_BUILTIN = {"", "json", "application/json", "msgpack", "application/msgpack", "x-msgpack", "application/x-msgpack", "cbor", "application/cbor"}

#: Registered custom codecs: ``content_type -> (encode, decode)``. ``encode`` maps a JSON-able object
#: (a protocol message, or a model ``Value``) to wire bytes/str; ``decode`` is its inverse.
_CODECS: dict[str, tuple[Callable[[Any], str | bytes], Callable[[str | bytes], Any]]] = {}

Codec = tuple[Callable[[Any], str | bytes], Callable[[str | bytes], Any]]


def register_codec(content_type: str, encode: Callable[[Any], str | bytes], decode: Callable[[str | bytes], Any]) -> None:
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


def registered_codecs() -> tuple[str, ...]:
    """The content types of the currently registered custom codecs."""
    return tuple(_CODECS)


def normalize_codec(name: str | None) -> str:
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


def crdt_snapshot_msg(model_id: int, type_name: str, rev: int, value: Any, spec: dict, state: dict) -> str:
    return json.dumps(
        {
            "t": "crdt_snapshot",
            "id": model_id,
            "type": type_name,
            "rev": rev,
            "value": value,
            "spec": spec,
            "state": state,
        }
    )


def patch_msg(model_id: int, patch: dict, proposal: str | None = None) -> str:
    msg = {"t": "patch", "id": model_id, "patch": patch}
    if proposal is not None:
        msg["proposal"] = proposal
    return json.dumps(msg)


def crdt_msg(
    model_id: int,
    ops: list[dict],
    *,
    rev: int = 0,
    effect: dict | None = None,
    proposal: str | None = None,
) -> str:
    msg = {"t": "crdt", "id": model_id, "rev": rev, "ops": ops}
    if effect is not None:
        msg["effect"] = effect
    if proposal is not None:
        msg["proposal"] = proposal
    return json.dumps(msg)


def ack_msg(model_id: int, rev: int, proposal: str) -> str:
    return json.dumps({"t": "ack", "id": model_id, "rev": rev, "proposal": proposal})


def reject_msg(
    model_id: int,
    rev: int,
    error: str,
    proposal: str | None = None,
    crdt_ops: list[dict] | None = None,
) -> str:
    msg = {"t": "reject", "id": model_id, "rev": rev, "error": error}
    if proposal is not None:
        msg["proposal"] = proposal
    if crdt_ops is not None:
        msg["crdt_ops"] = crdt_ops
    return json.dumps(msg)


def awareness_msg(model_id: int, state: Any | None, peer: str | None = None) -> str:
    """Build ephemeral per-model awareness. ``peer`` is assigned by the server on fan-out."""
    msg = {"t": "awareness", "id": model_id, "state": state}
    if peer is not None:
        msg["peer"] = peer
    return json.dumps(msg)


def batch_msg(msg_jsons: list[str]) -> str:
    """Combine several already-serialized messages into one ``{"t": "batch", "msgs": [...]}`` frame.

    Spliced as strings (each input is valid JSON), so batching adds no re-serialization cost.
    Sent only to connections that negotiated ``batch`` — clients that never asked never see it."""
    return '{"t":"batch","msgs":[' + ",".join(msg_jsons) + "]}"


def encode(msg_json: str, codec: str = JSON) -> str | bytes:
    """Encode a JSON message string into the wire form for ``codec``.

    Returns the string unchanged for JSON, MessagePack ``bytes`` for the msgpack codec, or whatever a
    registered custom codec produces — so the caller sends a text or binary frame accordingly.
    """
    c = normalize_codec(codec)
    if c in _CODECS:
        return _CODECS[c][0](json.loads(msg_json))
    if c == MSGPACK:
        return _encode_message(msg_json, c)
    if c == CBOR:
        return _encode_message(msg_json, c)
    _normalize_message(msg_json)
    return msg_json


def decode(data: str | bytes, codec: str | None = None) -> dict:
    """Parse an inbound frame to a message dict.

    Pass the connection's ``codec`` to select the decoder (required for custom codecs). With no
    ``codec`` the built-ins are inferred from the frame type (str=JSON, bytes=msgpack).
    """
    if codec is not None:
        c = normalize_codec(codec)
        if c in _CODECS:
            return _CODECS[c][1](data)
        if c == JSON:
            raw = data.encode() if isinstance(data, str) else bytes(data)
            return json.loads(_decode_message(raw, c))
        if c == CBOR:
            return json.loads(_decode_message(bytes(data), c))
    if isinstance(data, (bytes, bytearray)):
        return json.loads(_decode_message(bytes(data), MSGPACK))
    return json.loads(_normalize_message(data))


def encode_as(value_json: str, content_type: str) -> bytes:
    """Encode a model ``Value`` (JSON string) to bytes with the named codec (built-in or registered)."""
    if content_type in _CODECS:
        out = _CODECS[content_type][0](json.loads(value_json))
        return out.encode() if isinstance(out, str) else out
    return _core_encode_as(value_json, content_type)


def decode_as(data: str | bytes, content_type: str) -> str:
    """Decode bytes back to a model ``Value`` (JSON string) with the named codec (built-in or registered)."""
    if content_type in _CODECS:
        return json.dumps(_CODECS[content_type][1](data))
    raw = bytes(data) if isinstance(data, (bytes, bytearray)) else data.encode()
    return _core_decode_as(raw, content_type)


def parse(text: str) -> dict:
    return json.loads(text)
