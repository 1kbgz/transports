"""Schema-directed merge policies backed by the shared Rust core."""

import json
from collections.abc import Iterable, Mapping
from typing import Any, Literal, TypedDict, cast

from ._bridge import _py_of, _value_of
from .transports import (
    CrdtDocument as _CrdtDocument,
    crdt_spec_hash as _crdt_spec_hash,
    normalize_crdt_spec as _normalize_crdt_spec,
    require_crdt_spec_hash as _require_crdt_spec_hash,
)


class RegisterPolicy(TypedDict):
    kind: Literal["register"]


class _MapOptions(TypedDict, total=False):
    fields: dict[str, "CrdtPolicy"]
    values: "CrdtPolicy"


class MapPolicy(_MapOptions):
    kind: Literal["map"]


class _SetOptions(TypedDict, total=False):
    keys: list[list[str]]
    element: "CrdtPolicy"


class SetPolicy(_SetOptions):
    kind: Literal["set"]


class _SequenceOptions(TypedDict, total=False):
    materialization: Literal["list", "string"]
    element: "CrdtPolicy"


class SequencePolicy(_SequenceOptions):
    kind: Literal["sequence"]


CrdtPolicy = RegisterPolicy | MapPolicy | SetPolicy | SequencePolicy
CrdtPathSegment = dict[str, Any]
CrdtMutation = dict[str, Any]
CrdtOp = dict[str, Any]


def _wire_item(item: Mapping[str, Any], *, encode: bool) -> dict[str, Any]:
    converted = dict(item)
    transform = _value_of if encode else _py_of
    if converted.get("kind") in {"register_set", "map_set", "set_add"}:
        if "value" not in converted:
            raise TypeError(f"{converted['kind']} requires value")
        converted["value"] = transform(converted["value"])
    elif converted.get("kind") == "sequence_insert":
        if "values" in converted:
            converted["values"] = [transform(value) for value in converted["values"]]
        elif "elements" in converted:
            converted["elements"] = [{**element, "value": transform(element["value"])} for element in converted["elements"]]
        else:
            raise TypeError("sequence_insert requires values or elements")
    return converted


def _public_effect(effect: Mapping[str, Any]) -> dict[str, Any]:
    converted = dict(effect)
    converted["deltas"] = [_wire_item(delta, encode=False) for delta in effect["deltas"]]
    return converted


class CrdtSpec:
    """Validated, canonical merge semantics for one model."""

    def __init__(self, root: CrdtPolicy, *, version: int | None = None) -> None:
        value: dict[str, object] = {"root": root}
        if version is not None:
            value["version"] = version
        self._json = _normalize_crdt_spec(json.dumps(value, allow_nan=False))

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "CrdtSpec":
        """Validate a complete serialized specification."""
        instance = cls.__new__(cls)
        instance._json = _normalize_crdt_spec(json.dumps(dict(value), allow_nan=False))
        return instance

    @classmethod
    def from_json(cls, value: str) -> "CrdtSpec":
        """Validate a serialized specification."""
        instance = cls.__new__(cls)
        instance._json = _normalize_crdt_spec(value)
        return instance

    def to_dict(self) -> dict[str, object]:
        """Return a detached JSON-compatible representation."""
        return cast(dict[str, object], json.loads(self._json))

    def to_json(self) -> str:
        """Return the deterministic serialized representation."""
        return self._json

    @property
    def hash(self) -> str:
        """SHA-256 of the deterministic serialized representation."""
        return _crdt_spec_hash(self._json)

    def require_hash(self, peer_hash: str) -> None:
        """Raise ``ValueError`` when a peer uses different merge semantics."""
        _require_crdt_spec_hash(self._json, peer_hash)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, CrdtSpec) and self._json == other._json

    def __hash__(self) -> int:
        return hash(self._json)

    def __repr__(self) -> str:
        return f"CrdtSpec.from_json({self._json!r})"


class CrdtDocument:
    """One schema-directed CRDT replica backed by the shared Rust reducer."""

    def __init__(self, spec: CrdtSpec, value: Any, replica: str) -> None:
        self.spec = spec
        self.replica = replica
        self._inner = _CrdtDocument(spec.to_json(), json.dumps(_value_of(value), allow_nan=False), replica)

    @classmethod
    def from_state(cls, spec: CrdtSpec, state: Mapping[str, Any], replica: str) -> "CrdtDocument":
        """Restore a transferred reducer state under a new local replica identifier."""
        instance = cls.__new__(cls)
        instance.spec = spec
        instance.replica = replica
        instance._inner = _CrdtDocument.from_state(spec.to_json(), json.dumps(dict(state), allow_nan=False), replica)
        return instance

    @property
    def value(self) -> Any:
        """Current ordinary Python value."""
        return _py_of(json.loads(self._inner.value()))

    @property
    def state(self) -> dict[str, Any]:
        """Detached, JSON-compatible reducer state for transfer or persistence."""
        return cast(dict[str, Any], json.loads(self._inner.state()))

    def mutate(self, mutations: Iterable[CrdtMutation]) -> dict[str, Any]:
        """Create and apply local operations, returning operations and materialized effects."""
        wire = [_wire_item(mutation, encode=True) for mutation in mutations]
        change = cast(dict[str, Any], json.loads(self._inner.mutate(json.dumps(wire, allow_nan=False))))
        change["ops"] = [_wire_item(op, encode=False) for op in change["ops"]]
        change["effect"] = _public_effect(change["effect"])
        return change

    def apply(self, ops: Iterable[CrdtOp]) -> dict[str, Any]:
        """Apply remote operations idempotently and return their materialized effect."""
        wire = [_wire_item(op, encode=True) for op in ops]
        effect = cast(dict[str, Any], json.loads(self._inner.apply(json.dumps(wire, allow_nan=False))))
        return _public_effect(effect)

    def member_key(self, path: list[CrdtPathSegment], value: Any) -> str:
        """Return canonical identity for a value at a set path."""
        return self._inner.member_key(json.dumps(path, allow_nan=False), json.dumps(_value_of(value), allow_nan=False))

    def compact(self, frontier: Mapping[str, int]) -> int:
        """Discard metadata covered by a caller-provided causally stable frontier."""
        return cast(int, self._inner.compact(json.dumps(dict(frontier), allow_nan=False)))
