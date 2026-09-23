"""Schema-directed merge policies backed by the shared Rust core."""

import json
from collections.abc import Mapping
from typing import Literal, TypedDict, cast

from .transports import (
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
