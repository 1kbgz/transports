import json
from pathlib import Path

import pytest

from transports import CrdtSpec

FIXTURE = json.loads((Path(__file__).parents[2] / "rust" / "tests" / "fixtures" / "crdt_spec.json").read_text())


def test_python_binding_matches_shared_crdt_spec_fixture():
    spec = CrdtSpec.from_dict(FIXTURE["spec"])

    assert spec.to_json() == FIXTURE["canonical"]
    assert spec.hash == FIXTURE["hash"]
    assert spec.to_dict() == json.loads(FIXTURE["canonical"])
    spec.require_hash(FIXTURE["hash"])


def test_python_crdt_spec_constructors_are_equivalent_and_hashable():
    root = {"kind": "map", "fields": {"document": {"kind": "sequence", "materialization": "string"}}}
    constructed = CrdtSpec(root)
    from_json = CrdtSpec.from_json(constructed.to_json())
    from_dict = CrdtSpec.from_dict(constructed.to_dict())

    assert constructed == from_json == from_dict
    assert len({constructed, from_json, from_dict}) == 1
    assert CrdtSpec.from_dict(constructed.to_dict()).to_dict() == constructed.to_dict()

    element = {"kind": "map", "fields": {"meta": {"kind": "map"}}}
    first = CrdtSpec({"kind": "set", "keys": [["tenant"], ["meta", "id"]], "element": element})
    reversed_keys = CrdtSpec({"kind": "set", "keys": [["meta", "id"], ["tenant"]], "element": element})
    assert first == reversed_keys
    assert first.hash == reversed_keys.hash


def test_crdt_spec_rejects_incompatible_peer():
    spec = CrdtSpec({"kind": "register"})

    with pytest.raises(ValueError, match="incompatible CRDT spec"):
        spec.require_hash("sha256:other")


def test_crdt_spec_rejects_invalid_policy():
    with pytest.raises(ValueError, match="unsupported CRDT spec version"):
        CrdtSpec.from_dict({"version": 2, "root": {"kind": "register"}})

    with pytest.raises(ValueError, match="unknown field"):
        CrdtSpec.from_dict({"root": {"kind": "register", "fields": {}}})

    with pytest.raises(ValueError):
        CrdtSpec.from_dict({"root": {"kind": "register"}, "extra": float("nan")})
