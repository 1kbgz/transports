import json
from pathlib import Path

import pytest

from transports import CrdtDocument, CrdtSpec

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


def test_python_binding_matches_shared_crdt_reducer_fixture():
    fixture = json.loads((Path(__file__).parents[2] / "rust" / "tests" / "fixtures" / "crdt_reducer.json").read_text())
    spec = CrdtSpec.from_dict(fixture["spec"])
    document = CrdtDocument(spec, {"text": "", "title": "draft"}, "a")
    change = document.mutate(
        [
            {
                "kind": "register_set",
                "path": [{"kind": "key", "key": "title"}],
                "value": "ready",
            },
            {
                "kind": "sequence_insert",
                "path": [{"kind": "key", "key": "text"}],
                "after": None,
                "values": ["h", "i"],
            },
            {
                "kind": "sequence_splice",
                "path": [{"kind": "key", "key": "text"}],
                "index": 1,
                "delete_count": 1,
                "values": ["λ"],
            },
        ]
    )

    assert document.value == {"text": "hλ", "title": "ready"}
    assert change["effect"]["applied"] == 4
    assert [op["dot"] for op in change["ops"]] == [
        {"counter": 1, "replica": "a"},
        {"counter": 2, "replica": "a"},
        {"counter": 3, "replica": "a"},
        {"counter": 4, "replica": "a"},
    ]
    receiver = CrdtDocument.from_state(spec, document.state, "b")
    assert receiver.value == document.value
    duplicate = receiver.apply(change["ops"])
    assert duplicate["patch"]["ops"] == []
    assert duplicate["applied"] == 0


def test_python_crdt_binding_exposes_validation_identity_and_compaction():
    explicit = CrdtSpec({"kind": "register"}, version=1)
    assert repr(explicit) == 'CrdtSpec.from_json(\'{"version":1,"root":{"kind":"register"}}\')'
    with pytest.raises(ValueError, match="unsupported CRDT spec version 2"):
        CrdtSpec({"kind": "register"}, version=2)

    register = CrdtDocument(explicit, "draft", "register")
    with pytest.raises(TypeError, match="register_set requires value"):
        register.mutate([{"kind": "register_set", "path": []}])

    sequence = CrdtDocument(CrdtSpec({"kind": "sequence", "materialization": "string"}), "", "sequence")
    with pytest.raises(TypeError, match="sequence_insert requires values or elements"):
        sequence.mutate([{"kind": "sequence_insert", "path": [], "after": None}])

    keyed = CrdtDocument(
        CrdtSpec(
            {
                "kind": "set",
                "keys": [["id"]],
                "element": {"kind": "map", "fields": {"id": {"kind": "register"}}},
            }
        ),
        [{"id": "row-1"}],
        "set",
    )
    assert keyed.member_key([], {"id": "row-1"}) in keyed.state["root"]["entries"]

    change = register.mutate([{"kind": "register_set", "path": [], "value": "ready"}])
    assert change["ops"][0]["dot"] == {"counter": 1, "replica": "register"}

    compacted = CrdtDocument(
        CrdtSpec({"kind": "map", "values": {"kind": "register"}}),
        {"old": 1},
        "map",
    )
    compacted.mutate([{"kind": "map_remove", "path": [], "key": "old"}])
    assert compacted.compact({"map": 1}) == 1
