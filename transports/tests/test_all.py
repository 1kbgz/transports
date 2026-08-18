import json

from transports import Store, apply, decode, diff, encode

# A model is a `Value`; on the wire that is the externally-tagged enum, e.g. a map of fields is
# {"Map": {"on": {"Bool": false}}}. The pydantic/msgspec bridge will hide this; the tests
# speak the raw wire form directly.


def _device(on: bool) -> str:
    return json.dumps({"Map": {"on": {"Bool": on}, "name": {"Str": "lamp"}}})


def test_exports():
    assert callable(diff) and callable(apply) and callable(encode) and callable(decode)


def test_diff_apply_round_trip():
    old, new = _device(False), _device(True)
    patch = diff(old, new)
    assert json.loads(apply(old, patch)) == json.loads(new)


def test_sequence_reorder_uses_move_and_round_trips():
    old = json.dumps({"List": [{"Str": "a"}, {"Str": "b"}, {"Str": "c"}, {"Str": "d"}]})
    new = json.dumps({"List": [{"Str": "d"}, {"Str": "b"}, {"Str": "a"}, {"Str": "c"}]})
    patch = json.loads(diff(old, new))

    assert patch["ops"] == [
        {"Move": {"path": [], "from": 3, "to": 0}},
        {"Move": {"path": [], "from": 2, "to": 1}},
    ]
    assert json.loads(apply(old, json.dumps(patch))) == json.loads(new)


def test_sequence_move_uses_final_destination_index():
    old = json.dumps({"List": [{"Int": 1}, {"Int": 2}, {"Int": 3}]})
    patch = json.dumps({"rev": 1, "ops": [{"Move": {"path": [], "from": 0, "to": 2}}]})

    assert json.loads(apply(old, patch)) == {"List": [{"Int": 2}, {"Int": 3}, {"Int": 1}]}


def test_sequence_move_rejects_an_out_of_bounds_destination():
    import pytest

    old = json.dumps({"List": [{"Int": 1}, {"Int": 2}]})
    patch = json.dumps({"rev": 1, "ops": [{"Move": {"path": [], "from": 0, "to": 2}}]})

    with pytest.raises(ValueError, match="move destination index 2 out of bounds"):
        apply(old, patch)


def test_dense_sequence_reorder_uses_one_permutation():
    old_values = list(range(32))
    new_values = list(reversed(old_values))
    old = json.dumps({"List": [{"Int": value} for value in old_values]})
    new = json.dumps({"List": [{"Int": value} for value in new_values]})
    patch = json.loads(diff(old, new))

    assert patch["ops"] == [{"Reorder": {"path": [], "order": new_values}}]
    assert json.loads(apply(old, json.dumps(patch))) == json.loads(new)


def test_sequence_reorder_rejects_an_invalid_permutation():
    import pytest

    old = json.dumps({"List": [{"Int": 1}, {"Int": 2}]})
    patch = json.dumps({"rev": 1, "ops": [{"Reorder": {"path": [], "order": [0, 0]}}]})

    with pytest.raises(ValueError, match="reorder index 0 is duplicated"):
        apply(old, patch)


def test_encode_decode():
    model = _device(True)
    blob = encode(model)
    assert isinstance(blob, bytes)
    assert json.loads(decode(blob)) == json.loads(model)


def test_store_mutate_is_incremental():
    s = Store()
    sid = s.host("Device", _device(False))
    snap_json = s.snapshot(sid)
    assert snap_json is not None
    snap = json.loads(snap_json)
    assert snap["rev"] == 0 and snap["type_name"] == "Device"
    patch_json = s.mutate(sid, _device(True))
    assert patch_json is not None
    patch = json.loads(patch_json)
    assert len(patch["ops"]) == 1  # only `on` changed — not a whole-model resend
    assert patch["rev"] == 1


def test_store_mirror_across_two_stores():
    server = Store()
    sid = server.host("Device", _device(False))
    snap_json = server.snapshot(sid)
    assert snap_json is not None
    snap = json.loads(snap_json)

    client = Store()
    cid = client.host(snap["type_name"], json.dumps(snap["value"]))

    patch = server.mutate(sid, _device(True))
    assert patch is not None
    assert client.apply(cid, patch) is True

    sv_json, cv_json = server.snapshot(sid), client.snapshot(cid)
    assert sv_json is not None and cv_json is not None
    server_value = json.loads(sv_json)["value"]
    client_value = json.loads(cv_json)["value"]
    assert client_value == server_value == json.loads(_device(True))


def test_malformed_raises():
    import pytest

    with pytest.raises(ValueError):
        diff("{bad", "{}")
