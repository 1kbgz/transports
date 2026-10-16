import json
from pathlib import Path

import pytest
from pydantic import BaseModel

from transports import Hub, Session, Store, apply, diff, to_value

RULES = [{"path": [{"Key": "rows"}], "key": [{"Key": "id"}]}]
CASES = json.loads((Path(__file__).parents[2] / "rust" / "tests" / "fixtures" / "keyed_lists.json").read_text())


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_keyed_fixtures(case):
    old, new = (json.dumps(to_value(case[key])) for key in ("old", "new"))
    patch = diff(old, new, list_keys=json.dumps(RULES))
    assert json.loads(apply(old, patch)) == json.loads(new)
    if case.get("fallback"):
        assert patch == diff(old, new)
    else:
        assert [next(iter(op)) for op in json.loads(patch)["ops"]] == case["kinds"]
    store = Store()
    mid = store.host("Rows", old, list_keys=json.dumps(RULES))
    mutation = store.mutate(mid, new)
    assert mutation is not None
    assert json.loads(mutation)["ops"] == json.loads(patch)["ops"]


class Rows(BaseModel):
    rows: list[dict]


def test_session_host_and_submit_keep_policy():
    case = CASES[0]
    session = Session()
    model = Rows(**case["old"])
    mid = session.host(model, list_keys=RULES)
    model.rows = case["new"]["rows"]
    patch = session.drain()[0][1]
    assert [next(iter(op)) for op in patch["ops"]] == case["kinds"]
    proposal = json.loads(diff(json.dumps(to_value(case["new"])), json.dumps(to_value(case["old"]))))
    accepted = session.submit(mid, proposal)
    expected = json.loads(diff(json.dumps(to_value(case["new"])), json.dumps(to_value(case["old"])), list_keys=json.dumps(RULES)))
    assert accepted is not None
    assert accepted["ops"] == expected["ops"]
    replay = session.since(mid, 0)
    assert replay is not None and len(replay) == 2


def test_hub_shared_policy_and_replay():
    hub = Hub(key=lambda conn: conn)
    sid = hub.share(Rows(**CASES[0]["old"]), list_keys=RULES, replay=True)
    hub.set_shared(sid, to_value(CASES[0]["new"]))
    replay = hub.since_shared(sid, 0)
    assert replay is not None
    patch = replay[0]
    assert [next(iter(op)) for op in patch["ops"]] == CASES[0]["kinds"]


@pytest.mark.parametrize("rules", [[RULES[0], RULES[0]], [{"path": [], "key": [], "typo": True}]])
def test_invalid_policy_is_rejected(rules):
    with pytest.raises(ValueError):
        diff('"Null"', '"Null"', list_keys=json.dumps(rules))
    with pytest.raises(ValueError):
        Store().host("Rows", '"Null"', list_keys=json.dumps(rules))


def test_nested_and_root_identity_paths():
    old = {"groups": [{"records": [{"meta": {"id": 1}, "label": "A"}, {"meta": {"id": 2}, "label": "B"}]}]}
    new = {"groups": [{"records": [{"meta": {"id": 2}, "label": "changed"}, {"meta": {"id": 1}, "label": "A"}]}]}
    rules = [{"path": [{"Key": "groups"}, {"Index": 0}, {"Key": "records"}], "key": [{"Key": "meta"}, {"Key": "id"}]}]
    patch = diff(json.dumps(to_value(old)), json.dumps(to_value(new)), list_keys=json.dumps(rules))
    assert [next(iter(op)) for op in json.loads(patch)["ops"]] == ["Move", "Set"]
    assert json.loads(apply(json.dumps(to_value(old)), patch)) == to_value(new)
    root = diff(json.dumps(to_value([1, 2, 3])), json.dumps(to_value([3, 1, 2])), list_keys='[{"path":[],"key":[]}]')
    assert [next(iter(op)) for op in json.loads(root)["ops"]] == ["Move"]


def test_keyed_policy_does_not_change_crdt_identity():
    with pytest.raises(ValueError, match="list_keys cannot"):
        Hub(key=lambda conn: conn).share(Rows(rows=[]), crdt_spec={"root": {"kind": "register"}}, list_keys=RULES)
