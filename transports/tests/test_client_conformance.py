import copy
import json
from pathlib import Path

import transports

FIXTURE = json.loads((Path(__file__).parents[2] / "rust" / "tests" / "fixtures" / "live_protocol.json").read_text())


def _effect(state, message):
    return json.loads(state.prepare(json.dumps(message)))


def _commit(state, effect):
    state.commit(json.dumps(effect))


def test_python_binding_matches_shared_client_state_trace():
    state = transports.ClientState()
    effects = FIXTURE["effects"]

    effect = _effect(state, FIXTURE["snapshot"])
    assert effect == effects["snapshot"]
    _commit(state, effect)
    state.proposal(7, "[]", "editor-1")
    state.proposal(7, "[]")

    effect = _effect(state, FIXTURE["patch"])
    assert effect == effects["patch"]
    _commit(state, effect)
    assert state.pending() == ["auto-1"]

    stale = copy.deepcopy(FIXTURE["patch"])
    stale["proposal"] = "auto-1"
    effect = _effect(state, stale)
    assert effect == effects["stale_patch"]
    _commit(state, effect)

    for proposal, message, expected in (
        ("editor-2", FIXTURE["ack"], effects["ack"]),
        ("editor-3", FIXTURE["reject"], effects["reject"]),
    ):
        state.proposal(7, "[]", proposal)
        effect = _effect(state, message)
        assert effect == expected
        _commit(state, effect)

    effect = _effect(state, FIXTURE["unknown"])
    assert effect == effects["unknown"]
    _commit(state, effect)
    state.proposal(7, "[]", "editor-4")
    assert json.loads(state.disconnect()) == effects["disconnect"]
    assert json.loads(state.revisions()) == {"7": 3}
    assert state.pending() == []


def test_python_binding_matches_shared_message_codec_trace():
    for name in ("snapshot", "patch", "ack", "reject", "batch", "unknown"):
        message = json.dumps(FIXTURE[name], separators=(",", ":"))
        for codec in ("json", "msgpack", "cbor"):
            assert json.loads(transports.decode_message(transports.encode_message(message, codec), codec)) == FIXTURE[name]
        assert transports.encode_message(message, "msgpack") == transports.json_to_msgpack(message)
        assert transports.encode_message(message, "cbor") == transports.json_to_cbor(message)


def test_python_client_applies_shared_batch_fixture_in_order():
    client = transports.Client()
    accepted = client.recv(json.dumps(FIXTURE["batch"]))
    assert [change["t"] for change in accepted] == ["snapshot", "patch"]
    assert client.value(8) == {"Map": {"count": {"Int": 6}}}
