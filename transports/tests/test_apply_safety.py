import json

from pydantic import BaseModel

import transports


class M(BaseModel):
    x: int = 0


class Device(BaseModel):
    brightness: int = 60


def test_submit_rejects_malformed_patch_without_crashing():
    """A malformed client proposal (path descends into the wrong type) is rejected, not applied — and
    it must not panic/abort the host (the core now returns a recoverable error)."""
    m = M()
    sess = transports.Session()
    mid = sess.host(m)

    # descend into x (an Int) as if it were a map -> previously aborted the process at the Rust boundary
    bad = {"rev": 0, "ops": [{"Set": {"path": [{"Key": "x"}, {"Key": "y"}], "value": {"Int": 9}}}]}
    assert sess.submit(mid, bad) is None  # dropped

    # out-of-bounds index is also rejected, not a crash
    bad_index = {"rev": 0, "ops": [{"RemoveAt": {"path": [], "index": 5}}]}
    assert sess.submit(mid, bad_index) is None

    # the session is still usable, and a valid proposal still goes through
    good = {"rev": 0, "ops": [{"Set": {"path": [{"Key": "x"}], "value": {"Int": 7}}}]}
    auth = sess.submit(mid, good)
    assert auth is not None
    assert m.x == 7


def test_submit_rejects_an_edit_the_model_cannot_validate():
    """The reported crash: a non-numeric string edited into an int field raised pydantic ValidationError
    from the model refresh and killed the connection. It must now be rejected, the model + core value
    left untouched, and the host kept alive."""
    d = Device()
    sess = transports.Session()
    mid = sess.host(d)
    before = sess.value(mid)

    bad = {"rev": 0, "ops": [{"Set": {"path": [{"Key": "brightness"}], "value": {"Str": ""}}}]}
    assert sess.submit(mid, bad) is None  # rejected, not applied (no exception)
    assert d.brightness == 60  # the hosted model is untouched
    assert sess.value(mid) == before  # the core value is untouched — no partial commit

    # the session still works: a valid edit goes through
    good = {"rev": 0, "ops": [{"Set": {"path": [{"Key": "brightness"}], "value": {"Int": 80}}}]}
    assert sess.submit(mid, good) is not None
    assert d.brightness == 80


def test_server_reverts_only_the_proposer_on_a_rejected_edit():
    """A rejected edit makes the server re-send the authoritative snapshot to the *proposing* connection
    (so its optimistic UI reverts to the last good value) and broadcast nothing to the others."""
    sess = transports.Session()
    mid = sess.host(Device(brightness=60))
    server = transports.Server(sess)
    proposer, other = object(), object()
    server.open(proposer)  # registers each connection's codec (default JSON)
    server.open(other)

    edit = {"t": "patch", "id": mid, "patch": {"rev": 0, "ops": [{"Set": {"path": [{"Key": "brightness"}], "value": {"Str": ""}}}]}}
    out = server.recv(proposer, transports.protocol.encode(json.dumps(edit), transports.protocol.JSON))

    assert set(out) == {proposer}  # only the proposer is messaged — no broadcast of the bad edit
    revert = transports.protocol.decode(out[proposer][0], transports.protocol.JSON)
    assert revert["t"] == "snapshot"  # a fresh authoritative snapshot…
    assert transports.from_value(revert["value"], Device).brightness == 60  # …restoring the good value
