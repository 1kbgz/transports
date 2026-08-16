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
    (so its optimistic UI reverts to the last good value), followed by a typed `reject` frame saying
    why, and broadcast nothing to the others."""
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
    reject = transports.protocol.decode(out[proposer][1], transports.protocol.JSON)
    assert reject["t"] == "reject"  # …and a typed reject saying why
    assert reject["id"] == mid
    assert reject["rev"] == revert["rev"]  # the server's current authoritative rev
    assert "int" in reject["error"]  # pydantic's validation message travels


def test_reject_frame_reaches_the_client_on_reject_callback():
    """The proposing client surfaces *why* an edit died: the reject frame fires `on_reject` with the
    validation error, the mirror reverts via the snapshot sent alongside, and `recv` stays quiet for
    the other message kinds it doesn't know."""
    sess = transports.Session()
    mid = sess.host(Device(brightness=60))
    server = transports.Server(sess)
    conn = object()

    client = transports.Client()
    rejections, changes = [], []
    client.on_reject(rejections.append)
    client.on_change(changes.append)
    for wire in server.open(conn):
        client.recv(wire)

    bad = client.edit(mid, {"Map": {"brightness": {"Str": ""}}})
    for wire in server.recv(conn, bad).get(conn, []):
        client.recv(wire)

    assert len(rejections) == 1
    assert rejections[0]["id"] == mid
    assert "int" in rejections[0]["error"]
    assert client.model(mid, Device).brightness == 60  # mirror reverted to the good value
    assert [c["t"] for c in changes] == ["snapshot", "snapshot"]  # open + revert; the reject is not a change


def test_a_coercible_edit_is_canonicalized_to_the_models_type():
    """A number control sends its value as a string; the stored value — and the broadcast patch — must be
    the model's canonical type, so the core value never diverges from the validated model."""
    d = Device()  # brightness: int = 60
    sess = transports.Session()
    mid = sess.host(d)

    # the wire carries brightness as the string "80" (what a number input's .value is)
    edit = {"rev": 0, "ops": [{"Set": {"path": [{"Key": "brightness"}], "value": {"Str": "80"}}}]}
    auth = sess.submit(mid, edit)

    assert auth is not None
    assert d.brightness == 80  # the model coerced it to an int
    assert sess.value(mid)["Map"]["brightness"] == {"Int": 80}  # the stored value is the canonical int, not "80"
    # the authoritative patch broadcast to every client carries the canonical int as well
    assert auth["ops"] == [{"Set": {"path": [{"Key": "brightness"}], "value": {"Int": 80}}}]
