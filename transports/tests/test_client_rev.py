from pydantic import BaseModel

import transports


class M(BaseModel):
    xs: list = []


def test_client_ignores_patch_at_or_below_mirror_rev():
    """A client whose snapshot already reflects a change must not re-apply the broadcast of that change.

    Reproduces the late-join bug: a connection opening after `xs` changed gets a snapshot at the new
    rev, then the server also broadcasts that change's patch; without rev-idempotency the mirror would
    end at [1, 2, 2].
    """
    m = M(xs=[1])
    sess = transports.Session()
    mid = sess.host(m)

    m.xs = m.xs + [2]
    (_, patch_a) = sess.flush()[0]  # the append-2 patch (broadcast to existing connections)
    snap = sess.snapshot(mid)  # a newly opened connection's snapshot already includes [1, 2] at this rev

    c = transports.Client()
    c.recv(transports.protocol.snapshot_msg(mid, snap["type_name"], snap["rev"], snap["value"]))
    c.recv(transports.protocol.patch_msg(mid, patch_a))  # rev already reflected -> ignored
    assert transports.from_value(c.value(mid), M).xs == [1, 2]  # not [1, 2, 2]

    m.xs = m.xs + [3]
    (_, patch_b) = sess.flush()[0]  # a genuinely newer patch
    c.recv(transports.protocol.patch_msg(mid, patch_b))
    assert transports.from_value(c.value(mid), M).xs == [1, 2, 3]  # newer rev still applies


def test_client_recv_returns_the_accepted_change():
    """`recv` returns the accepted change (parity with the JS client): snapshot metadata for a
    snapshot, the decoded patch message for a patch, and None for a stale revision, a reject, or an
    unknown message type — the last ignored so a newer server can add frame types."""
    m = M(xs=[1])
    sess = transports.Session()
    mid = sess.host(m)
    snap = sess.snapshot(mid)

    c = transports.Client()
    changes = []
    unsubscribe = c.on_change(changes.append)
    accepted = c.recv(transports.protocol.snapshot_msg(mid, snap["type_name"], snap["rev"], snap["value"]))
    assert accepted == {"t": "snapshot", "id": mid, "rev": snap["rev"]}

    m.xs = m.xs + [2]
    (_, patch) = sess.flush()[0]
    accepted = c.recv(transports.protocol.patch_msg(mid, patch))
    assert accepted == {"t": "patch", "id": mid, "patch": patch}
    assert c.recv(transports.protocol.patch_msg(mid, patch)) is None  # stale rev: ignored
    assert c.recv('{"t": "presence", "id": 1}') is None  # unknown type: ignored (forward compat)
    assert changes == [{"t": "snapshot", "id": mid, "rev": snap["rev"]}, {"t": "patch", "id": mid, "patch": patch}]

    unsubscribe()
    m.xs = m.xs + [3]
    (_, newer) = sess.flush()[0]
    c.recv(transports.protocol.patch_msg(mid, newer))
    assert len(changes) == 2  # unsubscribed


def test_client_raises_on_patch_before_snapshot():
    c = transports.Client()
    try:
        c.recv(transports.protocol.patch_msg(7, {"rev": 1, "ops": []}))
        raise AssertionError("expected ValueError")
    except ValueError as e:
        assert "before snapshot" in str(e)
