import asyncio

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


def test_client_acknowledges_patches_and_ack_frames_without_fast_forwarding():
    c = transports.Client()
    c.recv(transports.protocol.snapshot_msg(1, "M", 3, {"Map": {"xs": {"List": []}}}))
    acknowledgements = []
    off = c.on_ack(acknowledgements.append)

    assert c.recv(transports.protocol.patch_msg(1, {"rev": 3, "ops": []}, "edit-2")) is None
    assert acknowledgements == [{"t": "patch", "id": 1, "patch": {"rev": 3, "ops": []}, "proposal": "edit-2"}]

    assert c.recv(transports.protocol.ack_msg(1, 99, "edit-3")) is None
    accepted = c.recv(transports.protocol.patch_msg(1, {"rev": 4, "ops": []}))
    assert accepted == {"t": "patch", "id": 1, "patch": {"rev": 4, "ops": []}}
    assert acknowledgements[-1] == {"t": "ack", "id": 1, "rev": 99, "proposal": "edit-3"}

    off()
    c.recv(transports.protocol.ack_msg(1, 4, "edit-4"))
    assert len(acknowledgements) == 2


def test_client_generated_proposal_ids_do_not_collide_with_caller_ids():
    c = transports.Client()

    explicit = transports.protocol.decode(c.edit_ops(1, [], "1"), "json")
    generated = transports.protocol.decode(c.edit_ops(1, []), "json")

    assert explicit["proposal"] == "1"
    assert generated["proposal"] == "auto-1"
    try:
        c.edit_ops(1, [], "auto-2")
        raise AssertionError("expected ValueError")
    except ValueError as error:
        assert "reserved" in str(error)


def test_client_tracks_settles_and_abandons_proposals():
    c = transports.Client()
    c.recv(transports.protocol.snapshot_msg(1, "M", 3, {"Map": {"xs": {"List": []}}}))
    abandoned = []
    c.on_abandon(abandoned.append)

    c.edit_ops(1, [], "editor-1")
    c.edit_ops(1, [])
    assert c.pending_proposals() == ["auto-1", "editor-1"]

    c.recv(transports.protocol.ack_msg(1, 3, "editor-1"))
    assert c.pending_proposals() == ["auto-1"]
    c._disconnected()
    assert abandoned == [["auto-1"]]
    assert c.pending_proposals() == []


def test_dropped_managed_proposal_is_not_left_pending():
    c = transports.Client()
    assert asyncio.run(c.propose_ops(1, [], "dropped")) is False
    assert c.pending_proposals() == []
