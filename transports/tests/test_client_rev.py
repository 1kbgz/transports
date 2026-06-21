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
