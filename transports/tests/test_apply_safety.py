from pydantic import BaseModel

import transports


class M(BaseModel):
    x: int = 0


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
