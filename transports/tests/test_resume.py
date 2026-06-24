import json

from pydantic import BaseModel

from transports import Client, Server, Session


class Doc(BaseModel):
    x: int = 0


def _bump(session, mid, value):
    """Mutate the hosted model and flush, advancing its rev by one."""
    session._models[mid].x = value
    session.update(mid)


def test_session_since_returns_delta_up_to_date_or_gap():
    s = Session()
    mid = s.host(Doc())
    for v in (1, 2, 3):
        _bump(s, mid, v)
    assert s.snapshot(mid)["rev"] == 3
    assert [p["rev"] for p in s.since(mid, 1)] == [2, 3]  # the patches after rev 1
    assert s.since(mid, 3) == []  # already current
    s._log[mid] = s._log[mid][-1:]  # evict all but rev 3 — rev 2 is gone
    assert s.since(mid, 1) is None  # gap: can't bridge from rev 1, caller must re-snapshot


def test_server_resume_replays_only_the_delta():
    s = Session()
    mid = s.host(Doc())
    server = Server(s)
    client = Client()
    for m in server.open("c1"):  # fresh connect → snapshot
        client.recv(m)
    assert client.model(mid, Doc).x == 0
    last = client._rev[mid]

    _bump(s, mid, 42)  # model advances while the client is away

    resume = server.open("c2", since={mid: last})  # reconnect with last-seen rev
    assert resume and all(json.loads(m)["t"] == "patch" for m in resume)  # delta, no snapshot
    for m in resume:
        client.recv(m)
    assert client.model(mid, Doc).x == 42


def test_server_resume_falls_back_to_snapshot_on_gap():
    s = Session()
    mid = s.host(Doc())
    server = Server(s)
    _bump(s, mid, 5)
    s._log[mid] = []  # log evicted → gap
    msgs = server.open("c", since={mid: 0})
    assert json.loads(msgs[0])["t"] == "snapshot"  # falls back to a fresh snapshot


def test_resume_when_already_current_sends_nothing():
    s = Session()
    mid = s.host(Doc())
    server = Server(s)
    _bump(s, mid, 7)
    cur = s.snapshot(mid)["rev"]
    assert server.open("c", since={mid: cur}) == []  # nothing to replay
