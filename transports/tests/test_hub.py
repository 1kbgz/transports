from pydantic import BaseModel

from transports import READ, WRITE, Client, Hub, LastWriteWins, LwwMapCrdt


class Doc(BaseModel):
    x: int = 0
    y: int = 0


# Connection handles are (tenant, name); the hub keys tenants off the first element.
def hub() -> Hub:
    return Hub(key=lambda c: c[0])


# --- tenant routing + isolation ------------------------------------------------------------------


def test_private_models_are_tenant_isolated():
    h = hub()
    h.tenant("t1").host(Doc(x=1))
    h.tenant("t2").host(Doc(x=2))

    a = h.open(("t1", "a"))
    b = h.open(("t2", "b"))
    ca, cb = Client(), Client()
    for m in a:
        ca.recv(m)
    for m in b:
        cb.recv(m)

    # each connection sees only its own tenant's private model (both numbered id 1, never shared)
    assert ca.model(1, Doc).x == 1
    assert cb.model(1, Doc).x == 2


def test_private_edit_relays_only_within_tenant():
    h = hub()
    h.tenant("t1").host(Doc())
    a1, a2, b = ("t1", "a1"), ("t1", "a2"), ("t2", "b")
    h.open(a1)
    h.open(b)
    ca2 = Client()
    for m in h.open(a2):
        ca2.recv(m)

    edit = '{"t":"patch","id":1,"patch":{"rev":1,"ops":[{"Set":{"path":[{"Key":"x"}],"value":{"Int":7}}}]}}'
    out = h.recv(a1, edit)
    assert set(out) == {a2}  # relayed to the other connection in t1, not to t2
    for m in out[a2]:
        ca2.recv(m)
    assert ca2.value(1)["Map"]["x"] == {"Int": 7}


# --- 1-N: one shared model, many READ subscribers ------------------------------------------------


def test_shared_read_fanout_to_many_tenants():
    h = hub()
    sid = h.share(Doc())
    h.subscribe("t1", sid, READ)
    h.subscribe("t2", sid, READ)
    c1, c2 = ("t1", "a"), ("t2", "b")
    cl1, cl2 = Client(), Client()
    for m in h.open(c1):
        cl1.recv(m)
    for m in h.open(c2):
        cl2.recv(m)

    h.set_shared(sid, {"Map": {"x": {"Int": 3}, "y": {"Int": 0}}})  # host write
    out = h.flush()
    assert set(out) == {c1, c2}  # broadcast to every subscriber
    for m in out[c1]:
        cl1.recv(m)
    for m in out[c2]:
        cl2.recv(m)
    assert cl1.value(sid)["Map"]["x"] == {"Int": 3}
    assert cl2.value(sid)["Map"]["x"] == {"Int": 3}


def test_read_only_subscriber_cannot_write():
    h = hub()
    sid = h.share(Doc())
    h.subscribe("t1", sid, READ)
    c = ("t1", "a")
    h.open(c)
    edit = '{"t":"patch","id":%d,"patch":{"rev":1,"ops":[{"Set":{"path":[{"Key":"x"}],"value":{"Int":9}}}]}}' % sid
    assert h.recv(c, edit) == {}  # write ignored
    assert h._shared[sid].value["Map"]["x"] == {"Int": 0}  # authoritative value untouched


# --- N-1 / N-N: many WRITE subscribers -----------------------------------------------------------


def test_shared_write_relays_to_other_writers():
    h = hub()
    sid = h.share(Doc())
    h.subscribe("t1", sid, WRITE)
    h.subscribe("t2", sid, WRITE)
    c1, c2 = ("t1", "a"), ("t2", "b")
    cl1, cl2 = Client(), Client()
    for m in h.open(c1):
        cl1.recv(m)
    for m in h.open(c2):
        cl2.recv(m)

    edit = cl1.edit(sid, {"Map": {"x": {"Int": 5}, "y": {"Int": 0}}})
    out = h.recv(c1, edit)
    assert set(out) == {c1, c2}  # shared models are server-authoritative: every subscriber, incl. origin
    for m in out[c2]:
        cl2.recv(m)
    assert cl2.value(sid)["Map"]["x"] == {"Int": 5}


def test_n_by_n_routes_each_connection_its_subscribed_set():
    h = hub()
    a = h.share(Doc(), merge=LastWriteWins)
    b = h.share(Doc(), merge=LastWriteWins)
    h.subscribe("t1", a, READ)
    h.subscribe("t1", b, READ)
    h.subscribe("t2", b, READ)  # t2 only sees b
    c1, c2 = ("t1", "a"), ("t2", "b")
    h.open(c1)
    h.open(c2)

    h.set_shared(a, {"Map": {"x": {"Int": 1}, "y": {"Int": 0}}})
    h.set_shared(b, {"Map": {"x": {"Int": 2}, "y": {"Int": 0}}})
    out = h.flush()
    assert len(out[c1]) == 2  # both a and b
    assert len(out[c2]) == 1  # only b


# --- merge strategies ----------------------------------------------------------------------------


def test_crdt_converges_regardless_of_order():
    base = {"Map": {"x": {"Int": 0}}}
    # two writes to the same key from the same base revision, different origins
    pa = {"rev": 1, "ops": [{"Set": {"path": [{"Key": "x"}], "value": {"Int": 1}}}]}
    pb = {"rev": 1, "ops": [{"Set": {"path": [{"Key": "x"}], "value": {"Int": 2}}}]}

    c1 = LwwMapCrdt()
    v1 = c1.merge(c1.merge(base, pa, "A"), pb, "B")
    c2 = LwwMapCrdt()
    v2 = c2.merge(c2.merge(base, pb, "B"), pa, "A")
    assert v1 == v2  # conflict-free: same converged value either way

    # last-write-wins is order-dependent on the same conflict
    lww = LastWriteWins()
    l1 = lww.merge(lww.merge(base, pa, "A"), pb, "B")
    l2 = lww.merge(lww.merge(base, pb, "B"), pa, "A")
    assert l1 != l2


def test_crdt_preserves_concurrent_edits_to_different_keys():
    base = {"Map": {"x": {"Int": 0}, "y": {"Int": 0}}}
    px = {"rev": 1, "ops": [{"Set": {"path": [{"Key": "x"}], "value": {"Int": 1}}}]}
    py = {"rev": 1, "ops": [{"Set": {"path": [{"Key": "y"}], "value": {"Int": 1}}}]}
    crdt = LwwMapCrdt()
    merged = crdt.merge(crdt.merge(base, px, "A"), py, "B")
    assert merged["Map"]["x"] == {"Int": 1}
    assert merged["Map"]["y"] == {"Int": 1}  # neither write clobbered the other


# --- real Starlette WebSockets -------------------------------------------------------------------


def test_starlette_two_tenants_share_over_mixed_codecs():
    from starlette.applications import Starlette
    from starlette.routing import WebSocketRoute
    from starlette.testclient import TestClient

    h = Hub(key=lambda ws: ws.path_params["tenant"])
    sid = h.share(Doc())
    h.subscribe("alice", sid, WRITE)
    h.subscribe("bob", sid, WRITE)
    app = Starlette(routes=[WebSocketRoute("/ws/{tenant}", h.endpoint())])

    with TestClient(app) as tc, tc.websocket_connect("/ws/alice?codec=json") as wa, tc.websocket_connect("/ws/bob?codec=msgpack") as wb:
        ca = Client()
        cb = Client(codec="msgpack")
        ca.recv(wa.receive_text())  # alice: JSON snapshot
        cb.recv(wb.receive_bytes())  # bob: msgpack snapshot

        # alice writes the shared model; bob (a different tenant, different codec) receives it
        edit = ca.edit(sid, {"Map": {"x": {"Int": 42}, "y": {"Int": 0}}})
        assert isinstance(edit, str)
        wa.send_text(edit)
        cb.recv(wb.receive_bytes())
        assert cb.value(sid)["Map"]["x"] == {"Int": 42}
