import itertools

from pydantic import BaseModel

from transports import READ, WRITE, Client, DeepLwwCrdt, Hub, LastWriteWins, LwwMapCrdt, ws_endpoint


class Doc(BaseModel):
    x: int = 0
    y: int = 0


# Connection handles are (tenant, name); the hub keys tenants off the first element.
def hub() -> Hub:
    return Hub(key=lambda c: c[0])


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
    assert set(out) == {a1, a2}  # server-authoritative: echoed to all of t1 (incl origin), not t2
    for m in out[a2]:
        ca2.recv(m)
    assert ca2.value(1)["Map"]["x"] == {"Int": 7}


def test_private_invalid_edit_reverts_only_the_proposer():
    """An invalid edit to a private model is rejected; the hub re-sends the authoritative snapshot to the
    proposing connection alone (so its UI reverts) and relays nothing to the tenant's other connections."""
    h = hub()
    h.tenant("t1").host(Doc(x=5))
    a1, a2 = ("t1", "a1"), ("t1", "a2")
    h.open(a1)
    h.open(a2)

    bad = '{"t":"patch","id":1,"patch":{"rev":1,"ops":[{"Set":{"path":[{"Key":"x"}],"value":{"Str":""}}}]}}'
    out = h.recv(a1, bad)
    assert set(out) == {a1}  # only the proposer is messaged — no relay of the bad edit to a2
    c = Client()
    for m in out[a1]:
        c.recv(m)
    assert c.value(1)["Map"]["x"] == {"Int": 5}  # reverted to the authoritative value


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


_NESTED = {"Map": {"a": {"Map": {"b": {"Int": 0}, "c": {"Int": 0}}}, "z": {"Int": 0}}}


def _set(path_keys, value, rev):
    return {"rev": rev, "ops": [{"Set": {"path": [{"Key": k} for k in path_keys], "value": value}}]}


def test_deep_lww_preserves_concurrent_nested_edits():
    # concurrent writes to a.b and a.c (different nested fields) must both survive
    crdt = DeepLwwCrdt()
    merged = crdt.merge(crdt.merge(_NESTED, _set(["a", "b"], {"Int": 1}, 1), "A"), _set(["a", "c"], {"Int": 2}, 1), "B")
    assert merged["Map"]["a"]["Map"]["b"] == {"Int": 1}
    assert merged["Map"]["a"]["Map"]["c"] == {"Int": 2}  # neither clobbered the other


def test_deep_lww_is_finer_grained_than_lww_map():
    # contrast: LwwMapCrdt stamps both nested writes under the top key "a", so the lower-stamped one is
    # dropped; DeepLwwCrdt keeps each field's own register.
    hi = _set(["a", "b"], {"Int": 1}, 2)  # higher stamp, field a.b
    lo = _set(["a", "c"], {"Int": 2}, 1)  # lower stamp, different field a.c
    coarse = LwwMapCrdt()
    assert coarse.merge(coarse.merge(_NESTED, hi, "A"), lo, "B")["Map"]["a"]["Map"]["c"] == {"Int": 0}  # dropped
    deep = DeepLwwCrdt()
    assert deep.merge(deep.merge(_NESTED, hi, "A"), lo, "B")["Map"]["a"]["Map"]["c"] == {"Int": 2}  # kept


def test_deep_lww_converges_over_every_order():
    # writes to distinct field paths, each with its own stamp; every permutation converges (conflict-free)
    writes = [
        (_set(["a", "b"], {"Int": 1}, 3), "A"),
        (_set(["a", "c"], {"Int": 2}, 1), "B"),
        (_set(["z"], {"Int": 9}, 2), "C"),
    ]
    results = []
    for perm in itertools.permutations(writes):
        crdt = DeepLwwCrdt()
        v = _NESTED
        for patch, origin in perm:
            v = crdt.merge(v, patch, origin)
        results.append(v)
    assert all(r == results[0] for r in results)  # order-independent
    assert results[0]["Map"]["a"]["Map"]["b"] == {"Int": 1}
    assert results[0]["Map"]["a"]["Map"]["c"] == {"Int": 2}
    assert results[0]["Map"]["z"] == {"Int": 9}


def test_deep_lww_same_field_converges_by_stamp():
    # two writes to the SAME nested field; the higher stamp wins, in either arrival order
    hi = _set(["a", "b"], {"Int": 5}, 2)
    lo = _set(["a", "b"], {"Int": 4}, 1)
    a, b = DeepLwwCrdt(), DeepLwwCrdt()
    v_a = a.merge(a.merge(_NESTED, hi, "A"), lo, "B")
    v_b = b.merge(b.merge(_NESTED, lo, "B"), hi, "A")
    assert v_a == v_b
    assert v_a["Map"]["a"]["Map"]["b"] == {"Int": 5}  # higher (rev 2) wins


def test_hub_shares_with_deep_lww_merge():
    # two WRITE subscribers edit different nested fields of one shared model; both land authoritatively
    h = hub()
    sid = h.share({"Map": {"a": {"Map": {"b": {"Int": 0}, "c": {"Int": 0}}}}}, type_name="Nested", merge=DeepLwwCrdt)
    h.subscribe("t1", sid, WRITE)
    h.subscribe("t2", sid, WRITE)
    h.open(("t1", "a"))
    h.open(("t2", "b"))
    h.recv(("t1", "a"), '{"t":"patch","id":%d,"patch":{"rev":1,"ops":[{"Set":{"path":[{"Key":"a"},{"Key":"b"}],"value":{"Int":1}}}]}}' % sid)
    h.recv(("t2", "b"), '{"t":"patch","id":%d,"patch":{"rev":1,"ops":[{"Set":{"path":[{"Key":"a"},{"Key":"c"}],"value":{"Int":2}}}]}}' % sid)
    shared = h._shared[sid].value["Map"]["a"]["Map"]
    assert shared["b"] == {"Int": 1} and shared["c"] == {"Int": 2}


def test_starlette_two_tenants_share_over_mixed_codecs():
    from starlette.applications import Starlette
    from starlette.routing import WebSocketRoute
    from starlette.testclient import TestClient

    h = Hub(key=lambda ws: ws.path_params["tenant"])
    sid = h.share(Doc())
    h.subscribe("alice", sid, WRITE)
    h.subscribe("bob", sid, WRITE)
    app = Starlette(routes=[WebSocketRoute("/ws/{tenant}", ws_endpoint(h))])

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
