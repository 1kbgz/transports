import asyncio
import itertools
import json

from pydantic import BaseModel

from transports import READ, WRITE, Client, DeepLwwCrdt, Hub, LastWriteWins, LwwMapCrdt, protocol, register_codec, unregister_codec, ws_endpoint


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


def test_reopening_connection_under_new_tenant_removes_old_routing():
    tenant_for = {"conn": "t1"}
    h = Hub(key=tenant_for.__getitem__)
    first = Doc()
    second = Doc()
    h.tenant("t1").host(first)
    h.tenant("t2").host(second)
    h.open("conn")

    tenant_for["conn"] = "t2"
    h.open("conn")
    first.x = 1
    assert h.flush() == {}

    second.x = 2
    assert set(h.flush()) == {"conn"}


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


def test_broken_custom_encoder_does_not_block_private_fanout():
    codec = "application/x-broken-hub-private-test"
    register_codec(codec, json.dumps, json.loads)
    h = hub()
    model = Doc()
    h.tenant("t1").host(model)
    broken, healthy = ("t1", "broken"), ("t1", "healthy")
    h.open(broken, codec=codec)
    h.open(healthy)

    def encode(_value):
        raise ValueError("broken encoder")

    register_codec(codec, encode, json.loads)
    try:
        model.x = 1
        out = h.flush()
        assert set(out) == {healthy}
        assert broken not in h._codecs
        assert json.loads(out[healthy][0])["patch"]["rev"] == 1
    finally:
        unregister_codec(codec)


def test_broken_custom_encoder_does_not_leave_failed_hub_open_registered():
    codec = "application/x-broken-hub-open-test"

    def encode(_value):
        raise ValueError("broken encoder")

    register_codec(codec, encode, json.loads)
    h = hub()
    h.tenant("t1").host(Doc())
    conn = ("t1", "broken")
    try:
        try:
            h.open(conn, codec=codec)
        except ValueError:
            pass
        else:
            raise AssertionError("expected encoder failure")
        assert conn not in h._codecs
        assert conn not in h._conn_key
        assert conn not in h._conns_by_key.get("t1", ())
    finally:
        unregister_codec(codec)


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


def test_shared_awareness_is_ephemeral_and_connection_scoped():
    h = hub()
    sid = h.share(Doc())
    h.subscribe("reader", sid, READ)
    h.subscribe("writer", sid, WRITE)
    reader = ("reader", "a")
    writer = ("writer", "a")
    observer = ("writer", "b")
    h.open(reader)
    writer_client = Client()
    for frame in h.open(writer):
        writer_client.recv(frame)

    updates = []
    writer_client.on_awareness(updates.append)
    state = {"selection": {"anchor": 2, "head": 4}}
    out = h.recv(reader, protocol.awareness_msg(sid, state))
    assert set(out) == {writer}
    writer_client.recv(out[writer][0])
    [(peer, remote)] = writer_client.awareness(sid).items()
    assert remote == state
    assert updates[-1]["peer"] == peer

    # A later subscriber receives current awareness, but it is not part of the model snapshot.
    observer_frames = [json.loads(frame) for frame in h.open(observer)]
    assert [frame["t"] for frame in observer_frames] == ["snapshot", "awareness"]
    assert observer_frames[1] == {"t": "awareness", "id": sid, "state": state, "peer": peer}
    assert h.snapshot_shared(sid)["value"] == {"Map": {"x": {"Int": 0}, "y": {"Int": 0}}}

    cleared = h.recv(reader, protocol.awareness_msg(sid, None))
    assert json.loads(cleared[writer][0])["state"] is None
    h.recv(reader, protocol.awareness_msg(sid, state))

    # Closing the publishing connection removes its state from every remaining subscriber.
    leave = h.close(reader)
    assert set(leave) == {writer, observer}
    writer_client.recv(leave[writer][0])
    assert writer_client.awareness(sid) == {}
    assert updates[-1] == {"t": "awareness", "id": sid, "state": None, "peer": peer}

    sent = []

    def sender(frame):
        sent.append(frame)

    asyncio.run(writer_client.attach(sender))
    assert asyncio.run(writer_client.set_awareness(sid, state)) is True
    assert protocol.decode(sent[-1])["state"] == state
    writer_client.recv(protocol.awareness_msg(sid, state, "temporary-peer"))
    assert writer_client.detach(sender) is True
    assert updates[-1]["state"] is None

    resent = []
    replacement_sender = resent.append
    assert asyncio.run(writer_client.set_awareness(sid, {"selection": {"anchor": 5}})) is False
    asyncio.run(writer_client.attach(replacement_sender))
    assert protocol.decode(resent[0])["state"] == {"selection": {"anchor": 5}}
    assert asyncio.run(writer_client.set_awareness(sid, None)) is True
    writer_client.detach(replacement_sender)

    outsider = ("outsider", "a")
    h.open(outsider)
    assert h.recv(outsider, protocol.awareness_msg(sid, state)) == {}


def test_broken_custom_encoder_does_not_block_shared_fanout():
    codec = "application/x-broken-hub-shared-test"
    register_codec(codec, json.dumps, json.loads)
    h = hub()
    sid = h.share(Doc())
    h.subscribe("broken", sid, WRITE)
    h.subscribe("healthy", sid, READ)
    broken, healthy = ("broken", "a"), ("healthy", "a")
    h.open(broken, codec=codec)
    h.open(healthy)
    proposal = protocol.patch_msg(sid, {"rev": 2, "ops": [{"Set": {"path": [{"Key": "x"}], "value": {"Int": 2}}}]})

    def encode(_value):
        raise ValueError("broken encoder")

    register_codec(codec, encode, json.loads)
    try:
        h.set_shared(sid, Doc(x=1))
        out = h.flush()
        assert set(out) == {healthy}
        assert broken not in h._codecs
        assert json.loads(out[healthy][0])["patch"]["rev"] == 1
        assert h.recv(broken, proposal) == {}
        assert h._shared[sid].value["Map"]["x"] == {"Int": 1}
    finally:
        unregister_codec(codec)


def test_read_only_subscriber_cannot_write():
    h = hub()
    sid = h.share(Doc())
    h.subscribe("t1", sid, READ)
    c = ("t1", "a")
    h.open(c)
    edit = json.dumps({"t": "patch", "id": sid, "patch": {"rev": 1, "ops": [{"Set": {"path": [{"Key": "x"}], "value": {"Int": 9}}}]}})
    out = h.recv(c, edit)
    assert set(out) == {c}  # the write is refused and only the proposer hears about it
    reject = json.loads(out[c][0])
    assert reject["t"] == "reject" and reject["error"] == "read-only subscription"
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
    assert json.loads(out[c1][0])["proposal"] == "auto-1"
    assert "proposal" not in json.loads(out[c2][0])
    for m in out[c2]:
        cl2.recv(m)
    assert cl2.value(sid)["Map"]["x"] == {"Int": 5}


def test_pending_shared_patch_precedes_client_write_reply():
    h = hub()
    sid = h.share(Doc())
    h.subscribe("t1", sid, WRITE)
    h.subscribe("t2", sid, WRITE)
    conns = [("t1", "a"), ("t2", "b")]
    clients = {conn: Client() for conn in conns}
    for conn, client in clients.items():
        for frame in h.open(conn):
            client.recv(frame)

    h.set_shared(sid, Doc(x=1))
    reply = h.recv(conns[0], clients[conns[0]].edit(sid, {"Map": {"x": {"Int": 0}, "y": {"Int": 1}}}))

    for conn, client in clients.items():
        assert [json.loads(frame)["patch"]["rev"] for frame in reply[conn]] == [1, 2]
        for frame in reply[conn]:
            client.recv(frame)
        assert client.model(sid, Doc) == Doc(x=1, y=1)
    assert h.flush() == {}


def test_private_reply_only_drains_the_proposer_tenant():
    h = hub()
    models = {tenant: Doc() for tenant in ("t1", "t2")}
    conns = {tenant: (tenant, "a") for tenant in models}
    clients = {tenant: Client() for tenant in models}
    for tenant, model in models.items():
        h.tenant(tenant).host(model)
        for frame in h.open(conns[tenant]):
            clients[tenant].recv(frame)

    models["t1"].x = 1
    models["t2"].x = 2
    out = h.recv(conns["t1"], clients["t1"].edit(1, {"Map": {"x": {"Int": 1}, "y": {"Int": 1}}}))

    assert set(out) == {conns["t1"]}
    remaining = h.flush()
    assert set(remaining) == {conns["t2"]}
    assert json.loads(remaining[conns["t2"]][0])["patch"]["rev"] == 1


def test_shared_noop_acknowledges_origin_without_fanout():
    h = hub()
    sid = h.share(Doc())
    h.subscribe("t1", sid, WRITE)
    h.subscribe("t2", sid, WRITE)
    c1, c2 = ("t1", "a"), ("t2", "b")
    cl1 = Client()
    for msg in h.open(c1):
        cl1.recv(msg)
    h.open(c2)
    acknowledgements = []
    cl1.on_ack(acknowledgements.append)

    frame = cl1.edit_ops(
        sid,
        [{"Set": {"path": [{"Key": "x"}], "value": {"Int": 0}}}],
        "same-value",
    )
    out = h.recv(c1, frame)
    assert set(out) == {c1}
    assert json.loads(out[c1][0]) == {"t": "ack", "id": sid, "rev": 0, "proposal": "same-value"}
    cl1.recv(out[c1][0])
    assert acknowledgements[0]["proposal"] == "same-value"


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
        v: dict = _NESTED
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
    h.recv(
        ("t1", "a"),
        json.dumps(
            {
                "t": "patch",
                "id": sid,
                "patch": {"rev": 1, "ops": [{"Set": {"path": [{"Key": "a"}, {"Key": "b"}], "value": {"Int": 1}}}]},
            }
        ),
    )
    h.recv(
        ("t2", "b"),
        json.dumps(
            {
                "t": "patch",
                "id": sid,
                "patch": {"rev": 1, "ops": [{"Set": {"path": [{"Key": "a"}, {"Key": "c"}], "value": {"Int": 2}}}]},
            }
        ),
    )
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

    with TestClient(app) as tc, tc.websocket_connect("/ws/bob?codec=msgpack") as wb:
        cb = Client(codec="msgpack")
        cb.recv(wb.receive_bytes())
        with tc.websocket_connect("/ws/alice?codec=json") as wa:
            ca = Client()
            ca.recv(wa.receive_text())

            # alice writes the shared model; bob (a different tenant, different codec) receives it
            edit = ca.edit(sid, {"Map": {"x": {"Int": 42}, "y": {"Int": 0}}})
            assert isinstance(edit, str)
            wa.send_text(edit)
            cb.recv(wb.receive_bytes())
            assert cb.value(sid)["Map"]["x"] == {"Int": 42}

            wa.send_text(protocol.awareness_msg(sid, {"selection": {"anchor": 1}}))
            cb.recv(wb.receive_bytes())
            assert len(cb.awareness(sid)) == 1

        cb.recv(wb.receive_bytes())
        assert cb.awareness(sid) == {}


def test_retire_tenant_releases_private_state_and_subscriptions():
    import gc
    import weakref

    import pytest

    h = hub()
    session = h.tenant("leaving")
    model = Doc(x=1)
    session.host(model)
    ref = weakref.ref(session)
    sid = h.share(Doc(x=2))
    h.subscribe("leaving", sid, WRITE)
    h.subscribe("staying", sid, READ)
    conn = ("leaving", "tab")
    h.open(conn)
    with pytest.raises(ValueError, match="active connections"):
        h.retire_tenant("leaving")
    h.close(conn)
    del session
    assert h.retire_tenant("leaving") is True
    assert h.retire_tenant("leaving") is False
    gc.collect()
    assert ref() is None  # externally held models must not keep the old Session alive
    model.x = 3  # its former watcher is harmless
    assert h.tenant("leaving").ids() == []
    assert h.open(conn) == []  # subscription permissions must be granted again
    assert len(h.open(("staying", "tab"))) == 1
    assert h.snapshot_shared(sid)["value"]["Map"]["x"] == {"Int": 2}


def test_idle_timeout_preserves_reconnects_and_expires_after_last_connection(monkeypatch):
    now = [0.0]
    monkeypatch.setattr("transports.hub.monotonic", lambda: now[0])
    h = Hub(key=lambda conn: conn[0], tenant_idle_timeout=10)
    session = h.tenant("user")
    session.host(Doc(x=1))
    one, two = ("user", "one"), ("user", "two")
    h.open(one)
    h.open(two)
    h.close(one)
    now[0] = 100
    assert h.prune_tenants() == []
    h.close(two)
    now[0] = 109
    assert h.prune_tenants() == []
    h.open(one)
    assert h.tenant("user") is session
    now[0] = 200
    assert h.prune_tenants() == []
    h.close(one)
    now[0] = 209
    h.close(one)  # duplicate closes must not extend the reconnect window
    now[0] = 210
    assert h.prune_tenants() == ["user"]
    assert h.prune_tenants() == []
    assert h.tenant("user") is not session


def test_timeout_applies_to_unused_tenants_and_flush_and_late_reconnect(monkeypatch):
    now = [0.0]
    monkeypatch.setattr("transports.hub.monotonic", lambda: now[0])
    h = Hub(key=lambda conn: conn, tenant_idle_timeout=5)
    sid = h.share(Doc())
    h.subscribe("never-connected", sid)
    expired = h.tenant("late")
    expired.host(Doc())
    h.open("late")
    h.close("late")
    now[0] = 5
    assert h.open("late") == []  # expire even if autosync has not run
    assert h.tenant("late") is not expired
    assert h.flush() == {}
    assert "never-connected" not in h._tenants
    assert "never-connected" not in h._shared[sid].subs
    assert "late" in h._tenants


def test_tenant_timeout_is_opt_in_and_none_is_a_valid_key(monkeypatch):
    now = [0.0]
    monkeypatch.setattr("transports.hub.monotonic", lambda: now[0])
    for timeout in (None, 1):
        h = Hub(key=lambda conn: conn, tenant_idle_timeout=timeout)
        session = h.tenant(None)
        h.open(None)
        h.close(None)
        now[0] += 100
        h.flush()
        assert (h.tenant(None) is session) == (timeout is None)


def test_rekeyed_connection_starts_previous_tenants_idle_window(monkeypatch):
    now = [0.0]
    monkeypatch.setattr("transports.hub.monotonic", lambda: now[0])
    keys = {"conn": "old"}
    h = Hub(key=keys.__getitem__, tenant_idle_timeout=5)
    h.open("conn")
    keys["conn"] = "new"
    h.open("conn")
    now[0] = 5
    assert h.prune_tenants() == ["old"]
    assert "new" in h._tenants


def test_tenant_timeout_rejects_invalid_values():
    import pytest

    for timeout in (0, -1, float("nan"), float("inf"), True, "5"):
        with pytest.raises(ValueError, match="tenant_idle_timeout"):
            Hub(key=lambda conn: conn, tenant_idle_timeout=timeout)  # ty: ignore[invalid-argument-type]


def test_retired_private_revision_numbers_are_not_reused_for_resume():
    h = hub()
    session = h.tenant("user")
    model = Doc(x=1)
    mid = session.host(model)
    old = ("user", "old")
    h.open(old)
    model.x = 2
    h.flush()
    revision = session.snapshot(mid)["rev"]
    h.close(old)
    assert h.retire_tenant("user")
    replacement = h.tenant("user")
    assert replacement.host(Doc(x=3)) == mid
    for name in ("reconnected-tab", "another-old-tab"):
        [wire] = h.open(("user", name), since={mid: revision})
        message = json.loads(wire)
        assert message["t"] == "snapshot"
        assert message["value"]["Map"]["x"] == {"Int": 3}


def test_autosync_prunes_inactive_tenants(monkeypatch):
    from contextlib import suppress

    from transports import autosync

    async def run():
        now = [0.0]
        monkeypatch.setattr("transports.hub.monotonic", lambda: now[0])
        h = Hub(key=lambda conn: conn, tenant_idle_timeout=5)
        h.open("user")
        h.close("user")
        retired = asyncio.Event()
        original = h.retire_tenant

        def retire(key):
            result = original(key)
            retired.set()
            return result

        monkeypatch.setattr(h, "retire_tenant", retire)
        task = asyncio.create_task(autosync(h))
        now[0] = 5
        try:
            await asyncio.wait_for(retired.wait(), timeout=1)
            assert "user" not in h._tenants
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_retiring_an_inactive_tenant_during_flush_does_not_interrupt_other_tenants():
    h = hub()
    inactive = Doc()
    session = h.tenant("inactive")
    session.host(inactive)

    def retire(mid, patch):
        h.retire_tenant("inactive")

    session.on_patch(retire)
    active = Doc()
    h.tenant("active").host(active)
    conn = ("active", "tab")
    h.open(conn)
    inactive.x = 1
    active.x = 2
    assert set(h.flush()) == {conn}
    assert "inactive" not in h._tenants
