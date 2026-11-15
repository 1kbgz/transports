import msgspec
import pytest
from pydantic import BaseModel, Field, RootModel

from transports import Client, Hub, Server, Session, protocol, to_value


def preset_model(trigger_first, alias=False):
    def assign(self, name, value):
        BaseModel.__setattr__(self, name, value)
        if name == "preset" and value == "loud":
            self.gain = 9
            self.samples.append(9)

    fields = {"preset": str, "gain": int, "samples": list[int]}
    if not trigger_first:
        fields = dict(reversed(fields.items()))
    return type(
        "Preset",
        (BaseModel,),
        {
            "__annotations__": fields,
            "model_config": {"serialize_by_alias": alias, "populate_by_name": True},
            "preset": Field(default="", alias="selection" if alias else None),
            "gain": 1,
            "samples": [],
            "__setattr__": assign,
        },
    )()


def set_op(field, value):
    return {"Set": {"path": [{"Key": field}], "value": value}}


def test_authoritative_apply_replaces_pending_local_writes_without_echo():
    class Model(BaseModel):
        x: int = 0
        y: int = 0

    model = Model()
    session = Session()
    mid = session.host(model)
    model.y = 9
    assert session.apply_patch(mid, {"rev": 1, "ops": [set_op("x", {"Int": 2})]})
    assert model == Model(x=2)
    assert session.drain() == []
    assert session.value(mid) == to_value(model)


def test_authoritative_apply_does_not_echo_validation_coercion():
    class Model(BaseModel):
        x: int = 0

    model = Model()
    session = Session()
    mid = session.host(model)
    assert session.apply_patch(mid, {"rev": 1, "ops": [set_op("x", {"Str": "2"})]})
    assert model.x == 2
    assert session.drain() == []
    assert session.snapshot(mid)["rev"] == 1


@pytest.mark.parametrize("submit", [False, True])
@pytest.mark.parametrize("shape", ["scalar", "root_map", "array"])
def test_inbound_apply_preserves_non_field_map_models(shape, submit):
    if shape == "scalar":
        model = RootModel[int](0)
        expected = RootModel[int](2)
        path = []
    elif shape == "root_map":
        model = RootModel[dict[str, int]]({"root": 0, "x": 0})
        expected = RootModel[dict[str, int]]({"root": 0, "x": 2})
        path = [{"Key": "x"}]
    else:

        class Model(msgspec.Struct, array_like=True):
            x: int = 0

        model = Model()
        expected = Model(x=2)
        path = [{"Index": 0}]
    session = Session()
    mid = session.host(model)
    patch = {"rev": 1, "ops": [{"Set": {"path": path, "value": {"Int": 2}}}]}
    if submit:
        assert session.submit(mid, patch) == patch
    else:
        assert session.apply_patch(mid, patch)
    assert model == expected
    assert session.drain() == []
    assert session.value(mid) == to_value(model)


@pytest.mark.parametrize("trigger_first", [False, True])
@pytest.mark.parametrize("alias", [False, True])
@pytest.mark.parametrize("submit", [False, True])
def test_observer_writes_are_emitted_after_inbound_apply(trigger_first, alias, submit):
    model = preset_model(trigger_first, alias)
    session = Session()
    mid = session.host(model)
    seen = []
    session.on_patch(lambda model_id, patch: seen.append((model_id, patch)))
    patch = {"rev": 1, "ops": [set_op("selection" if alias else "preset", {"Str": "loud"})]}
    if submit:
        assert session.submit(mid, patch) == patch
    else:
        assert session.apply_patch(mid, patch)
    assert model.gain == 9
    assert model.samples == [9]
    followup = session.drain()
    assert len(followup) == 1
    assert followup[0][0] == mid
    assert followup[0][1]["rev"] == 2
    assert seen == followup
    assert session.value(mid) == to_value(model)
    assert session.drain() == []
    if submit:
        assert session.since(mid, 0) == [patch, followup[0][1]]


def test_unchanged_inbound_values_do_not_run_observers_or_echo():
    model = preset_model(True)
    session = Session()
    mid = session.host(model)
    patch = {"rev": 0, "ops": [set_op("preset", {"Str": "loud"})]}
    assert session.submit(mid, patch)
    session.drain()
    authoritative = session.submit(mid, patch)
    assert authoritative is not None
    assert authoritative["ops"] == []
    assert model.samples == [9]
    assert session.drain() == []


def test_observer_can_normalize_the_incoming_field():
    class Model(BaseModel):
        name: str = ""

        def __setattr__(self, name, value):
            super().__setattr__(name, value)
            if name == "name" and value != value.strip():
                self.name = value.strip()

    model = Model()
    session = Session()
    mid = session.host(model)
    authoritative = session.submit(mid, {"rev": 0, "ops": [set_op("name", {"Str": " trimmed "})]})
    assert authoritative is not None
    assert authoritative["rev"] == 1
    assert session.drain() == [(mid, {"rev": 2, "ops": [set_op("name", {"Str": "trimmed"})]})]
    assert session.value(mid) == to_value(model)


@pytest.mark.parametrize("trigger_first", [False, True])
def test_explicit_field_assignments_follow_model_declaration_order(trigger_first):
    model = preset_model(trigger_first)
    session = Session()
    mid = session.host(model)
    assert session.submit(mid, {"rev": 0, "ops": [set_op("gain", {"Int": 4}), set_op("preset", {"Str": "loud"})]})
    assert model.gain == (4 if trigger_first else 9)
    assert model.samples == [9]
    assert session.value(mid) == to_value(model)


@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
@pytest.mark.parametrize("tenant", [False, True])
def test_proposal_and_observer_patches_arrive_in_revision_order(codec, tenant):
    session = Session()
    if tenant:
        server = Hub(key=lambda conn: "tenant")
        session = server.tenant("tenant")
    else:
        server = Server(session)
    model = preset_model(True)
    mid = session.host(model)
    clients = {conn: Client(codec) for conn in ("proposer", "peer")}
    for conn, client in clients.items():
        for frame in server.open(conn, codec):
            client.recv(frame)
    model.gain = 2  # a pending host write must precede the proposal
    proposal = clients["proposer"].edit_ops(mid, [set_op("preset", {"Str": "loud"})], "preset-edit")
    output = server.recv("proposer", proposal)
    for conn, client in clients.items():
        messages = [protocol.decode(frame, codec) for frame in output[conn]]
        assert [message["patch"]["rev"] for message in messages] == [1, 2, 3]
        assert [message.get("proposal") for message in messages] == [None, "preset-edit" if conn == "proposer" else None, None]
        for frame in output[conn]:
            client.recv(frame)
        assert client.value(mid) == session.value(mid) == to_value(model)
    assert server.flush() == {}
    replay = [protocol.decode(frame, codec) for frame in server.open("reconnected", codec, since={mid: 0})]
    assert [message["patch"]["rev"] for message in replay] == [1, 2, 3]
