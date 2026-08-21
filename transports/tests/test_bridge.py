import inspect
from typing import Annotated, Literal

import pytest
from pydantic import BaseModel, Field, SerializeAsAny, model_validator

from transports import Session, from_value, schema_of, to_value


class Sub(BaseModel):
    label: str = ""
    tags: list = []


class Device(BaseModel):
    name: str
    on: bool = False
    meta: Sub = Sub()


def test_to_from_value_round_trip():
    d = Device(name="lamp", on=True, meta=Sub(label="x", tags=["a", "b"]))
    v = to_value(d)
    assert v["Map"]["on"] == {"Bool": True}
    assert v["Map"]["meta"]["Map"]["tags"] == {"List": [{"Str": "a"}, {"Str": "b"}]}
    assert from_value(v, Device) == d


def test_schema():
    s = schema_of(Device)
    assert s["type_name"] == "Device"
    types = {f["name"]: f["ty"] for f in s["fields"]}
    assert types == {"name": "Str", "on": "Bool", "meta": "Map"}


def test_reactive_emit():
    sess = Session()
    d = Device(name="lamp")
    sess.host(d)
    d.on = True
    patches = sess.drain()
    assert len(patches) == 1
    _, patch = patches[0]
    assert patch["ops"] == [{"Set": {"path": [{"Key": "on"}], "value": {"Bool": True}}}]


def test_coalesce_between_flushes():
    sess = Session()
    d = Device(name="lamp")
    sess.host(d)
    d.on = True
    d.name = "lamp2"
    patches = sess.drain()  # one flush coalesces both writes into one patch
    assert len(patches) == 1
    _, patch = patches[0]
    assert len(patch["ops"]) == 2


def test_nested_model_mutation_emits_nested_path():
    sess = Session()
    d = Device(name="lamp", meta=Sub(label="a"))
    sess.host(d)
    d.meta.label = "b"
    patches = sess.drain()
    assert len(patches) == 1
    _, patch = patches[0]
    assert patch["ops"] == [{"Set": {"path": [{"Key": "meta"}, {"Key": "label"}], "value": {"Str": "b"}}}]


def test_nested_list_append_emits_insert():
    sess = Session()
    d = Device(name="lamp", meta=Sub(tags=["a"]))
    sess.host(d)
    d.meta.tags.append("b")
    patches = sess.drain()
    assert len(patches) == 1
    _, patch = patches[0]
    assert patch["ops"] == [{"Insert": {"path": [{"Key": "meta"}, {"Key": "tags"}], "index": 1, "value": {"Str": "b"}}}]


def test_nested_list_reorder_emits_moves():
    sess = Session()
    d = Device(name="lamp", meta=Sub(tags=["a", "b", "c", "d"]))
    sess.host(d)
    d.meta.tags[:] = ["d", "b", "a", "c"]

    patches = sess.drain()

    assert len(patches) == 1
    _, patch = patches[0]
    assert patch["ops"] == [
        {"Move": {"path": [{"Key": "meta"}, {"Key": "tags"}], "from": 3, "to": 0}},
        {"Move": {"path": [{"Key": "meta"}, {"Key": "tags"}], "from": 2, "to": 1}},
    ]


def test_no_change_no_patch():
    sess = Session()
    d = Device(name="lamp", on=True)
    sess.host(d)
    d.on = True  # same value
    assert sess.drain() == []


class Circle(BaseModel):
    kind: Literal["circle"] = "circle"
    radius: int = 1


class Square(BaseModel):
    kind: Literal["square"] = "square"
    side: int = 1


class Drawing(BaseModel):
    shape: Annotated[Circle | Square, Field(discriminator="kind")]


def test_discriminated_union_round_trip_preserves_subclass():
    # inlining nested models as Maps keeps the discriminator in the dump, so
    # from_value re-dispatches to the concrete subclass
    d = Drawing(shape=Square(side=3))
    m = from_value(to_value(d), Drawing)
    assert isinstance(m.shape, Square)
    assert m == d


class Node(BaseModel):
    """ccflow-style polymorphism: a `type_` field in the dump drives subclass dispatch on validate."""

    type_: str = ""

    @model_validator(mode="wrap")
    @classmethod
    def _load_subclass(cls, value, handler):
        if cls is Node and isinstance(value, dict):
            target = _NODE_TYPES.get(value.get("type_"))
            if target is not None:
                return target.model_validate(value)
        return handler(value)


class TaskNode(Node):
    type_: str = "task"
    steps: list = []


_NODE_TYPES = {"task": TaskNode}


class Flow(BaseModel):
    # SerializeAsAny makes the dump duck-typed on any pydantic 2.x (ccflow's BaseModel applies it
    # to every field via its metaclass); on 2.13+ the bridge dumps polymorphically by default
    root: SerializeAsAny[Node] = Node()


def test_type_field_dispatch_round_trip_preserves_subclass():
    f = Flow(root=TaskNode(steps=["a", "b"]))
    m = from_value(to_value(f), Flow)
    assert isinstance(m.root, TaskNode)
    assert m.root.steps == ["a", "b"]
    assert m == f


@pytest.mark.skipif(
    "polymorphic_serialization" not in inspect.signature(BaseModel.model_dump).parameters,
    reason="pydantic < 2.13 dumps base-annotated fields by their annotation",
)
def test_plain_base_annotated_field_round_trips_subclass():
    # the bridge dumps polymorphically by default on pydantic 2.13+, so a subclass instance in a
    # base-annotated field keeps its fields with no SerializeAsAny or config on the model
    class PlainFlow(BaseModel):
        root: Node = Node()

    f = PlainFlow(root=TaskNode(steps=["a"]))
    m = from_value(to_value(f), PlainFlow)
    assert isinstance(m.root, TaskNode)
    assert m.root.steps == ["a"]


def test_mirror_across_sessions():
    server = Session()
    d = Device(name="lamp", on=False)
    sid = server.host(d)

    client = Session()
    cd = from_value(server.snapshot(sid)["value"], Device)
    cid = client.host(cd)

    d.on = True
    d.meta.label = "living-room"
    for _mid, patch in server.drain():
        assert client.apply_patch(cid, patch) is True

    mirrored = from_value(client.value(cid), Device)
    assert mirrored.on is True
    assert mirrored.meta.label == "living-room"
