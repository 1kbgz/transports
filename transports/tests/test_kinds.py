from dataclasses import dataclass, field

import msgspec

from transports import Session, from_value, schema_of, schema_to_ts, to_value


@dataclass
class DCSub:
    label: str = ""
    tags: list = field(default_factory=list)


@dataclass
class DCDevice:
    name: str
    on: bool = False
    meta: DCSub = field(default_factory=DCSub)


class MSub(msgspec.Struct):
    label: str = ""
    tags: list = msgspec.field(default_factory=list)


class MDevice(msgspec.Struct):
    name: str
    on: bool = False
    meta: MSub = msgspec.field(default_factory=MSub)


def test_dataclass_round_trip():
    d = DCDevice(name="lamp", on=True, meta=DCSub(label="x", tags=["a"]))
    v = to_value(d)
    assert v["Map"]["on"] == {"Bool": True}
    assert from_value(v, DCDevice) == d


def test_dataclass_schema_and_ts():
    s = schema_of(DCDevice)
    assert {f["name"]: f["ty"] for f in s["fields"]} == {"name": "Str", "on": "Bool", "meta": "Map"}
    assert schema_to_ts(s) == ("export interface DCDevice {\n  name: string;\n  on: boolean;\n  meta: Record<string, unknown>;\n}")


def test_dataclass_reactive_auto():
    sess = Session()
    d = DCDevice(name="lamp", meta=DCSub(label="a"))
    sess.host(d)
    d.on = True
    d.meta.label = "b"
    patches = sess.drain()
    assert len(patches) == 1
    ops = patches[0][1]["ops"]
    assert {"Set": {"path": [{"Key": "on"}], "value": {"Bool": True}}} in ops
    assert {"Set": {"path": [{"Key": "meta"}, {"Key": "label"}], "value": {"Str": "b"}}} in ops


def test_msgspec_round_trip():
    m = MDevice(name="lamp", on=True, meta=MSub(label="x"))
    v = to_value(m)
    assert v["Map"]["name"] == {"Str": "lamp"}
    assert from_value(v, MDevice) == m


def test_msgspec_schema():
    s = schema_of(MDevice)
    assert {f["name"]: f["ty"] for f in s["fields"]} == {"name": "Str", "on": "Bool", "meta": "Map"}


def test_msgspec_reactive_via_update():
    sess = Session()
    m = MDevice(name="lamp")
    mid = sess.host(m)
    m.on = True  # msgspec has no __dict__ → not auto-watched
    assert sess.drain() == []  # nothing emitted automatically
    patches = sess.update(mid)  # explicit re-diff
    assert len(patches) == 1
    assert patches[0][1]["ops"] == [{"Set": {"path": [{"Key": "on"}], "value": {"Bool": True}}}]
