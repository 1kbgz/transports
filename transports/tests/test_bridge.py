from pydantic import BaseModel

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


def test_no_change_no_patch():
    sess = Session()
    d = Device(name="lamp", on=True)
    sess.host(d)
    d.on = True  # same value
    assert sess.drain() == []


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
