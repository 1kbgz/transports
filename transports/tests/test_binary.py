import base64
import json
from dataclasses import dataclass

import msgspec
import pytest
from pydantic import BaseModel

from transports import Client, Server, Session, from_value, protocol, schema_of, schema_to_ts, serve_anywidget, to_value
from transports._buffers import join_buffers, split_buffers
from transports.transports import BufferFrame, decode_as, encode_as


class Sample(BaseModel):
    data: bytes = b""


@dataclass
class DataSample:
    data: bytes = b""


class StructSample(msgspec.Struct):
    data: bytes = b""


@pytest.mark.parametrize("cls", [Sample, DataSample, StructSample])
def test_binary_bridges(cls):
    raw = bytes(range(256))
    value = to_value(cls(data=raw))
    assert value == {"Map": {"data": {"Bytes": base64.b64encode(raw).decode()}}}
    assert from_value(value, cls).data == raw
    assert schema_of(cls)["fields"] == [{"name": "data", "ty": "Bytes"}]
    assert "data: Uint8Array" in schema_to_ts(schema_of(cls))
    for buffer in [bytearray(raw), memoryview(raw)]:
        assert to_value({"data": buffer}) == value


def test_binary_golden_vectors():
    value = '{"Bytes":"AP8B"}'
    for codec, expected in [
        ("application/json", value.encode().hex()),
        ("application/msgpack", "81a54279746573c40300ff01"),
        ("application/cbor", "a16542797465734300ff01"),
    ]:
        wire = encode_as(value, codec)
        assert wire.hex() == expected
        assert decode_as(wire, codec) == value


@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
def test_binary_session_round_trip(codec):
    model = Sample(data=b"\x00\xff")
    session = Session()
    mid = session.host(model)
    server = Server(session)
    client = Client(codec=codec)
    for wire in server.open("client", codec=codec):
        client.recv(wire)
    assert client.model(mid, Sample).data == model.data
    replacement = bytes(range(256)) * 1024
    for wire in server.recv("client", client.edit(mid, to_value(Sample(data=replacement))))["client"]:
        if codec != "json":
            assert len(wire) < len(replacement) + 1024
        client.recv(wire)
    assert model.data == replacement
    assert client.model(mid, Sample).data == replacement
    model.data = b"next"
    for wire in server.flush()["client"]:
        client.recv(wire)
    assert client.model(mid, Sample).data == b"next"


@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
def test_anywidget_large_buffer_both_directions(codec):
    class Widget:
        def on_msg(self, callback):
            self.callback = callback

        def send(self, data, buffers=()):
            self.sent = data, list(buffers)

    model = Sample(data=b"\xff" * (10 * 1024 * 1024))
    session = Session()
    mid = session.host(model)
    server = Server(session)
    widget = Widget()
    serve_anywidget(server, widget, codec=codec)
    widget.callback(widget, {"ready": True}, [])
    content, buffers = widget.sent
    assert len(json.dumps(content)) < 500
    assert buffers == [model.data]
    client = Client(codec=codec)
    client.recv(join_buffers(content["wire"], buffers, codec))
    assert client.model(mid, Sample).data == model.data
    edit = client.edit(mid, to_value(Sample(data=b"\x00\xff\x01")))
    header, buffers = split_buffers(edit, codec)
    widget.callback(widget, {"wire": header}, [memoryview(buffer) for buffer in buffers])
    assert model.data == b"\x00\xff\x01"


def test_replay_budget_deduplicates_then_evicts_to_snapshot():
    session = Session(log_bytes=20_000, blob_threshold=100)
    model = Sample()
    mid = session.host(model)
    for index in range(512):
        model.data = bytes([index % 2]) * 1024
        session.drain()
        assert session.replay_stats(mid)["bytes"] <= 20_000
    assert session.replay_stats(mid)["blobs"] == 2
    assert session.since(mid, 0) is None
    replay = session.since(mid, 511)
    assert replay is not None and len(replay) == 1
    server = Server(session)
    assert protocol.decode(server.open("c", since={mid: 0})[0])["t"] == "snapshot"
    model.data = b"z" * 25_000
    session.drain()
    assert session.replay_stats(mid) == {"patches": 0, "blobs": 0, "bytes": 0}


def test_malformed_attachments_are_rejected():
    frame = BufferFrame.from_json(protocol.snapshot_msg(1, "Sample", 0, to_value(Sample(data=b"x"))))
    with pytest.raises(ValueError, match="buffer count"):
        join_buffers(json.loads(frame.header), [])
    header = json.loads(frame.header)
    header["buffer_paths"][0][-1] = "missing"
    with pytest.raises(ValueError, match="path"):
        join_buffers(header, [b"x"])


@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
def test_crdt_binary_register_updates_and_state(codec):
    from transports import WRITE, CrdtSpec, Hub

    hub = Hub(key=lambda _: "tenant")
    sid = hub.share(Sample(data=b"old"), crdt_spec=CrdtSpec({"kind": "map"}))
    hub.subscribe("tenant", sid, WRITE)
    client = Client(codec=codec)
    for wire in hub.open("client", codec=codec):
        client.recv(wire)
    assert client.model(sid, Sample).data == b"old"
    data = bytes(range(256)) * 4096
    frame = client.edit_crdt(sid, [{"kind": "register_set", "path": [{"kind": "key", "key": "data"}], "value": data}])
    for wire in hub.recv("client", frame)["client"]:
        if codec != "json":
            assert len(wire) < len(data) + 1024
        header, buffers = split_buffers(wire, codec)
        assert len(json.dumps(header)) < 1024
        assert buffers == [data]
        client.recv(join_buffers(header, buffers, codec))
    assert client.model(sid, Sample).data == data
    assert client.pending_crdt_ops(sid) == 0
    for wire in hub.open("other", codec=codec):
        header, buffers = split_buffers(wire, codec)
        assert len(json.dumps(header)) < 2048
        assert buffers == [data]


def test_pydantic_nested_dataclass_preserves_non_utf8_bytes():
    class Envelope(BaseModel):
        payload: DataSample

    model = Envelope(payload=DataSample(data=b"\xff\x00"))
    value = to_value(model)
    assert value["Map"]["payload"]["Map"]["data"] == {"Bytes": "/wA="}
    assert from_value(value, Envelope) == model


def test_strided_memoryview_preserves_selected_bytes():
    assert to_value({"data": memoryview(b"aXbYcZ")[::2]}) == {"Map": {"data": {"Bytes": "YWJj"}}}


def test_binary_crdt_round_trip_with_custom_codec():
    from transports import WRITE, CrdtSpec, Hub, register_codec, unregister_codec

    codec = "application/test-json"
    register_codec(codec, json.dumps, json.loads)
    try:
        hub = Hub(key=lambda _: "tenant")
        sid = hub.share(Sample(data=b"old"), crdt_spec=CrdtSpec({"kind": "map"}))
        hub.subscribe("tenant", sid, WRITE)
        client = Client(codec=codec)
        for wire in hub.open("owner", codec):
            client.recv(wire)
        wire = client.edit_crdt(sid, [{"kind": "register_set", "path": [{"kind": "key", "key": "data"}], "value": b"\xff"}])
        for reply in hub.recv("owner", wire)["owner"]:
            client.recv(reply)
        assert client.model(sid, Sample).data == b"\xff"
        assert client.pending_crdt_ops(sid) == 0
    finally:
        unregister_codec(codec)


def test_nonbinary_msgspec_json_normalization_is_preserved():
    @dataclass
    class Number:
        value: float

    assert to_value(Number(float("nan"))) == {"Map": {"value": "Null"}}


def test_pydantic_binary_preserves_json_serializers_and_rich_values():
    from pathlib import Path

    from pydantic import AnyUrl, BaseModel, field_serializer

    from transports import to_value

    class Child(BaseModel):
        name: str = "child"

    class BinaryChild(Child):
        payload: bytes = b"\xff\x00"

    class Model(BaseModel):
        payload: bytes = b"\xfe"
        url: AnyUrl = AnyUrl("https://example.com/path")
        path: Path = Path("/tmp/example")
        count: int = 2
        child: Child = BinaryChild()

        @field_serializer("count", when_used="json")
        def count_label(self, value):
            return f"count={value}"

    value = to_value(Model())["Map"]
    assert value["url"] == {"Str": "https://example.com/path"}
    assert value["path"] == {"Str": str(Path("/tmp/example"))}
    assert value["count"] == {"Str": "count=2"}
    assert value["payload"] == {"Bytes": "/g=="}
    assert value["child"]["Map"]["payload"] == {"Bytes": "/wA="}


def test_binary_custom_serializer_remains_explicit():
    from pydantic import BaseModel, field_serializer

    from transports import to_value

    class Model(BaseModel):
        data: bytes = b"\xff"

        @field_serializer("data", when_used="json")
        def hex_data(self, value):
            return value.hex()

    assert to_value(Model()) == {"Map": {"data": {"Str": "ff"}}}


def test_binary_dictionary_keys_and_sets_preserve_bytes():
    class Model(BaseModel):
        data: dict[int, bytes]
        flags: dict[bool, bytes]
        values: set[bytes]

    model = Model(data={1: b"\xff"}, flags={True: b"\xfe"}, values={b"\xff", b"\xfe"})
    assert from_value(to_value(model), Model) == model


@pytest.mark.parametrize("container", [set, frozenset])
@pytest.mark.parametrize("nested", [False, True])
def test_binary_sets_restore_bytes_when_serialization_reorders_elements(container, nested):
    from pydantic import field_serializer

    class Model(BaseModel):
        values: container[tuple[bytes, int] if nested else bytes]

        @field_serializer("values", when_used="json")
        def reversed_values(self, _value):
            return list(reversed(list(self.model_dump(mode="python")["values"])))

    values = [bytes([i]) for i in range(16)]
    model = Model(values=container((value, i) if nested else value for i, value in enumerate(values)))
    assert from_value(to_value(model), Model) == model


@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
def test_buffer_channels_accept_plain_json_without_attachments(codec):
    from transports._buffers import join_buffers

    wire = protocol.patch_msg(1, {"rev": 0, "ops": [{"Set": {"path": [{"Key": "value"}], "value": {"Int": 1}}}]})
    assert protocol.decode(join_buffers(wire, codec=codec), codec) == protocol.decode(wire)


def test_binary_serializer_preserves_nested_serializers_and_model_configuration():
    from pydantic import ConfigDict, Field, RootModel, model_serializer
    from pydantic.dataclasses import dataclass as pydantic_dataclass

    class Child(BaseModel):
        model_config = ConfigDict(serialize_by_alias=True, populate_by_name=True)
        data: bytes = Field(alias="payload")

    class Wrapped(Child):
        @model_serializer(mode="wrap")
        def serialize(self, handler):
            return {**handler(self), "label": "wrapped"}

    @pydantic_dataclass
    class Record:
        data: bytes

    class Model(BaseModel):
        child: Child
        wrapped: Wrapped
        record: Record
        root: RootModel[bytes]

    child = Child(data=b"pcm")
    model = Model(child=child, wrapped=Wrapped(data=b"\xff"), record=Record(b"\xfe"), root=b"\x00")
    original = Child.__pydantic_serializer__
    value = to_value(model)["Map"]
    assert value["child"] == {"Map": {"payload": {"Bytes": "cGNt"}}}
    assert value["wrapped"] == {"Map": {"payload": {"Bytes": "/w=="}, "label": {"Str": "wrapped"}}}
    assert value["record"] == {"Map": {"data": {"Bytes": "/g=="}}}
    assert value["root"] == {"Bytes": "AA=="}
    assert Child.__pydantic_serializer__ is original
    assert child.model_dump(mode="json") == {"payload": "pcm"}
