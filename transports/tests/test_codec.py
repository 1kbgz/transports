import json

import pytest
from pydantic import BaseModel

from transports import (
    Client,
    Server,
    Session,
    decode_as,
    encode,
    encode_as,
    json_to_msgpack,
    msgpack_to_json,
    protocol,
    register_codec,
    registered_codecs,
    unregister_codec,
)

MODEL = json.dumps({"Map": {"name": {"Str": "lamp"}, "on": {"Bool": True}, "count": {"Int": 123456}}})


def test_msgpack_round_trip():
    blob = encode_as(MODEL, "application/msgpack")
    assert isinstance(blob, bytes)
    assert json.loads(decode_as(blob, "application/msgpack")) == json.loads(MODEL)


def test_msgpack_is_smaller_than_json():
    assert len(encode_as(MODEL, "application/msgpack")) < len(encode(MODEL))


def test_json_via_encode_as_matches_encode():
    assert encode_as(MODEL, "application/json") == encode(MODEL)


def test_unknown_codec_raises():
    with pytest.raises(ValueError):
        encode_as(MODEL, "application/protobuf")


# --- whole-message JSON <-> msgpack (protocol framing) -------------------------------------------

MSG = json.dumps({"t": "patch", "id": 7, "patch": {"rev": 2, "ops": []}})


def test_json_msgpack_message_round_trip():
    blob = json_to_msgpack(MSG)
    assert isinstance(blob, bytes)
    assert json.loads(msgpack_to_json(blob)) == json.loads(MSG)


def test_protocol_encode_decode_json():
    wire = protocol.encode(MSG, protocol.JSON)
    assert isinstance(wire, str)
    assert protocol.decode(wire) == json.loads(MSG)


def test_protocol_encode_decode_msgpack():
    wire = protocol.encode(MSG, protocol.MSGPACK)
    assert isinstance(wire, bytes)
    assert protocol.decode(wire) == json.loads(MSG)


def test_normalize_codec_aliases():
    assert protocol.normalize_codec("application/msgpack") == protocol.MSGPACK
    assert protocol.normalize_codec(None) == protocol.JSON
    with pytest.raises(ValueError):
        protocol.normalize_codec("application/protobuf")


# --- custom codec registration -------------------------------------------------------------------

CUSTOM = "application/x-test"


def _enc(obj):  # toy custom *binary* codec: a 1-byte marker + utf-8 JSON
    return b"X" + json.dumps(obj).encode()


def _dec(data):
    return json.loads(bytes(data)[1:])


class _Device(BaseModel):
    name: str
    on: bool = False


@pytest.fixture
def custom_codec():
    register_codec(CUSTOM, _enc, _dec)
    yield CUSTOM
    unregister_codec(CUSTOM)


def test_register_and_normalize(custom_codec):
    assert CUSTOM in registered_codecs()
    assert protocol.normalize_codec(CUSTOM) == CUSTOM


def test_cannot_override_builtin():
    with pytest.raises(ValueError):
        register_codec("application/json", _enc, _dec)


def test_unregister_makes_codec_unknown():
    register_codec(CUSTOM, _enc, _dec)
    unregister_codec(CUSTOM)
    assert CUSTOM not in registered_codecs()
    with pytest.raises(ValueError):
        protocol.normalize_codec(CUSTOM)


def test_custom_codec_protocol_round_trip(custom_codec):
    wire = protocol.encode(MSG, custom_codec)
    assert isinstance(wire, bytes) and wire[:1] == b"X"
    assert protocol.decode(wire, custom_codec) == json.loads(MSG)


def test_custom_codec_via_encode_as(custom_codec):
    blob = encode_as(MODEL, custom_codec)
    assert isinstance(blob, bytes)
    assert json.loads(decode_as(blob, custom_codec)) == json.loads(MODEL)


def test_custom_codec_end_to_end_over_server(custom_codec):
    session = Session()
    server = Server(session)
    d = _Device(name="lamp")
    mid = session.host(d)
    client = Client(codec=custom_codec)

    for m in server.open("c", codec=custom_codec):
        assert isinstance(m, bytes)  # our codec frames as binary
        client.recv(m)
    assert client.model(mid, _Device) == d

    d.on = True
    for m in server.flush()["c"]:
        client.recv(m)
    assert client.model(mid, _Device).on is True
