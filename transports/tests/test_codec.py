import json

import pytest

from transports import decode_as, encode, encode_as, json_to_msgpack, msgpack_to_json, protocol

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
