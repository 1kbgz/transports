import json

import pytest

from transports import decode_as, encode, encode_as

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
