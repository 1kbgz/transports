import sys

import pytest
from pydantic import BaseModel

from transports import Session, from_value
from transports.protocol import decode, encode, patch_msg

pytestmark = pytest.mark.skipif(sys.platform != "emscripten", reason="requires Pyodide")


class Device(BaseModel):
    name: str
    enabled: bool = False


def test_session_patch_round_trip() -> None:
    server = Session()
    server_device = Device(name="lamp")
    server_id = server.host(server_device)

    client = Session()
    client_device = from_value(server.snapshot(server_id)["value"], Device)
    client_id = client.host(client_device)

    server_device.enabled = True
    _, patch = server.drain()[0]
    wire = encode(patch_msg(client_id, patch), "msgpack")
    message = decode(wire, "msgpack")

    assert client.apply_patch(message["id"], message["patch"])
    assert client_device.enabled is True
    assert client.snapshot(client_id)["rev"] == 1
