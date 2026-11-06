"""Native attachments for JSON control channels."""

import json
from typing import Any

from .transports import BufferFrame


def split_buffers(wire: str | bytes, codec: str = "json") -> tuple[str | dict, list[bytes]]:
    frame = BufferFrame.from_json(wire) if isinstance(wire, str) else BufferFrame.decode(wire, codec)
    buffers = [frame.buffer(index) for index in range(frame.count())]
    if not buffers and isinstance(wire, str):
        return wire, []
    return json.loads(frame.header), buffers


def join_buffers(wire: str | dict, buffers: Any = (), codec: str = "json") -> str | bytes:
    if isinstance(wire, str):
        if buffers:
            raise ValueError("unexpected buffers without a buffer header")
        return wire if codec == "json" else BufferFrame.from_json(wire).encode(codec)
    frame = BufferFrame(json.dumps(wire))
    for buffer in buffers or ():
        frame.push(bytes(buffer))
    return frame.to_json() if codec == "json" else frame.encode(codec)
