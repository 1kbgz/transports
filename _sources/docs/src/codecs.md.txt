# How to use codecs

This guide shows you how to encode values and protocol messages as JSON, MessagePack, or a registered
custom format.

## Encode a model value

`encode_as` and `decode_as` operate on a JSON string containing a core `Value`.

```python
import json
import transports
from pydantic import BaseModel

class Device(BaseModel):
    name: str
    on: bool = False

value = transports.to_value(Device(name="lamp", on=True))
blob = transports.encode_as(json.dumps(value), "application/msgpack")
restored = json.loads(transports.decode_as(blob, "application/msgpack"))

assert restored == value
```

`transports.encode(value_json)` and `transports.decode(bytes)` are JSON-codec shortcuts for model
values.

## Select a connection codec

Use JSON when you want readable text frames. Use MessagePack when you want compact binary frames.

```python
client = transports.Client(codec="msgpack")
await client.connect("ws://127.0.0.1:8000/ws")
```

```ts
const client = new Client("msgpack");
const ws = client.connect("ws://127.0.0.1:8000/ws");
```

The server stores the codec per connection. JSON and MessagePack clients can connect to the same
server, send edits, and receive frames encoded for their own connection.

## Convert whole protocol messages

Use `json_to_msgpack` and `msgpack_to_json` for whole snapshot or patch messages.

```python
msg = transports.protocol.snapshot_msg(1, "Device", 0, value)
wire = transports.json_to_msgpack(msg)
round_trip = transports.msgpack_to_json(wire)
```

## Register a custom Python codec

A custom codec maps a JSON-able object to `bytes` or `str`, and maps that wire value back to a
JSON-able object.

```python
import json
import zlib
import transports

def encode_zlib(obj):
    return zlib.compress(json.dumps(obj).encode())

def decode_zlib(data):
    return json.loads(zlib.decompress(bytes(data)).decode())

transports.register_codec("application/x-json-zlib", encode_zlib, decode_zlib)
```

Once registered, the content type works wherever a codec name is accepted:

```python
client = transports.Client(codec="application/x-json-zlib")
blob = transports.encode_as(json.dumps(value), "application/x-json-zlib")
```

Register the same content type on every process that needs to read or write it. Built-in JSON and
MessagePack names cannot be overridden.

## Register a custom JavaScript codec

```ts
import { registerCodec } from "transports";

registerCodec("application/x-json-zlib", encodeZlib, decodeZlib);
const client = new Client("application/x-json-zlib");
```

Custom codecs for browser WebSockets may return `string` or `Uint8Array`. A `string` is sent as a
text frame; bytes are sent as a binary frame.

## Remove a custom codec

```python
transports.unregister_codec("application/x-json-zlib")
print(transports.registered_codecs())
```
