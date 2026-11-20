# How to use codecs

This guide shows you how to encode values and protocol messages as JSON, MessagePack, CBOR, or a
registered custom format. Codecs select the wire encoding independently of the
[model type](bridges.md).

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
values. Use `application/cbor` for CBOR:

```python
blob = transports.encode_as(json.dumps(value), "application/cbor")
restored = json.loads(transports.decode_as(blob, "application/cbor"))
assert restored == value
```

## Select a connection codec

Use JSON when you want readable text frames. Use MessagePack or CBOR for binary frames.

```python
client = transports.Client(codec="cbor")
await client.connect("ws://127.0.0.1:8000/ws")
```

```ts
const client = new Client("cbor");
const ws = client.connect("ws://127.0.0.1:8000/ws");
```

Use `codec="json"` or `codec="msgpack"` in Python, or `new Client("json")` or `new Client("msgpack")`
in JavaScript, to select the other built-in formats. The server stores the codec per connection.
JSON, MessagePack, and CBOR clients can connect to the same WebSocket server, send edits, and receive
frames encoded for their own connection. See the [protocol reference](protocol-reference.md#built-in-codecs)
for other connection adapters' codec restrictions.

## Convert whole protocol messages

Use `json_to_msgpack` and `msgpack_to_json`, or `json_to_cbor` and `cbor_to_json`, for snapshot or patch messages.

```python
msg = transports.protocol.snapshot_msg(1, "Device", 0, value)
wire = transports.json_to_msgpack(msg)
round_trip = transports.msgpack_to_json(wire)

wire = transports.json_to_cbor(msg)
round_trip = transports.cbor_to_json(wire)
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

Use the registered content type with a WebSocket client or `encode_as` / `decode_as`:

```python
client = transports.Client(codec="application/x-json-zlib")
blob = transports.encode_as(json.dumps(value), "application/x-json-zlib")
```

Register matching encoder and decoder implementations under the same content type in each Python
or JavaScript process that uses it. Registration is local to that process. Built-in JSON,
MessagePack, and CBOR names cannot be overridden.

## Register a custom JavaScript codec

```ts
import { Client, registerCodec } from "@1kbgz/transports";

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
