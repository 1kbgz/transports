# Codecs

The wire format is pluggable. A **codec** turns a `Value` (or a patch) into bytes and back.
transports ships two, both self-describing — they need no schema, and produce identical bytes from
Python and JavaScript:

| Codec | Content type | |
|---|---|---|
| JSON | `application/json` | human-readable; the default |
| MessagePack | `application/msgpack` | compact binary |

## Encoding

`encode_as` / `decode_as` take a content type. They operate on the core `Value` in its JSON text
form (what {py:func}`~transports.to_value` produces, serialized):

```python
import json
import transports

value = transports.to_value(Device(name="lamp", on=True))     # a Value (dict)
blob = transports.encode_as(json.dumps(value), "application/msgpack")   # compact bytes
restored = json.loads(transports.decode_as(blob, "application/msgpack"))
assert restored == value
```

`transports.encode(value)` / `transports.decode(bytes)` are JSON shortcuts. In JavaScript the same
pair is `encodeAs(value, codec)` / `decodeAs(bytes, codec)`.

Because a codec only changes the *bytes* — never the model, the `Value`, or the patch semantics —
switching formats is a one-line change and never touches your models. MessagePack is typically the
choice for production traffic; JSON is convenient for debugging.

## On a connection

A connection negotiates its codec independently, so JSON and MessagePack clients can share one
server. Pass `codec=` when opening a client; over WebSocket the choice rides on a `?codec=` query
param, and JSON travels as text frames while MessagePack travels as binary frames.

```python
from transports import Client

await Client(codec="msgpack").connect("ws://localhost:8000/ws")   # appends ?codec=msgpack
```

```javascript
new Client("msgpack").connect("ws://localhost:8000/ws");
```

The server encodes every outbound message in *that* connection's codec and decodes inbound frames by
type, so a MessagePack client's edit is transparently relayed to a JSON client and vice versa — see
[Connections](connections.md). Whole protocol messages (not just model values) are converted with
`json_to_msgpack` / `msgpack_to_json` (`jsonToMsgpack` / `msgpackToJson` in JavaScript).

## Registering your own codec

Beyond the built-ins, you can register a codec under any content type. A codec is just a pair of
functions over a JSON-able object — a protocol message or a model `Value`:

```python
import transports

transports.register_codec(
    "application/protobuf",
    encode=lambda obj: my_proto_encode(obj),   # object -> bytes (or str)
    decode=lambda data: my_proto_decode(data),  # bytes (or str) -> object
)
```

Once registered, the content type works anywhere a codec name is accepted — `Client(codec=...)`, a
`?codec=` query param, and {py:func}`~transports.encode_as` / {py:func}`~transports.decode_as`. A
codec returning `bytes` travels as a binary frame; returning `str` travels as a text frame. The
built-in `json` / `msgpack` codecs cannot be overridden.

Register the matching implementation in every binding that participates. JavaScript has its own
`registerCodec`:

```javascript
import { registerCodec, Client } from "transports";

registerCodec("application/protobuf", encode, decode);
new Client("application/protobuf").connect("ws://localhost:8000/ws");
```

```{note}
JSON and MessagePack are *self-describing*, so they encode the dynamic `Value` with no schema.
Schema-driven formats such as Protobuf or FlatBuffers need a descriptor per model — your `encode` /
`decode` are where that mapping lives.
```
