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
