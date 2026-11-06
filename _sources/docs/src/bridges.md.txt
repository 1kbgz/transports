# How to bridge models and objects

This guide shows you how to convert supported Python models and JavaScript objects to the core
`Value` form, host them in a `Session`, and generate TypeScript interfaces from Python types.

## Use pydantic models

```python
import transports
from pydantic import BaseModel

class Device(BaseModel):
    name: str
    on: bool = False

model = Device(name="lamp")
value = transports.to_value(model)
restored = transports.from_value(value, Device)

assert restored == model
```

Pydantic models are observed automatically when hosted:

```python
session = transports.Session()
mid = session.host(model)
model.on = True
patches = session.drain()
```

## Use dataclasses

```python
from dataclasses import dataclass
import transports

@dataclass
class Reading:
    sensor: str
    value: float = 0.0

reading = Reading(sensor="temp")
session = transports.Session()
session.host(reading)

reading.value = 21.5
print(session.drain())
```

Dataclasses are observed automatically, including nested lists, dicts, and model fields.

## Use msgspec structs

`msgspec.Struct` instances use slots, so automatic mutation watching is not available. Mutate the
struct and call `Session.update(id)`.

```python
import msgspec
import transports

class Reading(msgspec.Struct):
    sensor: str
    value: float = 0.0

reading = Reading(sensor="temp")
session = transports.Session()
mid = session.host(reading)

reading.value = 21.5
print(session.drain())
# []

print(session.update(mid))
# [(1, {'rev': 1, 'ops': [...]})]
```

## Convert values manually

Use `to_value` when a lower-level API expects the core wire value. Use `from_value` to materialize a
mirrored value as a Python model.

```python
value = transports.to_value(Device(name="lamp", on=True))
# {'Map': {'name': {'Str': 'lamp'}, 'on': {'Bool': True}}}

model = transports.from_value(value, Device)
```

## Generate TypeScript from a Python model

```python
schema = transports.schema_of(Device)
print(transports.schema_to_ts(schema))
```

Output:

```ts
export interface Device {
  name: string;
  on: boolean;
}
```

## Bridge plain JavaScript objects

```ts
import { fromValue, toValue } from "1kbgz/transports";

const value = toValue({ name: "lamp", on: true });
// { Map: { name: { Str: "lamp" }, on: { Bool: true } } }

const object = fromValue(value);
// { name: "lamp", on: true }
```

Use the JavaScript bridge before calling `Client.edit`, because edits compare core `Value` objects:

```ts
ws.send(client.edit(id, toValue({ name: "lamp", on: false })));
```

## Transfer binary fields

Declare binary data as `bytes` on pydantic models, dataclasses, or msgspec structs:

```python
class Sample(BaseModel):
    data: bytes = b""

sample = Sample(data=bytes([0, 255, 1]))
mid = session.host(sample)
```

Use `Client(codec="msgpack")` or `Client(codec="cbor")` for native binary WebSocket payloads.
Comm and anywidget carry these bytes in separate message buffers automatically. JSON/SSE uses
base64. The input bridge also accepts `bytearray` and `memoryview`.

In JavaScript, pass a `Uint8Array` or another `ArrayBufferView` through `toValue`. `fromValue`
returns a `Uint8Array`, preserving the underlying buffer when a native view is available.
`Client` handles wire encoding. For a low-level JSON API, use `valueToJson(toValue(data))`;
ordinary `JSON.stringify` does not encode typed arrays as the protocol's base64 byte strings.

## Let the frontend own status

Mark browser-derived status separately from server-controlled fields:

```python
from pydantic import Field

class Clock(BaseModel):
    is_playing: bool = Field(False, json_schema_extra={"owner": "client"})
    bpm: int = Field(120, ge=1)

clock = Clock()
session = transports.Session()
mid = session.host(clock)
server = transports.Server(session)
session.command(mid, "is_playing", True)  # retained until an owner connects and acknowledges
```

For dataclasses, use `field(metadata={"owner": "client"})`. To make every field client-owned, pass
`session.host(model, owner="client")`. Field metadata overrides that model default.

Handle commands in the browser and report the resulting status:

```ts
const client = new Client("msgpack", { updateInterval: 0.25 });
client.onCommand((command) => {
  const playing = fromValue(command.value);
  // Apply playing to the browser's clock here, then report its resulting state.
  client.proposeOps(command.id, [{
    Set: { path: [{ Key: command.field }], value: toValue(playing) },
  }]);
  client.ackCommand(command);
});
client.connect(url);
```

Use `client.owns(id, "is_playing")` before driving local status, and `onOwnership` to start or stop
that work as ownership changes. Stop local work on `onDisconnect` as well; disconnection revokes
all grants. Python clients expose matching `on_command`, `on_ownership`,
`propose_ops`, and `ack_command` methods; managed sends are awaited. Commands received before a
listener is registered are retained. Acknowledge only after applying a command successfully. Managed `ack_command` / `ackCommand`
flush pending owned status before sending the acknowledgement; manual `command_ack` / `commandAck`
callers must send that status first.

Assign `clock.bpm` normally. Assigning client-owned fields from Python raises; use `session.command`
instead. For unobservable models such as msgspec structs, `Session.update` detects and restores a
forbidden assignment before raising. Nested Python mutations are restored with a warning on flush;
idle flushes do not serialize owned values. Client-owned status preserves annotated Python types but
bypasses field constraints and model validators. Invalid type representations are rejected before
changing the hosted state. Use ownership for values whose authority belongs to the frontend; keep
security-sensitive validation on server-owned fields. Server edits still run model-level validators,
including cross-field checks. Those checks must tolerate every valid owner state, or unrelated server
edits can be rejected until the owner changes that state. Keep independent owner status in a separate
model when a cross-field constraint cannot tolerate it.

Ownership grants and client paths use serialized field names, including aliases. Python assignment
and `Session.command` use attribute names. Changes spanning both ownership kinds must be sent as
separate edits; `propose` does not split a whole-model replacement automatically.

Owned nested msgspec structs with `__post_init__` need an existing instance to copy. Adding a new
such instance through an owned update is unsupported, because constructing it would run that hook.

Command delivery may repeat after handover. See the [protocol reference](protocol-reference.md)
for delivery and memory limits. Set `Client(ownership=False)` in Python or
`new Client("json", { ownership: false })` in JavaScript for a passive WebSocket observer.

## Current bridge limits

Nested Python models are currently inlined as `Map` values. The Rust core has a `Submodel` value,
but the Python and JavaScript bridges do not yet emit submodel-by-id references. Inlining loses
identity: the same instance referenced from two places syncs as two independent maps, and cycles
are unsupported. Type fidelity is kept. A polymorphic nested model round-trips to its concrete
subclass whenever its dump carries the discriminator. On pydantic 2.13+ the bridge dumps
polymorphically by default, so a subclass instance in a base-annotated field keeps all its fields
on the wire. Pydantic discriminated unions and ccflow-style `type_` dispatch both work with no
annotations. On older pydantic, dumps follow the field's annotation and subclass-only fields are
dropped. Opt in per field with `SerializeAsAny[Base]` (ccflow's own `BaseModel` applies that to
every field through its metaclass).

JavaScript has one number type. Whole-valued JavaScript numbers encode as `Int`; other numbers
encode as `Float`.
