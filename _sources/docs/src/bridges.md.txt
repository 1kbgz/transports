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
import { fromValue, toValue } from "transports";

const value = toValue({ name: "lamp", on: true });
// { Map: { name: { Str: "lamp" }, on: { Bool: true } } }

const object = fromValue(value);
// { name: "lamp", on: true }
```

Use the JavaScript bridge before calling `Client.edit`, because edits compare core `Value` objects:

```ts
ws.send(client.edit(id, toValue({ name: "lamp", on: false })));
```

## Current bridge limits

Nested Python models are currently inlined as `Map` values. The Rust core has a `Submodel` value,
but the Python and JavaScript bridges do not yet emit submodel-by-id references.

JavaScript has one number type. Whole-valued JavaScript numbers encode as `Int`; other numbers
encode as `Float`.
