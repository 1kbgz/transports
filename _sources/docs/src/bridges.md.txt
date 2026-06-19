# Model bridges

transports speaks to three Python model libraries through one contract, plus plain objects in
JavaScript. A *bridge* does three things: convert an instance to/from the core
[`Value`](concepts.md), derive a core schema from the class, and (for reactive hosting) observe
mutations.

## Conversion

`to_value` / `from_value` move an instance to and from the wire form:

```python
import transports
from pydantic import BaseModel

class Device(BaseModel):
    name: str
    on: bool = False

v = transports.to_value(Device(name="lamp", on=True))
# {'Map': {'name': {'Str': 'lamp'}, 'on': {'Bool': True}}}

transports.from_value(v, Device)
# Device(name='lamp', on=True)
```

The same calls work for dataclasses and `msgspec.Struct`s. Conversion routes JSON-native python
through each library's serializer (pydantic `model_dump(mode="json")`, otherwise msgspec), so rich
field types (datetimes, enums, …) are normalized for you.

## Reactive observation

Hosting a model with a {py:class}`~transports.Session` installs a mutation watcher (via
[bigbrother](https://github.com/1kbgz/bigbrother)). Watching is **recursive** — nested models, lists,
and dicts are observed too — so deep edits produce nested-path patches:

```python
session = transports.Session()
d = Device(name="lamp")
session.host(d)

d.on = True
session.drain()
# [(mid, {'rev': 1, 'ops': [{'Set': {'path': [{'Key': 'on'}], 'value': {'Bool': True}}}]})]
```

Observation marks the model dirty; the patch is computed on the next `flush()` (which `drain()` and
`snapshot()` call for you). Deferring to a flush is what makes writes coalesce.

### pydantic and dataclasses: automatic

Both store their state in `__dict__`, so the watcher fires on every assignment — including nested
mutations like `d.meta.tags.append("x")`. Just mutate and `drain()`.

### msgspec: explicit `update()`

`msgspec.Struct`s use `__slots__` and have no `__dict__`, so they can't be observed automatically.
Mutate them and call {py:meth}`~transports.Session.update` to emit:

```python
import msgspec

class Reading(msgspec.Struct):
    sensor: str
    value: float = 0.0

session = transports.Session()
r = Reading(sensor="temp")
mid = session.host(r)

r.value = 21.5
session.drain()         # [] — not observed automatically
session.update(mid)     # [(mid, {'rev': 1, 'ops': [...]})]
```

## Schemas, and one schema for both languages

`schema_of` derives a core schema from any supported class; `schema_to_ts` renders that schema as a
TypeScript interface — one definition, both languages:

```python
schema = transports.schema_of(Device)
# {'type_name': 'Device', 'fields': [{'name': 'name', 'ty': 'Str'}, {'name': 'on', 'ty': 'Bool'}]}

print(transports.schema_to_ts(schema))
# export interface Device {
#   name: string;
#   on: boolean;
# }
```

## JavaScript: plain objects

On the JS side, the bridge maps plain objects to and from the same `Value` wire form:

```ts
import { toValue, fromValue } from "transports";

const v = toValue({ name: "lamp", on: true });
// { Map: { name: { Str: "lamp" }, on: { Bool: true } } }
fromValue(v); // { name: "lamp", on: true }
```

Because both bridges target the same core `Value`, an object bridged in JavaScript and a model
bridged in Python are interchangeable on the wire.

> [!NOTE]
> JavaScript has a single number type, so integers and whole-valued floats both encode as `Int`.

## Notes

- Nested models are inlined as `Map`s in the wire form.
- On the receive side, `apply_patch` updates the mirrored core value; read it back with
  `from_value(session.value(id))`.
