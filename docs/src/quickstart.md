# Quickstart

## Install

```bash
pip install transports
```

## Host a model and react to changes

A {py:class}`~transports.Session` hosts models and turns their mutations into incremental patches.

```python
import transports
from pydantic import BaseModel

class Device(BaseModel):
    name: str
    on: bool = False

session = transports.Session()
lamp = Device(name="lamp")
mid = session.host(lamp)        # registers the schema, stores the value, watches the model

lamp.on = True                  # mutate normally — no .send(), no manual diff
patches = session.drain()       # -> [(mid, {'rev': 1, 'ops': [...]})]
```

`drain()` returns (and clears) every patch accumulated since the last drain. Each patch is the
**minimal** set of operations — flipping `on` produces a single `Set`, not a whole-model resend.

## Patches coalesce

Emission is deferred to a flush (triggered by `drain()`, `snapshot()`, or an explicit `flush()`), so
several writes between flushes collapse into one patch:

```python
lamp.on = False
lamp.name = "desk lamp"
session.drain()   # one patch, two ops
```

## Mirror a model

Patches are portable: apply them to a copy of the model hosted elsewhere (here, a second in-process
session; in a real app the patch travels over a connection).

```python
server = transports.Session()
lamp = Device(name="lamp")
sid = server.host(lamp)

client = transports.Session()
mirror = transports.from_value(server.snapshot(sid)["value"], Device)
cid = client.host(mirror)

lamp.on = True
for _mid, patch in server.drain():
    client.apply_patch(cid, patch)

assert transports.from_value(client.value(cid), Device).on is True
```

## Any model kind

The same API works for dataclasses and msgspec structs — see [Model bridges](bridges.md).

```python
from dataclasses import dataclass

@dataclass
class Reading:
    sensor: str
    value: float = 0.0

s = transports.Session()
r = Reading(sensor="temp")
s.host(r)
r.value = 21.5
s.drain()   # [(mid, {'rev': 1, 'ops': [{'Set': {'path': [{'Key': 'value'}], 'value': {'Float': 21.5}}}]})]
```
