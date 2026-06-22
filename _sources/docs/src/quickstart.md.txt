# Quickstart

In this tutorial, we will host one Python model, mirror it with a `Client`, send one server-side
change, and send one client proposal back to the server.

## Install

```bash
pip install transports
```

## Create a model

Create a file or Python session with this model:

```python
from pydantic import BaseModel

class Device(BaseModel):
    name: str
    on: bool = False
```

## Host it

Host the model in a `Session`. The returned id names this model on the wire.

```python
import transports

session = transports.Session()
lamp = Device(name="lamp")
mid = session.host(lamp)

print(mid)
# 1
```

Mutate the model and drain the session:

```python
lamp.on = True
print(session.drain())
# [(1, {'rev': 1, 'ops': [{'Set': {'path': [{'Key': 'on'}], 'value': {'Bool': True}}}]})]
```

Notice that only `on` is present in the patch.

## Mirror it with a client

Create an in-process server and client. This uses the same messages as a real WebSocket connection,
but no network is needed for the tutorial.

```python
server = transports.Server(session)
client = transports.Client()

for frame in server.open("browser"):
    client.recv(frame)

print(client.model(mid, Device))
# name='lamp' on=True
```

## Send another server-side change

Mutate the hosted model again, flush the server, and feed the outbound frame to the client.

```python
lamp.name = "desk lamp"

for frame in server.flush()["browser"]:
    client.recv(frame)

print(client.model(mid, Device))
# name='desk lamp' on=True
```

## Send a client proposal

A client edit is a proposal. The server applies it, assigns the revision, and echoes the
accepted patch back.

```python
next_value = transports.to_value(Device(name="desk lamp", on=False))
proposal = client.edit(mid, next_value)

for _conn, frames in server.recv("browser", proposal).items():
    for frame in frames:
        client.recv(frame)

print(lamp.on)
# False
print(client.model(mid, Device))
# name='desk lamp' on=False
```

You have now hosted a model, mirrored it, streamed a patch, and sent a client edit through the same
server-authoritative path used by live connections.

Next: use [model bridges](bridges.md) for dataclasses and msgspec, or [connections](connections.md)
for WebSocket, SSE, Jupyter comm, and anywidget adapters.
