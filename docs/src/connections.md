# How to connect live clients

This guide shows you how to serve a `Session` or `Hub` over the connection adapters transports ships
today: WebSocket, Server-Sent Events, Jupyter comm, and anywidget custom messages.

## Serve a session over WebSocket

Install the WebSocket dependencies:

```bash
pip install "transports[connections]" uvicorn
```

Create a Starlette app:

```python
import asyncio

import transports
from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute

class Counter(BaseModel):
    tick: int = 0

session = transports.Session()
counter = Counter()
session.host(counter)
server = transports.Server(session)

async def ticker():
    while True:
        await asyncio.sleep(1)
        counter.tick += 1

async def startup():
    asyncio.create_task(transports.autosync(server))
    asyncio.create_task(ticker())

app = Starlette(
    routes=[WebSocketRoute("/ws", transports.ws_endpoint(server))],
    on_startup=[startup],
)
```

Run it:

```bash
uvicorn app:app --reload
```

Run one `autosync` task per `Server` or `Hub`. It drains host-side mutations and broadcasts the
resulting patches to all open connections.

## Mirror the server in a browser

Initialize the wasm package, connect, and render whenever a message arrives.

```ts
import init, { Client, fromValue } from "1kbgz/transports";

await init();

const client = new Client();
const ws = client.connect("ws://127.0.0.1:8000/ws");

ws.addEventListener("message", () => {
  const [id] = client.ids();
  if (id === undefined) return;
  render(fromValue(client.value(id)));
});
```

To send an edit, create the next core `Value` and send the encoded proposal frame:

```ts
import { toValue } from "1kbgz/transports";

const [id] = client.ids();
ws.send(client.edit(id, toValue({ tick: 10 })));
```

The local mirror updates when the server echoes the authoritative patch.

## Mirror the server in Python

`Client.connect()` runs a receive loop until the WebSocket closes.

```python
client = transports.Client()
await client.connect("ws://127.0.0.1:8000/ws")
```

For Python clients that also send edits, manage the WebSocket loop directly and send the frame
returned by `client.edit(id, value)`.

## Use MessagePack on a connection

Pass `codec="msgpack"` on the client. The client appends `?codec=msgpack`; the server sends binary
frames to that connection and can still serve JSON clients at the same time.

```python
client = transports.Client(codec="msgpack")
await client.connect("ws://127.0.0.1:8000/ws")
```

```ts
const client = new Client("msgpack");
const ws = client.connect("ws://127.0.0.1:8000/ws");
```

## Stream receive-only updates over SSE

Use SSE for dashboards and other receive-only clients.

```bash
pip install "transports[sse]"
```

```python
import asyncio
from starlette.applications import Starlette
from starlette.routing import Route

async def startup():
    asyncio.create_task(transports.autosync(server))

app = Starlette(
    routes=[Route("/sse", transports.sse_endpoint(server))],
    on_startup=[startup],
)
```

Python client:

```python
client = transports.Client()
await client.connect_sse("http://127.0.0.1:8000/sse")
```

Browser client:

```ts
const client = new Client();
const events = client.connectSSE("http://127.0.0.1:8000/sse");
```

SSE is JSON/text and server-to-client only. Use WebSocket when clients need to send edits.

## Use a Jupyter comm

Install the comm dependency:

```bash
pip install "transports[jupyter]"
```

Wire a kernel comm to a `Server` or `Hub`:

```python
from comm import create_comm

comm = create_comm(target_name="transports")
transports.serve_comm(server, comm)

# after mutating hosted models
transports.sync(server)
```

The comm carries JSON wire strings in `data`, so `serve_comm` rejects non-JSON codecs.

## Use anywidget custom messages

`serve_anywidget` uses an anywidget-style `send` / `on_msg` object. The frontend sends
`{"ready": true}` before snapshots are delivered.

```python
conn = transports.serve_anywidget(server, widget)

# after mutating hosted models
transports.sync(server)
```

Frontend messages use the same client protocol:

```ts
const client = new Client();

model.on("msg:custom", (content) => {
  if (content.wire) client.recv(content.wire);
});

model.send({ ready: true });
```

Use `model.send({ wire: client.edit(id, value) })` to send an edit from the frontend.

## Serve a Hub

A `Hub` satisfies the same connection contract as `Server`, so the same adapters serve it:
`transports.ws_endpoint(hub)` for WebSocket, `transports.sse_endpoint(hub)` for SSE, and the same
`serve_comm` / `serve_anywidget` helpers for Jupyter (with `autosync(hub)` or `sync(hub)`).

```python
hub = transports.Hub(key=lambda ws: ws.path_params["tenant"])
app = Starlette(routes=[WebSocketRoute("/ws/{tenant}", transports.ws_endpoint(hub))])
```
