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

To send an edit, propose it over the active connection — `connect()`/`run()` own the socket, so no
socket handling is needed:

```ts
import { toValue } from "1kbgz/transports";

const [id] = client.ids();
client.propose(id, toValue({ tick: 10 })); // send(edit(id, value)) over the managed connection
```

The local mirror updates when the server echoes the authoritative patch (or `onReject` fires with
why it was refused). `client.send(frame)` sends any pre-built frame the same way — it is what an
adapter hands its send callback, e.g. spaday's `connectStore(store, client, (f) => client.send(f),
codec)`. Both return `false` and drop the frame when no managed connection is open (matching a
browser WebSocket's send on a closed socket, so they are safe fire-and-forget callbacks even across
`run()` reconnect gaps); check `client.connected` (or the return value) when delivery matters. With
a hand-rolled socket, send `client.edit(id, value)` yourself as before.

## Mirror the server in Python

`Client.connect()` runs a receive loop until the WebSocket closes.

```python
client = transports.Client()
await client.connect("ws://127.0.0.1:8000/ws")
```

Edits work the same as in JS: `await client.propose(mid, value)` (or `await client.send(frame)`)
rides the active `connect()`/`run()` connection — native or Pyodide — returning `False` (dropped)
when there is none, with `client.connected` to check first; with a hand-rolled socket, send
`client.edit(mid, value)` yourself.

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

For the common case, `transports.widget(server)` is turnkey: it builds an `anywidget.AnyWidget`
whose frontend ships inside the wheel — display it and every hosted model mirrors live, with
`transports-change` / `transports-reject` DOM events and a wasm-free `el.transports.edit` for
proposals. See [Pyodide](pyodide.md) for details.

```python
w = transports.widget(server)   # pip install anywidget
w                               # display; then mutate models + transports.sync(server)
```

For a custom frontend, `serve_anywidget` wires any anywidget-style `send` / `on_msg` object — you
supply the `_esm`. The frontend sends `{"ready": true}` before snapshots are delivered.

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
