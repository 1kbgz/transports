# Connections

Patches travel between processes over a **WebSocket**. A server hosts a
{py:class}`~transports.Session` and broadcasts its patches; a client — in Python or the browser —
mirrors the model live.

```bash
pip install "transports[connections]"
```

## Server

A {py:class}`~transports.Server` wraps a `Session`. Its logic is transport-agnostic — it returns the
messages to send — and {py:func}`~transports.starlette_endpoint` adapts it to a Starlette WebSocket
route. Run {py:func}`~transports.autoflush` as a background task to stream server-side changes to
connected clients.

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
        counter.tick += 1               # mutate the model; clients receive the patch

async def startup():
    asyncio.create_task(transports.autoflush(server))
    asyncio.create_task(ticker())

app = Starlette(
    routes=[WebSocketRoute("/ws", transports.starlette_endpoint(server))],
    on_startup=[startup],
)
```

On connect, a client receives a snapshot of every hosted model, then a stream of patches. A patch a
client sends back is applied and broadcast, so the server acts as a hub.

### Server-authoritative edits

Models are **server-authoritative**: a client's `edit(id, value)` is a *proposal*. The server applies
it, owns the revision (`rev`), and echoes the authoritative patch to **every** connection — including
the one that sent it. A client's local mirror therefore updates when that echo arrives, not
optimistically, so all mirrors and the server's own hosted object stay in lock-step without `rev`
drift. (For multi-writer convergence on shared models, see [Multi-tenancy](multitenancy.md).)

### Choosing a codec

Each connection negotiates its wire format with a `?codec=` query param (`json`, the default, or
`msgpack`). The server tracks it per connection and encodes every outbound message to match — JSON
as text frames, MessagePack as binary frames — so JSON and MessagePack clients can share one server
and still exchange edits. See [Codecs](codecs.md).

## Python client

{py:class}`~transports.Client` mirrors a remote session without hosting it:

```python
client = transports.Client()                     # or Client(codec="msgpack")
await client.connect("ws://localhost:8000/ws")   # mirrors until the connection closes
client.model(mid, Counter)                       # materialize the mirrored model as a Counter
```

`Client` is also usable without a live connection — feed it messages with `recv(data)` (a text or
binary frame) and read with `value(id)` / `model(id, cls)`.

## Browser client

The JavaScript `Client` applies patches with the wasm core (initialize the wasm first):

```ts
import init, { Client } from "transports";
await init();

const client = new Client();
const ws = new WebSocket("ws://localhost:8000/ws");
ws.addEventListener("message", (e) => {
  client.recv(e.data);
  render(client.value(1)); // your render function
});
```

## Server-Sent Events (receive-only)

For receive-mostly UIs (dashboards), a server can push over **SSE** — a one-way server→client stream.
{py:func}`~transports.sse_endpoint` builds a Starlette route; run {py:func}`~transports.autoflush` to
stream changes. Clients receive snapshots and patches but do not send edits back (use WebSocket for
that).

```bash
pip install "transports[sse]"
```

```python
from starlette.routing import Route

app = Starlette(
    routes=[Route("/sse", transports.sse_endpoint(server))],
    on_startup=[lambda: asyncio.create_task(transports.autoflush(server))],
)
```

```python
client = transports.Client()
await client.connect_sse("http://localhost:8000/sse")   # mirrors until the stream closes
```

In the browser, the JavaScript `Client` mirrors a stream with `connectSSE(url)` (native
`EventSource`).

## Jupyter (comm)

Inside a kernel, a model syncs to the frontend over a **Jupyter comm** — the same channel ipywidgets
use. {py:func}`~transports.serve_comm` wires a comm (the connection handle) to a `Server`/`Hub`;
{py:func}`~transports.pump_comms` pushes host-side changes after you mutate models.

```bash
pip install "transports[jupyter]"
```

```python
from comm import create_comm

comm = create_comm(target_name="transports")
transports.serve_comm(server, comm)   # send snapshots, relay inbound edits
# ... mutate hosted models ...
transports.pump_comms(server)          # push patches to the frontend
```

The comm carries the same messages, so the browser `Client` mirrors it unchanged.

## Runnable example

A complete example — a Python server hosting a counter and a browser page that displays it updating
live — is in [`examples/`](https://github.com/1kbgz/transports/tree/main/examples):

```bash
pip install "transports[connections]" uvicorn
(cd js && pnpm build)        # build the wasm the page loads
python examples/server.py    # open http://127.0.0.1:8000
```
