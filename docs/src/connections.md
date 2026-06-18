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
client sends back is applied and relayed to the *other* clients, so the server acts as a hub.

## Python client

{py:class}`~transports.Client` mirrors a remote session without hosting it:

```python
client = transports.Client()
await client.connect("ws://localhost:8000/ws")   # mirrors until the connection closes
client.model(mid, Counter)                       # materialize the mirrored model as a Counter
```

`Client` is also usable without a live connection — feed it messages with `recv(text)` and read with
`value(id)` / `model(id, cls)`.

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

## Runnable example

A complete example — a Python server hosting a counter and a browser page that displays it updating
live — is in [`examples/`](https://github.com/1kbgz/transports/tree/main/examples):

```bash
pip install "transports[connections]" uvicorn
(cd js && pnpm build)        # build the wasm the page loads
python examples/server.py    # open http://127.0.0.1:8000
```
