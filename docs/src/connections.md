# How to connect live clients

This guide shows you how to serve a `Session` or `Hub` over the connection adapters transports ships
today: WebSocket, WebRTC data channels, Server-Sent Events, Jupyter comm, and anywidget custom
messages.

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

To send an edit, propose it over the active connection. `connect()` and `run()` own the socket, so
you do not handle it yourself:

```ts
import { toValue } from "1kbgz/transports";

const [id] = client.ids();
client.propose(id, toValue({ tick: 10 }), "tick-1");
```

The local mirror updates when the server echoes the authoritative patch, or `onReject` fires with
the reason the edit was refused. `edit` assigns a proposal identifier; pass one explicitly when an
adapter needs to match pending state. `onAck` receives the authoritative patch carrying that
identifier, and rejects include it in `onReject`. Use `proposeOps` on a managed connection, or
`editOps` with a hand-rolled socket, to send explicit operations when a whole-value diff would be
empty against the current mirror. Generated identifiers use the reserved `auto-N` form; explicit
identifiers matching that form are rejected so the two sources cannot collide.

`onConnect` fires whenever a managed duplex connection opens, including each `run()` reconnect and
custom channels attached with `attach()`. It fires after `client.connected` becomes true and does
not depend on the server sending a snapshot or patch.
Use it with `onDisconnect` when an adapter needs connection status. Python names the hooks
`on_connect` and `on_disconnect`. These hooks do not apply to receive-only `connectSSE` /
`connect_sse` streams.

Proposal correlation lasts for one live connection. If it drops, `onAbandon` receives the unsettled
proposal identifiers before `onDisconnect` fires. Python exposes the same hook as `on_abandon`.
`pendingProposals()` / `pending_proposals()` returns the current set. Resume replays authoritative
state without old proposal identifiers; it does not resend those proposals.

`client.send(frame)` sends any pre-built frame the same way. It is
what an adapter hands its send callback, for example spaday's
`connectStore(store, client, (f) => client.send(f), codec)`. Both return `false` and drop the frame
when no managed connection is open. This matches a browser WebSocket's send on a closed socket, so
they are safe as fire-and-forget callbacks even across `run()` reconnect gaps. Check
`client.connected` (or the return value) when delivery matters. With a hand-rolled socket, send
`client.edit(id, value)` yourself as before.

An adapter can also make its channel the client's managed connection. Attach its sender, feed
received frames to `recv`, and detach the same sender object when the channel closes:

```ts
channel.binaryType = "arraybuffer";
const sender = (frame: string | Uint8Array) => channel.send(frame);
client.attach(sender);
channel.onmessage = ({ data }) =>
  client.recv(typeof data === "string" ? data : new Uint8Array(data));
channel.onclose = () => client.detach(sender);
```

```python
sender = channel.send
await client.attach(sender)
try:
    async for frame in channel:
        client.recv(frame)
finally:
    client.detach(sender)
```

`attach` flushes queued CRDT operations. Attaching a replacement first disconnects the old channel,
which abandons its unsettled ordinary proposals but retains CRDT operations for the replacement.
`detach` checks sender identity, so a late close from the old channel cannot clear the replacement.
If the initial CRDT flush fails, `attach` detaches the failed channel and reports the send error.

If a hand-rolled sender fails after `edit` or `editOps` creates a proposal, call
`client.abandonProposal(proposal)` to remove it from the JavaScript client's pending set. Python
provides `client.abandon_proposal(proposal)`. The methods return whether the proposal was pending;
they do not fire the disconnect-only `onAbandon` or `on_abandon` callbacks.

CRDT-backed shared models use `proposeCrdt(id, mutations)` or `editCrdt(id, mutations)`. These edits
are optimistic: the client applies them locally, retains their causally identified operations while
offline, and resends them after reconnect. An authoritative echo clears the matching operations.
Unlike ordinary patch proposals, CRDT operations survive a connection drop because resending one
causal dot is safe. Use `pendingCrdtOps(id)` to inspect the outbox.

## Publish ephemeral awareness

Awareness carries transient state for a shared `Hub` model. It is suitable for cursor positions,
selections, typing state, and other hints that should disappear when a connection closes. The Hub
does not store awareness in the model, replay log, CRDT state, or persistence callbacks.

```ts
client.onAwareness(({ id, peer, state }) => {
  if (id !== documentId) return;
  if (state === null) removePeerCursor(peer);
  else updatePeerCursor(peer, state);
});

client.setAwareness(documentId, {
  selection: { anchor: 12, head: 18 },
});
```

```python
client.on_awareness(handle_awareness)
await client.set_awareness(document_id, {"selection": {"anchor": 12, "head": 18}})
```

`awareness(id)` returns the latest remote state keyed by the Hub-assigned peer id. A client retains
its latest local state and republishes it whenever a managed connection opens, including a resume
that produces no model frame. Pass `null` in JavaScript or `None` in Python to clear that state. The
Hub also sends a removal when the connection closes. Read-only subscribers may publish awareness;
it does not grant model write access. Payload meaning, user identity, names, colors, and rendering
remain application concerns.

## Use an existing WebRTC data channel

transports can manage an `RTCDataChannel` after your application negotiates it. Signaling and
`RTCPeerConnection` ownership remain with the application:

```ts
const client = new Client();
client.connectDataChannel(peerConnection.createDataChannel("transports"));
```

The adapter receives frames, sends proposals, flushes queued CRDT operations and awareness when the
channel opens, and reports connection lifecycle through the same hooks as `connect()`. The remote
endpoint must speak the transports protocol with the same codec.

Pyodide uses the same browser channel through the `js` FFI:

```python
client = transports.Client()
await client.connect_data_channel(channel)
```

`connect_data_channel` runs until the channel closes. Native Python intentionally does not install
`aiortc`; browser peer-to-peer sessions are the supported WebRTC path. transports does not yet
provide signaling or turn two clients into a peer-hosted `Hub`.

## Mirror the server in Python

`Client.connect()` runs a receive loop until the WebSocket closes.

```python
client = transports.Client()
await client.connect("ws://127.0.0.1:8000/ws")
```

Edits work the same as in JS. `await client.propose(mid, value, "edit-1")` and
`await client.propose_ops(mid, ops, "edit-2")` ride the active `connect()` or `run()` connection,
native or Pyodide. They return `False` and drop the frame when there is none, so check
`client.connected` first when delivery matters. With a hand-rolled socket, send
`client.edit(mid, value)` or `client.edit_ops(mid, ops)` yourself.

Python exposes the same correlation API as `edit(..., proposal=...)`, `edit_ops`, `on_ack`, and
`on_reject`.

For a CRDT-backed shared model, use `await client.propose_crdt(mid, mutations)` on a managed
connection or send `client.edit_crdt(mid, mutations)` over a hand-rolled socket. The local mirror
updates immediately, including while disconnected. `pending_crdt_ops(mid)` reports operations still
waiting for the server echo.

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

For the common case, `transports.widget(server)` builds an `anywidget.AnyWidget` whose frontend
ships inside the wheel. Display it and every hosted model mirrors live. The frontend loads the same
WASM client state as the browser package, emits `transports-change` / `transports-reject` DOM events,
and exposes `el.transports.edit` for proposals. See [Pyodide](pyodide.md) for details.

```python
w = transports.widget(server)   # pip install anywidget
w                               # display; then mutate models + transports.sync(server)
```

For a custom frontend, `serve_anywidget` wires any anywidget-style `send` / `on_msg` object. You
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
