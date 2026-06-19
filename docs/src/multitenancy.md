# Multi-tenancy & sharing

A single process often serves many independent tenants, and some data is **shared** between them. A
{py:class}`~transports.Hub` handles both: it routes each connection to its tenant and lets any number
of tenants subscribe to a shared data structure.

## Tenants

Each tenant's models live in their own {py:class}`~transports.Session`, so tenants are fully
isolated — one tenant never sees another's models. The `Hub` maps a connection to a tenant with a
`key` function you provide:

```python
import transports

hub = transports.Hub(key=lambda ws: ws.path_params["tenant"])

# a tenant's private models
hub.tenant("alice").host(Document(title="notes"))
```

A connection only ever receives its own tenant's private models (plus any shared models it
subscribes to), and a private edit is relayed only to that tenant's other connections.

## Shared data structures

A **shared** model's authoritative state lives in the hub. Register it with `share()`, then connect
tenants to it with `subscribe()` and an access mode — `READ` or `WRITE`. The sharing shape is just
the set of subscriptions:

```python
from transports import READ, WRITE

doc = hub.share(Document(title="roadmap"))   # returns a shared id
hub.subscribe("alice", doc, WRITE)
hub.subscribe("bob", doc, READ)              # bob mirrors it but cannot write
```

- **One model, many readers** — subscribe many tenants `READ` (broadcast / fan-out).
- **Many writers, one model** — subscribe many tenants `WRITE` (collaborative editing).
- **Many models, many tenants** — any mix of the above; each connection receives exactly the models
  it subscribes to.

Shared models are **server-authoritative**: a writer sends its edit and receives the reconciled
patch back, and every subscriber is kept in sync. Write to a shared model from the host side with
`set_shared(id, value)`; the change is broadcast on the next flush.

## Reconciling concurrent writes

When several tenants write the same shared model, a {py:class}`~transports.MergeStrategy` decides how
the writes combine. Pass one per shared model:

```python
from transports import LastWriteWins, LwwMapCrdt

hub.share(Document(), merge=LastWriteWins)   # default: apply writes in arrival order
hub.share(Document(), merge=LwwMapCrdt)      # conflict-free per-field resolution
```

- {py:class}`~transports.LastWriteWins` (the default) applies each write as it arrives.
- {py:class}`~transports.LwwMapCrdt` resolves per top-level field: concurrent edits to *different*
  fields both survive, and conflicting edits to the *same* field converge to the same value
  regardless of the order the writes arrive in.

To plug in your own reconciliation, implement `merge(current, patch, origin) -> value` and pass the
class to `share(merge=...)`.

## Serving it

`Hub.endpoint()` adapts the hub to a Starlette WebSocket route, reusing the same per-connection
[codec negotiation](codecs.md) as a single-tenant server. Run one
{py:func}`~transports.autoflush` task to stream host-side changes to every connection.

```python
import asyncio
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute

app = Starlette(
    routes=[WebSocketRoute("/ws/{tenant}", hub.endpoint())],
    on_startup=[lambda: asyncio.create_task(transports.autoflush(hub))],
)
```

On the client side nothing changes — a {py:class}`~transports.Client` mirrors whatever models the
hub sends it, private or shared.
