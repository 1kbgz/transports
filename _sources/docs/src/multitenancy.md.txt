# How to isolate tenants and share models

This guide shows you how to route connections to tenant-local sessions, subscribe tenants to shared
models, and choose a merge strategy for shared writes.

## Create a hub

A `Hub` maps each connection handle to a tenant key. With Starlette, the connection handle is the
`WebSocket`.

```python
import transports

hub = transports.Hub(key=lambda ws: ws.path_params["tenant"])
```

## Host private tenant models

Each tenant has its own `Session`. Private model ids can overlap between tenants because each tenant
has an isolated store.

```python
from pydantic import BaseModel

class Document(BaseModel):
    title: str
    body: str = ""

hub.tenant("alice").host(Document(title="Alice notes"))
hub.tenant("bob").host(Document(title="Bob notes"))
```

A connection for `alice` receives only Alice's private snapshots. A private edit is echoed only to
other Alice connections.

## Share a model read-only

Register a shared model and subscribe tenants with `READ` access.

```python
from transports import READ

sid = hub.share(Document(title="Roadmap"))
hub.subscribe("alice", sid, READ)
hub.subscribe("bob", sid, READ)
```

Write to the shared model from the host side with `set_shared`. Subscribers receive the patch on the
next `sync` or `autosync` tick.

```python
hub.set_shared(sid, Document(title="Roadmap", body="Updated"))
```

## Allow shared writes

Subscribe writers with `WRITE` access.

```python
from transports import WRITE

hub.subscribe("alice", sid, WRITE)
hub.subscribe("bob", sid, WRITE)
```

A write from one subscriber is merged into the authoritative shared value and echoed to every
subscriber, including the origin.

## Choose a merge strategy

Use `LastWriteWins` for arrival-order writes. Use `LwwMapCrdt` when top-level map fields should
converge independent of arrival order.

```python
sid = hub.share(Document(title="Shared"), merge=transports.LwwMapCrdt)
```

For custom reconciliation, implement `merge(current, patch, origin) -> value` on a `MergeStrategy`
subclass and pass the class to `share`.

```python
class MyMerge(transports.MergeStrategy):
    def merge(self, current, patch, origin):
        ...

sid = hub.share(Document(title="Shared"), merge=MyMerge)
```

## Share a schema-directed CRDT

Use a `CrdtSpec` when clients need offline edits, concurrent sequence changes, keyed-set changes, or
nested merge behavior that must match in Python and JavaScript. A CRDT-backed shared model uses the
shared Rust reducer instead of a Python `MergeStrategy`.

```python
spec = transports.CrdtSpec(
    {
        "kind": "map",
        "fields": {
            "title": {"kind": "register"},
            "body": {"kind": "sequence", "materialization": "string"},
        },
    }
)

sid = hub.share(Document(title="Shared", body=""), crdt_spec=spec)
hub.subscribe("alice", sid, WRITE)
hub.subscribe("bob", sid, WRITE)
```

Clients receive the specification and reducer state in the initial snapshot. `edit_crdt` /
`editCrdt` applies mutations locally and returns causally identified operations. `propose_crdt` /
`proposeCrdt` sends them over a managed connection.

```python
await client.propose_crdt(
    sid,
    [
        {
            "kind": "sequence_insert",
            "path": [{"kind": "key", "key": "body"}],
            "after": None,
            "values": list("Hello"),
        }
    ],
)
```

A disconnected proposal remains in the client's CRDT outbox. The client reapplies it over the next
server snapshot and resends it when a managed WebSocket opens. The reducer's causal dots make that
resend idempotent. `pending_crdt_ops()` / `pendingCrdtOps()` reports how many local operations still
await an authoritative echo. The outbox has no automatic size limit; applications that allow long
offline sessions should monitor that count and set their own editing or storage policy.

Host code uses `hub.mutate_shared_crdt(sid, mutations)`. With a `RelayBroadcaster`, use
`await relay.mutate_shared_crdt(...)` so the operation also reaches other workers.

To persist the model, store all arguments delivered to `Hub.on_shared_write`. Its `merge_state`
argument contains `crdt_spec` and `crdt_state`; pass it back to `share(..., rev=...,
merge_state=...)` after a restart. `Hub.snapshot_shared()` returns the same nested `merge_state`
shape for explicit checkpoints.

`Hub` binds each observed client replica ID to its tenant key for that process lifetime and rejects
another tenant's attempt to reuse it. A relay propagates observed bindings with accepted operations,
so other workers enforce them after receiving the operation. Replica IDs are still coordination
identifiers, not durable authentication credentials; simultaneous reuse on different workers is not
an authorization boundary. The process-lifetime binding table grows with distinct replica IDs, as
does uncompacted reducer history; long-lived services should monitor both with their session policy.
Enforce writer authorization with subscription modes and tenant keys.

Reducer metadata grows with edit history until causally stable dots are compacted. After every
replica has acknowledged a version-vector frontier, call `hub.compact_shared_crdt(sid, frontier)`.
For a relayed hub, call `await relay.compact_shared_crdt(sid, frontier)` so every worker compacts the
same frontier. The durability callback receives the compacted state without incrementing the model
revision; persistence must store that checkpoint even when its revision matches the previous write.
Never infer a frontier from one connection.

## Serve the hub over WebSocket

```python
import asyncio
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute

async def startup():
    asyncio.create_task(transports.autosync(hub))

app = Starlette(
    routes=[WebSocketRoute("/ws/{tenant}", transports.ws_endpoint(hub))],
    on_startup=[startup],
)
```

Clients use the same `Client` API as single-tenant servers. The hub decides which private and shared
model snapshots each tenant receives.
