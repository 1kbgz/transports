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
next `flush` or `autoflush` tick.

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

## Serve the hub over WebSocket

```python
import asyncio
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute

async def startup():
    asyncio.create_task(transports.autoflush(hub))

app = Starlette(
    routes=[WebSocketRoute("/ws/{tenant}", hub.endpoint())],
    on_startup=[startup],
)
```

Clients use the same `Client` API as single-tenant servers. The hub decides which private and shared
model snapshots each tenant receives.
