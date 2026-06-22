# Concepts

## Why patches instead of whole models?

transports is built around one constraint: a changed field should not require resending the whole
model. Whole-model messages are simple at first, but they make every participant reason about stale
copies, large payloads, and accidental overwrites. A patch keeps the update narrow. It says which
path changed, what operation happened there, and which revision the model reached.

That design matters most when several runtimes participate. A Python process can host a pydantic
model, a browser can mirror it as a JavaScript object, and another client can propose an edit. They do
not need to agree on a Python class or JavaScript prototype. They only need to agree on the core
`Value` and patch format.

## Why a Rust core?

The core representation, diff engine, patch application, codecs, framing, and store live in Rust.
Python and JavaScript bindings call into the same implementation. This keeps the round-trip property
in one place:

```text
apply(old, diff(old, new)) == new
```

The bridge code at the edge is deliberately thin. Python model libraries and JavaScript objects are
converted into `Value`; after that, both languages use the same machinery.

## Why a `Value` layer?

Application models carry library-specific behavior: pydantic validation, dataclass defaults,
msgspec slots, JavaScript number semantics. The wire needs something smaller and more stable than any
one of those object systems. `Value` is that common form: tagged nulls, scalars, lists, maps, and core
submodel references.

The bridges hide the tags for normal application code. They become useful when you need a stable
protocol boundary, a custom codec, or a language-neutral client.

## Why server-owned revisions?

A client edit is a proposal, not an optimistic local commit. The server applies the proposed ops,
assigns the next revision, refreshes its hosted Python object, and echoes the authoritative patch to
every connection. The origin updates from that echo just like every other client.

This avoids two common sync problems. First, clients do not invent revisions that later conflict with
the server's sequence. Second, the server's hosted object cannot become stale after it accepts a
remote edit.

## Why transport-agnostic adapters?

`Server` and `Hub` are synchronous protocol objects. Their methods accept opaque connection handles
and return the messages each connection should receive. WebSocket, SSE, Jupyter comm, and anywidget
support are thin adapters around that contract.

The practical result is that most behavior can be tested without a network. A test can call
`server.open`, `server.recv`, and `server.flush` directly, then feed the returned frames into a
`Client`.

## Why a Hub for multi-tenancy?

A single `Session` is a good fit for one owner and many mirrors. A multi-tenant service needs a
second routing layer: private state must stay tenant-local, while selected shared state must fan out
to many tenants. `Hub` provides that layer without changing the client protocol.

Shared models use ids above a reserved base so they do not collide with tenant-local session ids.
Subscribers receive the same snapshot and patch messages as any other client. The difference is on
the server: the hub decides who can see the model, who can write it, and how concurrent shared writes
are reconciled.

## Why pluggable codecs?

The patch semantics do not depend on JSON, MessagePack, or any custom encoding. Codecs only change
how a message becomes bytes or text on the wire. That separation lets JSON remain convenient for
debugging while MessagePack or a registered custom codec carries production traffic.
