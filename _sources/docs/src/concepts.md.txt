# Concepts

## One core, two bindings

transports is a **Rust core** with thin **Python (PyO3)** and **JavaScript (wasm)** bindings. The
core owns the model representation, the diff/patch engine, the codecs, and the wire framing; both
languages call into the same compiled implementation. A patch produced by Python and a patch produced
by JavaScript for the same change are byte-identical, so either side can host a model and the other
can mirror it.

```text
   Python (PyO3)            JavaScript (wasm)
   ┌─────────────┐  patch   ┌─────────────┐
   │ transports  │◄────────►│ transports  │
   │ core (Rust) │  bytes   │ core (Rust) │
   └─────────────┘          └─────────────┘
        model bridge              object bridge
   pydantic / dataclass /     plain objects /
        msgspec                  toValue/fromValue
```

## Value

Every model is, on the wire, a **Value** — a small tagged union: `Null`, `Bool`, `Int`, `Float`,
`Str`, `List`, `Map`, and `Submodel` (a reference to another model by id). The serialized form is the
externally-tagged enum the Rust core speaks:

```json
{"Map": {"name": {"Str": "lamp"}, "on": {"Bool": true}}}
```

You rarely write this by hand — the [model bridges](bridges.md) (`to_value`/`from_value` in Python,
`toValue`/`fromValue` in JavaScript) convert your models to and from it.

## Patch and revisions

A **Patch** is the incremental update between two values: an ordered list of operations
(`Set`, `Remove`, `Insert`, `RemoveAt`) addressed by a path into the value, plus a `rev` (revision)
counter. This is the core idea that distinguishes transports from "serialize the whole model and
send it": when one field changes, only that field's operation travels.

```python
{"rev": 1, "ops": [{"Set": {"path": [{"Key": "on"}], "value": {"Bool": True}}}]}
```

The engine guarantees the round-trip property `apply(old, diff(old, new)) == new`. Maps diff by key
and lists diff positionally; a type change at a path replaces the value there wholesale.

## Store and Session

The low-level {py:class}`~transports.Store` holds model values by id and, on `mutate`, diffs the new
value against the held one, bumps the `rev`, and returns the patch. The high-level
{py:class}`~transports.Session` wraps it with the model bridge and reactive observation, so you work
with your own model objects and get patches automatically.

This is the single-owner nucleus; multi-tenant sessions (fan-out to many subscribers, backpressure,
authorization) are on the roadmap.

## What's next

- **Codecs.** JSON is the current wire format, behind a pluggable codec trait. MessagePack and other
  binary codecs are planned; they change only the *bytes*, not the model or patch semantics.
- **Connections.** A length-prefixed, codec-tagged `Frame` envelope exists in the core; the actual
  transport adapters (WebSocket, SSE, HTTP, Jupyter comm, TCP) come next, carrying frames between
  processes.
```
