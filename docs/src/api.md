# API Reference

This reference lists the Python surface exposed by `transports`.

## Session

```{eval-rst}
.. autoclass:: transports.Session
   :members:
   :undoc-members:
   :member-order: bysource
```

## Model bridge

### List identity policy

`Session.host(model, list_keys=...)` and `Hub.share(model, list_keys=...)` accept a list of
`{"path": [...], "key": [...]}` records. `path` addresses a list from the model root;
`key` addresses its identity within each item. Segments are `{"Key": "field"}` or
`{"Index": 0}`. Empty paths address the root. Paths are exact; wildcards are not supported.

```python
rules = [{"path": [{"Key": "rows"}], "key": [{"Key": "id"}]}]
model_id = session.host(model, list_keys=rules)
```

The low-level Python `diff(old, new, list_keys=json_string)` and
`Store.host(type_name, value_json, list_keys=json_string)` take JSON-encoded policy records.
JavaScript's `diff(old, new, listKeys)` and `Store.host(typeName, valueJson, listKeys)`
take the same optional JSON string as their third argument.

Keys are unique strings or integers; string `"1"` and integer `1` are distinct.
Use strings for integer identifiers outside JavaScript's safe integer range when mirroring to a browser.
Missing, duplicate, or unsupported item keys select the existing default list diff for that list.
Duplicate policy paths, unknown policy fields, and malformed paths raise an error.
The default policy remains unchanged when the argument is omitted.

Retained identities emit existing move/reorder operations, followed by field updates at their new
positions. Added and removed identities emit insert/remove operations. Policy metadata is host
configuration and is not included in snapshots or patches; configure it again when restoring a host.
Client proposals are still positional and server-authoritative, not concurrency-safe keyed intents.
`list_keys` cannot be combined with `crdt_spec`; CRDT identity remains governed by its schema.

```{eval-rst}
.. autofunction:: transports.to_value

.. autofunction:: transports.from_value

.. autofunction:: transports.schema_of

.. autofunction:: transports.schema_to_ts
```

## Connections

```{eval-rst}
.. autoclass:: transports.Server
   :members:
   :undoc-members:
   :member-order: bysource

.. autoclass:: transports.Client
   :members:
   :undoc-members:
   :member-order: bysource

.. autofunction:: transports.ws_endpoint

.. autofunction:: transports.sse_endpoint

.. autofunction:: transports.serve_comm

.. autofunction:: transports.serve_anywidget

.. autofunction:: transports.autosync

.. autofunction:: transports.sync
```

## Multi-tenancy and sharing

```{eval-rst}
.. autodata:: transports.READ

.. autodata:: transports.WRITE

.. autoclass:: transports.Hub
   :members:
   :undoc-members:
   :member-order: bysource

.. autoclass:: transports.MergeStrategy
   :members:

.. autoclass:: transports.LastWriteWins
   :members:

.. autoclass:: transports.LwwMapCrdt
   :members:
```

## CRDT specifications

```{eval-rst}
.. autoclass:: transports.CrdtSpec
   :members:
   :member-order: bysource

.. autoclass:: transports.CrdtDocument
   :members:
   :member-order: bysource
```

`RegisterPolicy`, `MapPolicy`, `SetPolicy`, `SequencePolicy`, and their `CrdtPolicy` union describe
the accepted dictionary shapes for type checkers. Policy validation, canonical serialization, and
hashing run in the shared Rust core.

`CrdtDocument` accepts ordinary Python values. `mutate()` creates causally identified operations and
applies them locally; `apply()` accepts the same operations from another replica. Both return a
positional patch for existing model consumers and identity-preserving deltas for CRDT-aware
consumers. For sequences, a local `sequence_splice` mutation converts a positional range into the
stable-ID delete and insert operations used on the wire. Transfer `state` to another process with
`from_state()` when a patch history is not available.

After a CRDT snapshot, `Client.crdt_spec(id)` returns that model's canonical specification. The
JavaScript spelling is `client.crdtSpec(id)`. Plain and unknown models return `None` / `undefined`.

## Protocol helpers

```{eval-rst}
.. autofunction:: transports.protocol.normalize_codec

.. autofunction:: transports.protocol.snapshot_msg

.. autofunction:: transports.protocol.crdt_snapshot_msg

.. autofunction:: transports.protocol.patch_msg

.. autofunction:: transports.protocol.crdt_msg

.. autofunction:: transports.protocol.encode

.. autofunction:: transports.protocol.decode
```

The codec registry functions (`encode_as`, `decode_as`, `register_codec`,
`unregister_codec`, `registered_codecs`) are re-exported at the top level and listed under
**Core** below.

## Core

```{eval-rst}
.. autofunction:: transports.diff

.. autofunction:: transports.apply

.. autofunction:: transports.encode

.. autofunction:: transports.decode

.. autofunction:: transports.encode_as

.. autofunction:: transports.decode_as

.. autofunction:: transports.json_to_msgpack

.. autofunction:: transports.msgpack_to_json

.. autofunction:: transports.register_codec

.. autofunction:: transports.unregister_codec

.. autofunction:: transports.registered_codecs

.. autoclass:: transports.Store
   :members:
   :undoc-members:
   :member-order: bysource
```

### Client model metadata

`Client.type_name(id)` in Python and `Client.typeName(id)` in JavaScript return the type name from the
last accepted snapshot, or `None` / `undefined` for an unknown ID. Ordinary and CRDT snapshots update
this metadata through the shared Rust client state before change listeners run. Metadata survives
disconnect and delta-only resume. `ids()` lists mirrored models; several IDs can have the same type.
The accepted-change notification shape is unchanged.
