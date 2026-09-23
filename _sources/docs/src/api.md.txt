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
consumers. Transfer `state` to another process with `from_state()` when a patch history is not
available.

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

The codec registry functions (``encode_as``, ``decode_as``, ``register_codec``,
``unregister_codec``, ``registered_codecs``) are re-exported at the top level and listed under
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
