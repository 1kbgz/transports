# API Reference

The high-level reactive session, the model bridge, the connection adapters, and the low-level core.

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

.. autofunction:: transports.starlette_endpoint

.. autofunction:: transports.autoflush
```

## Multi-tenancy & sharing

```{eval-rst}
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

## Core

The low-level surface, exposed directly from the Rust core. Most users work through
{py:class}`transports.Session`; these are useful for stateless diff/apply, encoding, and custom
hosting.

```{eval-rst}
.. autofunction:: transports.diff

.. autofunction:: transports.apply

.. autofunction:: transports.encode

.. autofunction:: transports.decode

.. autofunction:: transports.encode_as

.. autofunction:: transports.decode_as

.. autofunction:: transports.json_to_msgpack

.. autofunction:: transports.msgpack_to_json

.. autoclass:: transports.Store
   :members:
   :undoc-members:
   :member-order: bysource
```
