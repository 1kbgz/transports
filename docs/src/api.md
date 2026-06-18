# API Reference

The high-level reactive session and model bridge, plus the low-level core (the compiled Rust
extension).

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

## Core

The low-level surface, exposed directly from the Rust core. Most users work through
{py:class}`transports.Session`; these are useful for stateless diff/apply, encoding, and custom
hosting.

```{eval-rst}
.. autofunction:: transports.diff

.. autofunction:: transports.apply

.. autofunction:: transports.encode

.. autofunction:: transports.decode

.. autoclass:: transports.Store
   :members:
   :undoc-members:
   :member-order: bysource
```
