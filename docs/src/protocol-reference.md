# Wire protocol reference

This reference describes the logical wire data before a connection codec turns it into a text or
binary frame.

## Value

A model value is an externally tagged enum.

| Variant | Shape | Notes |
|---|---|---|
| Null | `"Null"` | Null value. |
| Bool | `{"Bool": true}` | Boolean. |
| Int | `{"Int": 1}` | Signed integer. |
| Float | `{"Float": 1.5}` | Floating-point number. |
| Str | `{"Str": "lamp"}` | String. |
| List | `{"List": [Value, ...]}` | Ordered values. |
| Map | `{"Map": {"field": Value}}` | String-keyed map. |
| Submodel | `{"Submodel": 1}` | Core model reference. Current Python and JavaScript bridges inline nested models as maps. |

Example:

```json
{"Map": {"name": {"Str": "lamp"}, "on": {"Bool": true}}}
```

## Path segments

Patch operations address values by paths from the model root.

| Segment | Shape |
|---|---|
| Map key | `{"Key": "name"}` |
| List index | `{"Index": 0}` |

An empty path addresses the whole model value.

## Patch

A patch contains the model revision reached by applying the patch and an ordered list of operations.

```json
{"rev": 1, "ops": []}
```

## Operations

### Set

Sets or replaces the value at `path`. An empty path replaces the whole model.

```json
{"Set": {"path": [{"Key": "on"}], "value": {"Bool": true}}}
```

### Remove

Removes a map entry. The last path segment is a `Key`.

```json
{"Remove": {"path": [{"Key": "name"}]}}
```

### Insert

Inserts a value into the list at `path`.

```json
{"Insert": {"path": [{"Key": "items"}], "index": 0, "value": {"Str": "first"}}}
```

### RemoveAt

Removes an element from the list at `path`.

```json
{"RemoveAt": {"path": [{"Key": "items"}], "index": 0}}
```

### Move

Moves an existing element within the list at `path`. `from` addresses the list before this operation;
`to` is the element's final index after the move. Both indices must be less than the list length.
Equal indices are a valid no-op.

```json
{"Move": {"path": [{"Key": "items"}], "from": 0, "to": 2}}
```

For `[{"id": "a"}, {"id": "b"}, {"id": "c"}]`, this moves `a` after `c`. Receivers can retain
the moved element's application identity instead of interpreting the change as positional
replacement.

### Reorder

Reorders a complete list in one operation. `order[new_index]` is the element's index before this
operation. `order` must contain every index from `0` through `list.length - 1` exactly once; wrong
lengths, out-of-bounds indices, and duplicates are rejected before the list changes.

```json
{"Reorder": {"path": [{"Key": "items"}], "order": [2, 0, 1]}}
```

For `[a, b, c]`, this produces `[c, a, b]`. Applying the permutation takes linear time and lets a
receiver carry each item's identity directly to its new index.

Malformed paths, wrong container types, and out-of-bounds list indexes are rejected by the core apply
path.

List diff recognizes exact-value matches and emits `Move` for them, including duplicate equal values.
If a moved record also changes, transports can use an unchanged record as a move anchor and diff the
changed record at its final position. Generic `Value` lists contain no key metadata from which
transports could infer identity when every moved record changes, so the diff remains positional rather
than guessing an application-specific `id` field.

Move reconciliation has a linear work budget. Sparse moves remain granular; an exact dense
permutation emits one `Reorder` instead of repeated matching and array movement. If the lists are not
an exact permutation and reconciliation exhausts the budget, diff falls back to positional updates.

## Protocol messages

Connections carry one logical message per frame.

### Snapshot

A snapshot initializes a client mirror for one model.

```json
{
  "t": "snapshot",
  "id": 1,
  "type": "Device",
  "rev": 0,
  "value": {"Map": {"name": {"Str": "lamp"}, "on": {"Bool": false}}}
}
```

### Patch

A patch advances an existing mirror.

```json
{
  "t": "patch",
  "id": 1,
  "patch": {
    "rev": 1,
    "ops": [
      {"Set": {"path": [{"Key": "on"}], "value": {"Bool": true}}}
    ]
  }
}
```

Clients ignore patch messages whose revision is less than or equal to the revision already seen for
that model.

`Client.recv()` (Python and JavaScript alike) returns `{t: "snapshot", id, rev}` for an accepted
snapshot, the decoded patch message for an accepted patch, and `None`/`undefined` for an ignored
revision or an unrecognized message type — unknown types are ignored, not errors, so a newer server
can add message types without breaking older clients. Reactive adapters can consume the returned
patch paths without reading and decoding the complete mirror, either from the `recv()` return value
or via `Client.on_change` / `Client.onChange`, which fires with the same accepted change under the
managed connect/run/SSE paths. The returned change and `Client.value()` share immutable branches
with the mirror and must not be mutated. A patch before its snapshot, an unknown patch operation, or
an invalid path raises; failed frames leave the mirror and its accepted revision unchanged.

### Reject

The server refuses a proposed edit that fails validation — or a write to a shared model the tenant
cannot write — by sending the proposer (and only the proposer) the authoritative revert followed by a
typed reject saying why. `rev` is the server's current revision for the model, and `error` carries
the model's validation message where available (e.g. pydantic's).

```json
{
  "t": "reject",
  "id": 1,
  "rev": 4,
  "error": "1 validation error for Device\nbrightness\n  Input should be a valid integer ..."
}
```

A reject never changes the mirror (the revert snapshot alongside does); clients surface it through
`Client.on_reject` / `Client.onReject` so an app can show *why* the edit died instead of a silent
revert.

## Model ids

`Session` model ids start at `1` inside each store. Tenant-local ids in a `Hub` are isolated per
tenant. Shared hub model ids start at `1099511627776` (`1 << 40`) so they do not collide with
session-local ids.

## Built-in codecs

| Name accepted | Canonical codec | Frame type |
|---|---|---|
| `json`, `application/json`, empty, `None` | `json` | Text frame. |
| `msgpack`, `application/msgpack`, `x-msgpack`, `application/x-msgpack` | `msgpack` | Binary frame. |

A WebSocket connection selects a codec with the `codec` query parameter. `Server` and `Hub` encode
each outbound message for the target connection's codec.

SSE, Jupyter comm, and anywidget adapters use JSON text only.

## Custom codecs

A custom codec is registered under a content type and provides two functions:

| Function | Input | Output |
|---|---|---|
| `encode` | JSON-able protocol message or `Value` object | `str` or `bytes` |
| `decode` | `str` or `bytes` | JSON-able protocol message or `Value` object |

Built-in codec names cannot be overridden. Custom codecs are binding-local; register matching
implementations in every Python or JavaScript process that uses the content type.
