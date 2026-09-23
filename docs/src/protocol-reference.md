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

## CRDT specification

A `CrdtSpec` assigns merge semantics to the model's `Value` tree. The current format version is
`1`. Specifications are validated and serialized canonically in the shared Rust core, then hashed
with SHA-256. A replica must reject a different hash instead of joining a model with incompatible
semantics.

| Policy   | Required shape                                                                   | Meaning                                                                                                                                                          |
| -------- | -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Register | `{"kind": "register"}`                                                           | One deterministic last-writer-wins value.                                                                                                                        |
| Map      | `{"kind": "map", "fields": {...}, "values": policy?}`                            | Recursively merge named fields; `values` supplies the policy for other keys.                                                                                     |
| Set      | `{"kind": "set", "keys": [["id"]], "element": policy}`                          | Add-wins observed-remove members. Each entry in `keys` is a field path; several paths form a composite key. An empty list identifies members by whole value.     |
| Sequence | `{"kind": "sequence", "materialization": "list" or "string", "element": policy}` | Stable-ID ordered elements. IDs are CRDT metadata, not positional indexes.                                                                                       |

Map `values` and `element` default to a register. Sequence materialization defaults to `list`;
`string` sequences require register elements because each element materializes as one Unicode scalar
value. UI adapters convert between those element offsets and runtime-specific string indexes.
Every set key path must be unique, non-empty, and resolve through the element's map policies to a
register. Composite path order does not affect identity; canonical serialization sorts the paths.

Version 1 fixes conflict behavior rather than adding policy-specific knobs: registers use causal
last-writer-wins ordered by counter and replica identifier; sets are add-wins; concurrent updates
keep a map entry over a removal; and concurrent sequence inserts are ordered by their stable causal
identifiers. Changing those rules requires a new specification version and therefore a different
compatibility hash.

```json
{
  "version": 1,
  "root": {
    "kind": "map",
    "fields": {
      "document": {"kind": "sequence", "materialization": "string"},
      "rows": {
        "kind": "set",
        "keys": [["id"]],
        "element": {
          "kind": "map",
          "fields": {"title": {"kind": "register"}}
        }
      }
    },
    "values": {"kind": "register"}
  }
}
```

Python and JavaScript expose matching `CrdtSpec` and `CrdtDocument` objects. JavaScript also exports
`normalizeCrdtSpec`, `crdtSpecHash`, and `requireCrdtSpecHash` helpers for plain object literals. Both
bindings call the same core and produce the same canonical JSON and hash.

### Operations and state

A `CrdtDocument` assigns each local operation a dot with a monotonically increasing `counter` and a
non-empty `replica` identifier. Applying the same dot again is a no-op. Operation paths use stable
identity rather than list positions:

| Segment          | Shape                                                 | Addresses                         |
| ---------------- | ----------------------------------------------------- | --------------------------------- |
| Map key          | `{"kind": "key", "key": "title"}`                     | One map entry                     |
| Set member       | `{"kind": "member", "key": "..."}`                    | One canonical set-member identity |
| Sequence element | `{"kind": "element", "id": {"dot": ..., "index": 0}}` | One stable sequence element       |

The operation kinds are `register_set`, `map_set`, `map_remove`, `set_add`, `set_remove`,
`sequence_insert`, and `sequence_delete`. Remove operations carry the dots observed by their author.
An unobserved concurrent map update or set add therefore survives a remove. Sequence inserts carry an
optional predecessor ID; every inserted element receives the operation dot plus its index within the
insert batch. Set identity fields are immutable in place because changing one would invalidate the
member path; remove the old member and add the new one instead. A field-only update does not add set
membership, so a concurrent `set_remove` wins over that update. Use `set_add` when an edit must also
assert membership. When reasserting an existing member that contains a sequence, send its identity
fields rather than replaying the sequence value: sequence values in `set_add` are insertions and
therefore receive new element identities. Restore sequence content with `sequence_insert` operations.

`mutate()` and `apply()` return two views of the same accepted change:

- `applied` counts operations whose causal dots were new to this replica. It is zero for an
  idempotent replay even when the input contains operations.
- `patch` is an ordinary positional transports patch from the previous materialized value to the new
  value. Existing model consumers can apply it without understanding CRDT identity.
- `deltas` preserve set-member and sequence-element identities for editors and other consumers that
  need them.

Serialized reducer state contains its format version, the specification hash, causal context, stable
identities, and tombstones. `from_state()` rejects an unknown state version or a state whose hash
does not match the local specification.
`compact(frontier)` may discard metadata covered by a causally stable version-vector frontier.
Callers must not pass counters that every replica has not acknowledged. Deleted sequence payloads are
discarded, but their small anchor records remain so later inserts can still name a predecessor. The
reducer also rejects a frontier unless every counter since its last compacted counter has been
observed. Long-lived documents should compact acknowledged history periodically; otherwise causal
dots and duplicate-detection hashes grow with edit history. `Hub` does not infer a safe frontier:
applications must track acknowledgements across every replica before calling
`compact_shared_crdt()` and persisting the resulting state. Use the relay method in a multi-worker
deployment so the frontier reaches every reducer replica. Compaction does not change the materialized
value or model revision, so persistence must not discard its checkpoint as a duplicate revision.

Values inside `patch` use the tagged core `Value` encoding documented above. `CrdtDocument.value`
and identity deltas use ordinary Python or JavaScript values.

The earlier Python `SeqCrdt` helper remains available for compatibility. `CrdtDocument` is the shared
Rust implementation for new Python and JavaScript integrations.

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

A client proposal may add an opaque string identifier:

```json
{
  "t": "patch",
  "id": 1,
  "patch": {"rev": 0, "ops": []},
  "proposal": "form-12"
}
```

On acceptance, the server adds the same `proposal` to the authoritative patch sent to the origin.
Other clients receive the patch without it. `Client.on_ack` / `Client.onAck` also fires when the
patch revision is already mirrored.

An accepted proposal that makes no authoritative change produces an origin-only acknowledgement:

```json
{"t": "ack", "id": 1, "rev": 4, "proposal": "form-12"}
```

An acknowledgement reports the server revision but does not change the client mirror or its
accepted revision.

Clients ignore patch messages whose revision is less than or equal to the revision already seen for
that model.

### CRDT snapshot and operations

A CRDT snapshot initializes both the materialized mirror and reducer metadata:

```json
{
  "t": "crdt_snapshot",
  "id": 1099511627776,
  "type": "Document",
  "rev": 4,
  "value": {"Str": "hello"},
  "spec": {
    "version": 1,
    "root": {"kind": "sequence", "materialization": "string"}
  },
  "state": {"version": 1, "spec_hash": "sha256:..."}
}
```

The abbreviated `state` above represents the full reducer state produced by `CrdtDocument.state`.
A client edit sends revision zero and one or more operations:

```json
{
  "t": "crdt",
  "id": 1099511627776,
  "rev": 0,
  "ops": [
    {
      "kind": "sequence_insert",
      "path": [],
      "after": null,
      "values": ["!"],
      "dot": {"counter": 1, "replica": "client-a"}
    }
  ]
}
```

The authoritative echo carries the shared-model revision. The `effect` field is optional; `Hub`
omits it because clients apply operations through their local reducer.
Clients apply every unseen causal dot even when relay revisions arrive out of order. Revision order
alone cannot identify a duplicate when workers accept concurrent operations; duplicate suppression
uses the dots in reducer state. A reconnect receives a full CRDT snapshot, reapplies local outbox
operations, and resends them. This preserves offline changes without applying an operation twice.

`Client.recv()` (Python and JavaScript alike) returns `{t: "snapshot", id, rev}` for an accepted
snapshot, `{t: "crdt_snapshot", id, rev}` for a CRDT snapshot, the decoded patch or CRDT operation
message for an accepted change, and `None`/`undefined` for an ignored revision or an unrecognized
message type. Unknown types are ignored rather than raised, so a newer server can add message types
without breaking older clients. Reactive adapters can consume the returned change without reading
and decoding the complete mirror, either from the `recv()` return value or via `Client.on_change` /
`Client.onChange`, which fires with the same accepted change under the managed connect/run/SSE paths.
The returned change and `Client.value()` share immutable branches with the mirror and must not be
mutated. A patch or CRDT operation before its matching snapshot, an unknown operation, or an invalid
path raises; failed frames leave the mirror and its accepted revision unchanged.

### Reject

The server refuses a proposed edit that fails validation (or a write to a shared model the tenant
cannot write) by sending the proposer, and only the proposer, the authoritative revert followed by a
typed reject saying why. `rev` is the server's current revision for the model, and `error` carries
the model's validation message where available, for example pydantic's.

```json
{
  "t": "reject",
  "id": 1,
  "rev": 4,
  "proposal": "form-12",
  "error": "1 validation error for Device\nbrightness\n  Input should be a valid integer ..."
}
```

A reject never changes the mirror (the revert snapshot alongside does); clients surface it through
`Client.on_reject` / `Client.onReject` so an app can show why the edit was refused instead of only
reverting. If the proposal supplied an identifier, the reject carries it. A CRDT rejection includes
`crdt_ops`; clients remove those causal dots from their outbox before applying the following CRDT
snapshot.

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
