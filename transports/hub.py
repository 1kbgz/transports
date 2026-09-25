"""Multi-tenant hub: route many connections to many tenant sessions, and share data structures.

A `Hub` serves many tenants from one process. Each connection is mapped to a **tenant** by an
app-supplied `key(conn)`; a tenant's *private* models live in its own isolated `Session` (so tenants
never see each other's data). On top of that, a hub hosts **shared data structures** — models whose
authoritative state lives in the hub and which any number of tenants can *subscribe* to with an
access **mode** (`READ` or `WRITE`). The sharing cardinalities fall out of the subscription edges:

- **1-N** — one shared model, many `READ` subscribers (broadcast / fan-out).
- **N-1 / N-N** — many `WRITE` subscribers on one (or many) shared models (collaborative editing).

Writes to a shared model are reconciled by a pluggable :class:`MergeStrategy` (default
:class:`LastWriteWins`; :class:`LwwMapCrdt` is a conflict-free reference). Like `Server`, the hub's
logic is synchronous and transport-agnostic — its methods *return* the messages to send, keyed by
connection — so it is unit-testable without a network. It satisfies the same `Broadcaster` contract as
`Server`, so the same adapters serve it: `ws_endpoint(hub)`, `sse_endpoint(hub)`, `serve_comm`, etc.

Shared models are **server-authoritative**: a writer sends its edit and receives the authoritative
patch back.
"""

import json
import uuid
from collections.abc import Callable
from typing import Any

from . import protocol
from ._bridge import _py_of, _value_of, to_value
from .crdt import CrdtDocument, CrdtSpec
from .server import Wire
from .session import Session
from .transports import apply as _apply, diff as _diff

READ = "read"
WRITE = "write"

#: Shared-model wire ids live above this base so they never collide with per-`Session` model ids
#: (which start at 1 in each tenant's own store).
SHARED_ID_BASE = 1 << 40


class MergeStrategy:
    """How a write to a shared model is reconciled into its authoritative value.

    `merge(current, patch, origin)` returns the new core `Value`. Implementations may be stateful;
    pass the **class** (not an instance) to `Hub.share(merge=...)` so each shared model gets its own
    instance and its own state.
    """

    def merge(self, current: Any, patch: dict, origin: Any) -> Any:  # pragma: no cover - interface
        raise NotImplementedError

    def state(self) -> dict:
        """The strategy's own metadata (e.g. a CRDT clock), JSON-serializable, so a model's full state can
        be transferred to a joining worker or persisted by the user. Stateless strategies return ``{}``."""
        return {}

    def restore(self, state: dict) -> None:
        """Adopt metadata produced by :meth:`state` (catch-up or durable restore). Default: ignore."""


class LastWriteWins(MergeStrategy):
    """Apply each write in arrival order (today's `Store` semantics). Order-dependent."""

    def merge(self, current: Any, patch: dict, origin: Any) -> Any:
        return json.loads(_apply(json.dumps(current), json.dumps(patch)))


class LwwMapCrdt(MergeStrategy):
    """Conflict-free per-top-level-key last-writer-wins register map.

    Each top-level map key carries a logical stamp `(patch rev, origin)`; a key's write is accepted
    only if its stamp is at least the stored one. Two consequences: concurrent edits to *different*
    keys both survive, and conflicting edits to the *same* key converge to the same value regardless
    of the order the hub happens to receive them in (the stamp is intrinsic to the write, not its
    arrival order). Nested or list ops fall back to a direct apply, stamped by their top-level key.
    """

    def __init__(self) -> None:
        self._clock: dict[str, tuple] = {}

    def merge(self, current: Any, patch: dict, origin: Any) -> Any:
        new = json.loads(json.dumps(current))
        mp = new.get("Map") if isinstance(new, dict) else None
        if mp is None:  # not a map model — fall back to whole-value LWW
            return json.loads(_apply(json.dumps(current), json.dumps(patch)))
        rev = patch.get("rev", 0)
        stamp = (rev, str(origin))
        for op in patch.get("ops", []):
            kind = next(iter(op))
            body = op[kind]
            path = body.get("path", [])
            top = path[0]["Key"] if path and "Key" in path[0] else None
            if top is None:  # unattributable to a key (e.g. whole-model op) — apply as-is
                new = json.loads(_apply(json.dumps(new), json.dumps({"rev": rev, "ops": [op]})))
                mp = new.get("Map")
                continue
            if top in self._clock and stamp < self._clock[top]:
                continue  # stale write — drop
            self._clock[top] = stamp
            if len(path) == 1 and kind == "Set":
                mp[top] = body["value"]
            elif len(path) == 1 and kind == "Remove":
                mp.pop(top, None)
            else:  # nested op under `top` — apply to the whole value, then refresh the map handle
                new = json.loads(_apply(json.dumps(new), json.dumps({"rev": rev, "ops": [op]})))
                mp = new.get("Map")
        return new

    def state(self) -> dict:
        return {"clock": {k: list(v) for k, v in self._clock.items()}}

    def restore(self, state: dict) -> None:
        self._clock = {k: tuple(v) for k, v in state.get("clock", {}).items()}


class DeepLwwCrdt(MergeStrategy):
    """Field-granular conflict-free LWW — an independent last-writer-wins register at **every** map path,
    not just the top level (cf. :class:`LwwMapCrdt`). Each map-key write carries a logical stamp
    `(patch rev, origin)` kept per *full path*; a write is accepted only if its stamp is at least the one
    stored for that exact path. So concurrent edits to **different** fields — however deeply nested — all
    survive, and conflicting edits to the **same** field converge to the same value regardless of the
    order the hub receives them.

    Scope: field-granular for scalar/map writes (the leaf ops a `diff` produces for edited fields).
    List-index ops and whole-subtree replaces fall back to a direct stamped apply — they are not
    element-granular (an order-free list/text CRDT needs per-element identity; see ROADMAP 6.2).
    """

    def __init__(self) -> None:
        self._clock: dict[tuple, tuple] = {}

    def merge(self, current: Any, patch: dict, origin: Any) -> Any:
        new = json.loads(json.dumps(current))
        rev = patch.get("rev", 0)
        stamp = (rev, str(origin))
        for op in patch.get("ops", []):
            kind = next(iter(op))
            body = op[kind]
            segs = body.get("path", [])
            keys = tuple(s["Key"] for s in segs if "Key" in s)
            if len(keys) != len(segs) or kind not in ("Set", "Remove"):
                # a list-index op or whole-subtree op — apply directly (not field-granular)
                new = json.loads(_apply(json.dumps(new), json.dumps({"rev": rev, "ops": [op]})))
                continue
            if keys in self._clock and stamp < self._clock[keys]:
                continue  # stale write to this exact field — drop
            self._clock[keys] = stamp
            new = json.loads(_apply(json.dumps(new), json.dumps({"rev": rev, "ops": [op]})))
        return new

    def state(self) -> dict:
        return {"clock": [[list(k), list(v)] for k, v in self._clock.items()]}

    def restore(self, state: dict) -> None:
        self._clock = {tuple(k): tuple(v) for k, v in state.get("clock", [])}


class _Shared:
    """Authoritative state for a shared data structure."""

    def __init__(
        self,
        type_name: str,
        value: dict,
        merge: MergeStrategy,
        *,
        replay: bool = False,
        rev: int = 0,
        log_cap: int = 512,
        crdt: CrdtDocument | None = None,
    ) -> None:
        self.type_name = type_name
        self.value = value
        self.rev = rev
        self.merge = merge
        self.crdt = crdt
        self.subs: dict[Any, str] = {}  # tenant key -> mode
        self.replay = replay
        self.log: list[tuple] = []  # bounded [(rev, patch)] for delta catch-up when replay=True
        self.log_cap = log_cap


class Hub:
    """Route connections to per-tenant `Session` objects and fan shared data structures to subscribers.

    Construct with `key`, a function mapping a connection handle to its tenant key. Register shared
    models with `share()` and connect tenants to them with `subscribe()`. Like `Server`, the methods
    return the messages to send keyed by connection; an adapter such as `ws_endpoint(hub)` performs I/O.
    """

    def __init__(self, key: Callable[[Any], Any], *, default_codec: str = protocol.JSON) -> None:
        self._key = key
        self.default_codec = protocol.normalize_codec(default_codec)
        self._tenants: dict[Any, Session] = {}
        self._shared: dict[int, _Shared] = {}
        self._replica = f"hub-{uuid.uuid4().hex}"
        self._crdt_replica_owners: dict[tuple[int, str], Any] = {}
        self._next_shared = 0
        self._conn_key: dict[Any, Any] = {}
        self._conns_by_key: dict[Any, set[Any]] = {}
        self._codecs: dict[Any, str] = {}
        self._peer_ids: dict[Any, str] = {}
        self._awareness: dict[int, dict[Any, Any]] = {}
        self._shared_outbox: list[tuple] = []  # (sid, fan_patch) from host-side writes
        self._crdt_outbox: list[tuple[int, list[dict]]] = []
        self._snapshot_outbox: set[int] = set()
        self._on_shared_write: Callable | None = None
        self._last_crdt_write: tuple[Any, int, str] | None = None

    def tenant(self, key: Any) -> Session:
        """Get (or create) the `Session` holding a tenant's private models."""
        sess = self._tenants.get(key)
        if sess is None:
            sess = self._tenants[key] = Session()
        return sess

    def share(
        self,
        model_or_value: Any,
        type_name: str | None = None,
        *,
        merge: Any = LastWriteWins,
        replay: bool = False,
        rev: int = 0,
        merge_state: dict | None = None,
        crdt_spec: CrdtSpec | dict | None = None,
        crdt_state: dict | None = None,
        replica: str | None = None,
    ) -> int:
        """Register a shared data structure; returns its shared id.

        Pass a model instance (pydantic/dataclass/msgspec) to capture its value and type name, or a
        core `Value` dict together with `type_name`. `merge` is a `MergeStrategy` subclass (each shared
        model gets its own instance) or an instance to reuse. ``replay=True`` keeps a bounded patch log
        so a joining worker can catch up by delta (see :meth:`since_shared`) instead of a full snapshot.
        To **restore** a model from a durable checkpoint on startup, pass ``rev`` and ``merge_state``
        (from a prior :meth:`snapshot_shared`) alongside the saved value. Pass ``crdt_spec`` to use
        the shared schema-directed reducer instead of ``merge``; ``crdt_state`` restores its reducer
        metadata and ``replica`` overrides this hub's unique operation identity.
        """
        if type_name is None:
            type_name = type(model_or_value).__name__
            value = to_value(model_or_value)
        else:
            value = model_or_value
        sid = SHARED_ID_BASE + self._next_shared
        self._next_shared += 1
        strategy = merge() if isinstance(merge, type) else merge
        crdt = None
        if crdt_spec is not None and merge_state is not None:
            raise ValueError("merge_state cannot be combined with crdt_spec; pass crdt_state directly")
        if crdt_spec is None and merge_state is not None and "crdt_spec" in merge_state:
            crdt_spec = merge_state["crdt_spec"]
            crdt_state = merge_state.get("crdt_state")
        if crdt_state is not None and crdt_spec is None:
            raise ValueError("crdt_state requires crdt_spec")
        if crdt_spec is not None:
            spec = crdt_spec if isinstance(crdt_spec, CrdtSpec) else CrdtSpec.from_dict(crdt_spec)
            replica = replica or f"{self._replica}-{sid}"
            crdt = CrdtDocument.from_state(spec, crdt_state, replica) if crdt_state is not None else CrdtDocument(spec, _py_of(value), replica)
            value = _value_of(crdt.value)
        elif merge_state is not None:
            strategy.restore(merge_state)
        self._shared[sid] = _Shared(type_name, value, strategy, replay=replay, rev=rev, crdt=crdt)
        return sid

    def subscribe(self, tenant_key: Any, sid: int, mode: str = READ) -> None:
        """Subscribe a tenant to a shared model with `READ` or `WRITE` access."""
        if mode not in (READ, WRITE):
            raise ValueError(f"unknown mode: {mode}")
        self.tenant(tenant_key)  # ensure the tenant exists
        self._shared[sid].subs[tenant_key] = mode

    def _shared_write_allowed(self, tenant_key: Any, sid: int) -> bool:
        shared = self._shared.get(sid)
        return shared is not None and shared.subs.get(tenant_key) == WRITE

    def _encode_for(self, conn: Any, msg_json: str) -> Wire:
        return protocol.encode(msg_json, self._codecs.get(conn, self.default_codec))

    def _encode_many(self, conns: Any, msg_jsons: list[str]) -> dict[Any, list[Wire]]:
        groups: dict[str, list[Any]] = {}
        for conn in list(conns):
            codec = self._codecs.get(conn)
            if codec is not None:
                groups.setdefault(codec, []).append(conn)
        out: dict[Any, list[Wire]] = {}
        for codec, codec_conns in groups.items():
            try:
                encoded = [protocol.encode(msg_json, codec) for msg_json in msg_jsons]
            except Exception:  # noqa: BLE001
                for conn in codec_conns:
                    self.close(conn)
                continue
            for conn in codec_conns:
                out[conn] = encoded
        return out

    def open(self, conn: Any, codec: str | None = None, since: dict[int, int] | None = None, batch: bool = False) -> list[Wire]:
        # `batch` is accepted for endpoint parity but not yet applied: Hub.flush interleaves
        # per-tenant and shared fan-outs, so its batching lands with that restructure.
        """Register a connection; return the messages to bring it up to date — its tenant's private models
        and its subscribed shared models. With ``since={mid: last_rev}`` a reconnecting client resumes its
        **private** models from the delta (shared models re-snapshot — a shared replay log is a follow-on)."""
        key = self._key(conn)
        codec = protocol.normalize_codec(codec or self.default_codec)
        if conn in self._conn_key:
            previous = self._conn_key[conn]
            if previous != key:
                previous_conns = self._conns_by_key.get(previous)
                if previous_conns is not None:
                    previous_conns.discard(conn)
                    if not previous_conns:
                        self._conns_by_key.pop(previous)
        self._conn_key[conn] = key
        self._conns_by_key.setdefault(key, set()).add(conn)
        self._codecs[conn] = codec
        self._peer_ids.setdefault(conn, uuid.uuid4().hex)
        try:
            sess = self.tenant(key)
            out: list[Wire] = []
            for mid in sess.ids():
                client_rev = since.get(mid) if since else None
                delta = sess.since(mid, client_rev) if client_rev is not None else None
                if delta is not None:
                    for patch in delta:
                        out.append(self._encode_for(conn, protocol.patch_msg(mid, patch)))
                else:
                    snap = sess.snapshot(mid)
                    out.append(self._encode_for(conn, protocol.snapshot_msg(mid, snap["type_name"], snap["rev"], snap["value"])))
            for sid, sh in self._shared.items():
                if key in sh.subs:
                    if sh.crdt is None:
                        message = protocol.snapshot_msg(sid, sh.type_name, sh.rev, sh.value)
                    else:
                        message = protocol.crdt_snapshot_msg(
                            sid,
                            sh.type_name,
                            sh.rev,
                            sh.value,
                            sh.crdt.spec.to_dict(),
                            sh.crdt.state,
                        )
                    out.append(self._encode_for(conn, message))
                    for peer_conn, state in self._awareness.get(sid, {}).items():
                        if peer_conn != conn and peer_conn in self._peer_ids:
                            message = protocol.awareness_msg(sid, state, self._peer_ids[peer_conn])
                            out.append(self._encode_for(conn, message))
            return out
        except Exception:
            self.close(conn)
            raise

    def recv(self, conn: Any, data: Wire) -> dict[Any, list[Wire]]:
        """Handle an inbound patch; returns messages to send, keyed by connection.

        A patch to a private model is applied as the server (the tenant's session owns `rev`) and the
        authoritative patch is broadcast to *all* of that tenant's connections. A patch to a shared
        model (from a `WRITE` subscriber) is merged into the authoritative value and broadcast to
        every subscriber connection. Both paths are server-authoritative (origin included), with
        pending host patches returned before the proposal reply.
        """
        self._last_crdt_write = None
        codec = self._codecs.get(conn)
        if codec is None:
            return {}
        try:
            msg = protocol.decode(data, codec)
        except (TypeError, ValueError):
            return {}
        if msg.get("t") == "awareness":
            sid = msg.get("id")
            key = self._conn_key.get(conn)
            shared = self._shared.get(sid)
            if shared is None or key not in shared.subs:
                return {}
            peers = self._awareness.setdefault(sid, {})
            state = msg.get("state")
            if state is None:
                peers.pop(conn, None)
                if not peers:
                    self._awareness.pop(sid, None)
            else:
                peers[conn] = state
            message = protocol.awareness_msg(sid, state, self._peer_ids[conn])
            targets = (target for tenant in shared.subs for target in self._conns_by_key.get(tenant, ()) if target != conn)
            return self._encode_many(targets, [message])
        if msg.get("t") not in ("patch", "crdt"):
            return {}
        wire_id = msg["id"]
        proposal = msg.get("proposal")
        crdt_ops = msg.get("ops") if msg["t"] == "crdt" else None
        key = self._conn_key.get(conn)

        def finish(pending: dict[Any, list[tuple[int | None, Wire]]], direct: dict[Any, list[Wire]]) -> dict[Any, list[Wire]]:
            out = {target: [wire for _, wire in tagged] for target, tagged in pending.items()}
            for target, wires in direct.items():
                out.setdefault(target, []).extend(wires)
            return out

        def reject_shared(error: str, sh: _Shared | None = None) -> dict[Any, list[Wire]]:
            rev = sh.rev if sh is not None else 0
            messages = [protocol.reject_msg(wire_id, rev, error, proposal, crdt_ops)]
            if crdt_ops is not None and sh is not None and sh.crdt is not None:
                messages.append(
                    protocol.crdt_snapshot_msg(
                        wire_id,
                        sh.type_name,
                        sh.rev,
                        sh.value,
                        sh.crdt.spec.to_dict(),
                        sh.crdt.state,
                    )
                )
            return self._encode_many([conn], messages)

        if wire_id >= SHARED_ID_BASE:
            sh = self._shared.get(wire_id)
            if sh is None:
                return reject_shared("unknown shared model")
            if not self._shared_write_allowed(key, wire_id):
                # a read-only (or unsubscribed) tenant's write is refused; tell the proposer why
                return reject_shared("read-only subscription", sh)
            if msg["t"] == "crdt":
                if sh.crdt is None:
                    self._last_crdt_write = (conn, wire_id, "rejected")
                    return reject_shared("model is not CRDT-backed", sh)
                if not isinstance(crdt_ops, list):
                    self._last_crdt_write = (conn, wire_id, "rejected")
                    return reject_shared("CRDT operations must be a list", sh)
                replicas = set()
                for op in crdt_ops:
                    if isinstance(op, dict) and isinstance(dot := op.get("dot"), dict):
                        replica = dot.get("replica")
                        if isinstance(replica, str):
                            replicas.add(replica)
                conflicting = next(
                    (
                        replica
                        for replica in replicas
                        if (wire_id, replica) in self._crdt_replica_owners and self._crdt_replica_owners[(wire_id, replica)] != key
                    ),
                    None,
                )
                if conflicting is not None:
                    self._last_crdt_write = (conn, wire_id, "rejected")
                    return reject_shared(f"CRDT replica {conflicting!r} belongs to another writer", sh)
                try:
                    result = self._write_shared_crdt(wire_id, crdt_ops)
                except (TypeError, ValueError) as error:
                    self._last_crdt_write = (conn, wire_id, "rejected")
                    return reject_shared(str(error), sh)
                for replica in replicas:
                    self._crdt_replica_owners.setdefault((wire_id, replica), key)
                if result is not None:
                    self._last_crdt_write = (conn, wire_id, "applied")
                    direct = self._fanout_crdt(wire_id, crdt_ops, origin=conn, proposal=proposal)
                    return finish(self._flush_crdt_tagged(wire_id), direct)
                self._last_crdt_write = (conn, wire_id, "duplicate")
                duplicate = protocol.crdt_msg(wire_id, crdt_ops, rev=sh.rev, proposal=proposal)
                direct = self._encode_many([conn], [duplicate])
                return finish(self._flush_crdt_tagged(wire_id), direct)
            if sh.crdt is not None:
                reject = protocol.reject_msg(wire_id, sh.rev, "CRDT-backed models require CRDT operations", proposal)
                return self._encode_many([conn], [reject])
            fan = self._write_shared(wire_id, msg["patch"], origin=key)
            if fan:
                direct = self._fanout(wire_id, fan, origin=conn, proposal=proposal)
                return finish(self._flush_shared_tagged(wire_id), direct)
            if proposal is not None:
                direct = self._encode_many([conn], [protocol.ack_msg(wire_id, sh.rev, proposal)])
                return finish(self._flush_shared_tagged(wire_id), direct)
            return finish(self._flush_shared_tagged(wire_id), {})
        if msg["t"] == "crdt":
            reject = protocol.reject_msg(wire_id, 0, "CRDT operations require a shared model", proposal, crdt_ops)
            return self._encode_many([conn], [reject])
        sess = self._tenants.get(key)
        if sess is None:
            return {}
        authoritative = sess.submit(wire_id, msg["patch"])
        if authoritative is None:
            # Rejected (invalid edit / malformed patch): revert just the proposer to the authoritative
            # state so its UI self-corrects, followed by a typed `reject` frame saying why; the host
            # never crashes and other tenants are untouched.
            error = sess.reject_reason or "rejected"
            try:
                snap = sess.snapshot(wire_id)
            except KeyError:
                reject = protocol.reject_msg(wire_id, 0, error, proposal)
                direct = self._encode_many([conn], [reject])
            else:
                revert = protocol.snapshot_msg(wire_id, snap["type_name"], snap["rev"], snap["value"])
                reject = protocol.reject_msg(wire_id, snap["rev"], error, proposal)
                direct = self._encode_many([conn], [revert, reject])
        else:
            relay = protocol.patch_msg(wire_id, authoritative)
            conns = self._conns_by_key.get(key, ())
            if proposal is None:
                direct = self._encode_many(conns, [relay])
            else:
                direct = self._encode_many((target for target in conns if target != conn), [relay])
                direct.update(self._encode_many([conn], [protocol.patch_msg(wire_id, authoritative, proposal)]))
        return finish(self._flush_tenant_tagged(key), direct)

    def flush(self) -> dict[Any, list[Wire]]:
        """Drain every tenant session and any host-side shared writes; route the patches per tenant/subscription."""
        return {conn: [wire for _, wire in tagged] for conn, tagged in self._flush_tagged().items()}

    def _flush_tagged(self) -> dict[Any, list[tuple[int | None, Wire]]]:
        """`flush`, with each message tagged by its model id so `autosync` can coalesce (see
        `Server._flush_tagged`). Shared ids live above ``SHARED_ID_BASE``, so a connection's tenant
        and shared tags never collide."""
        out: dict[Any, list[tuple[int | None, Wire]]] = {}
        for key in self._tenants:
            for conn, tagged in self._flush_tenant_tagged(key).items():
                out.setdefault(conn, []).extend(tagged)
        for conn, tagged in self._flush_snapshots_tagged().items():
            out.setdefault(conn, []).extend(tagged)
        for conn, tagged in self._flush_shared_tagged().items():
            out.setdefault(conn, []).extend(tagged)
        for conn, tagged in self._flush_crdt_tagged().items():
            out.setdefault(conn, []).extend(tagged)
        return out

    def _flush_snapshots_tagged(self) -> dict[Any, list[tuple[int | None, Wire]]]:
        out: dict[Any, list[tuple[int | None, Wire]]] = {}
        for sid in self._snapshot_outbox:
            sh = self._shared.get(sid)
            if sh is None:
                continue
            if sh.crdt is None:
                message = protocol.snapshot_msg(sid, sh.type_name, sh.rev, sh.value)
            else:
                message = protocol.crdt_snapshot_msg(sid, sh.type_name, sh.rev, sh.value, sh.crdt.spec.to_dict(), sh.crdt.state)
            conns = (conn for key in sh.subs for conn in self._conns_by_key.get(key, ()))
            for conn, messages in self._encode_many(conns, [message]).items():
                out.setdefault(conn, []).extend((sid, wire) for wire in messages)
        self._snapshot_outbox.clear()
        return out

    def _flush_tenant_tagged(self, key: Any) -> dict[Any, list[tuple[int | None, Wire]]]:
        sess = self._tenants.get(key)
        if sess is None:
            return {}
        drained = sess.drain()
        conns = self._conns_by_key.get(key)
        if not drained or not conns:
            return {}
        msgs = [(mid, protocol.patch_msg(mid, patch)) for mid, patch in drained]
        groups: dict[str, list[Any]] = {}
        for conn in list(conns):
            codec = self._codecs.get(conn)
            if codec is not None:
                groups.setdefault(codec, []).append(conn)
        out: dict[Any, list[tuple[int | None, Wire]]] = {}
        for codec, codec_conns in groups.items():
            try:
                encoded = [(mid, protocol.encode(msg, codec)) for mid, msg in msgs]
            except Exception:  # noqa: BLE001
                for conn in codec_conns:
                    self.close(conn)
                continue
            for conn in codec_conns:
                out[conn] = encoded
        return out

    def _flush_shared_tagged(self, only_sid: int | None = None) -> dict[Any, list[tuple[int | None, Wire]]]:
        selected = self._shared_outbox if only_sid is None else [item for item in self._shared_outbox if item[0] == only_sid]
        out: dict[Any, list[tuple[int | None, Wire]]] = {}
        for sid, fan in selected:
            for c, msgs in self._fanout(sid, fan).items():
                out.setdefault(c, []).extend((sid, wire) for wire in msgs)
        if only_sid is None:
            self._shared_outbox.clear()
        else:
            self._shared_outbox = [item for item in self._shared_outbox if item[0] != only_sid]
        return out

    def _flush_crdt_tagged(self, only_sid: int | None = None) -> dict[Any, list[tuple[int | None, Wire]]]:
        selected = self._crdt_outbox if only_sid is None else [item for item in self._crdt_outbox if item[0] == only_sid]
        out: dict[Any, list[tuple[int | None, Wire]]] = {}
        for sid, ops in selected:
            for conn, messages in self._fanout_crdt(sid, ops).items():
                out.setdefault(conn, []).extend((sid, wire) for wire in messages)
        if only_sid is None:
            self._crdt_outbox.clear()
        else:
            self._crdt_outbox = [item for item in self._crdt_outbox if item[0] != only_sid]
        return out

    def set_shared(self, sid: int, new_value_or_model: Any) -> None:
        """Write to a shared model from the host side; the change is broadcast on the next `sync`/`autosync`."""
        if self._shared[sid].crdt is not None:
            raise ValueError("use mutate_shared_crdt for a CRDT-backed shared model")
        value = new_value_or_model if isinstance(new_value_or_model, dict) else to_value(new_value_or_model)
        patch = json.loads(_diff(json.dumps(self._shared[sid].value), json.dumps(value)))
        if not patch["ops"]:
            return
        fan = self._write_shared(sid, patch, origin="<host>")
        if fan:
            self._shared_outbox.append((sid, fan))

    def apply_shared(self, sid: int, patch: dict, origin: Any) -> None:
        """Merge a shared-model write that happened on another worker (delivered over a backplane), and
        queue the resulting authoritative fan for this worker's subscribers on the next `flush`. Do not
        re-publish — the originating worker already broadcast it. When the model's `MergeStrategy` is a
        CRDT this is convergent: applying the same set of writes in any order yields the same value, so
        concurrent edits from clients on different workers reconcile identically everywhere."""
        if self._shared.get(sid) is None:
            return
        if self._shared[sid].crdt is not None:
            return
        fan = self._write_shared(sid, patch, origin)
        if fan:
            self._shared_outbox.append((sid, fan))

    def mutate_shared_crdt(self, sid: int, mutations: list[dict]) -> list[dict]:
        """Apply host-side CRDT mutations and queue their operations for subscriber fan-out."""
        sh = self._shared[sid]
        if sh.crdt is None:
            raise ValueError("shared model is not CRDT-backed")
        change = sh.crdt.mutate(mutations)
        if change["ops"]:
            self._commit_shared_crdt(sid, change["ops"], change["effect"])
            self._crdt_outbox.append((sid, change["ops"]))
        return change["ops"]

    def apply_crdt_shared(self, sid: int, ops: list[dict], origin: Any = None) -> None:
        """Apply CRDT operations received from another worker and queue local subscriber fan-out."""
        sh = self._shared.get(sid)
        if sh is None or sh.crdt is None:
            return
        result = self._write_shared_crdt(sid, ops)
        if result is not None:
            if origin is not None:
                for op in ops:
                    if isinstance(op, dict) and isinstance(dot := op.get("dot"), dict) and isinstance(replica := dot.get("replica"), str):
                        self._crdt_replica_owners.setdefault((sid, replica), origin)
            self._crdt_outbox.append((sid, ops))

    def compact_shared_crdt(self, sid: int, frontier: dict[str, int]) -> int:
        """Compact causally stable reducer metadata for a shared CRDT model."""
        sh = self._shared[sid]
        if sh.crdt is None:
            raise ValueError("shared model is not CRDT-backed")
        before = sh.crdt.state
        compacted = sh.crdt.compact(frontier)
        if sh.crdt.state != before and self._on_shared_write is not None:
            state = {"crdt_spec": sh.crdt.spec.to_dict(), "crdt_state": sh.crdt.state}
            change = {"crdt_compacted": dict(frontier)}
            self._on_shared_write(sid, sh.type_name, sh.value, sh.rev, change, state)
        return compacted

    def on_shared_write(self, callback: Callable | None) -> None:
        """Register a callback fired after each authoritative shared write, with
        ``(sid, type_name, value, rev, change, merge_state)``. For CRDT-backed models, ``change``
        contains ``crdt_ops`` and ``effect``, or ``crdt_compacted`` for a same-revision compaction,
        while ``merge_state`` contains the CRDT specification and reducer state. transports stores
        nothing durably; persist these to survive a full-cluster restart, then restore with
        ``share(value=…, rev=…, merge_state=…)``. Gate on a single writer (e.g. the relay's leader) if
        you don't want every worker persisting the same change."""
        self._on_shared_write = callback

    def snapshot_shared(self, sid: int) -> dict:
        """The full transferable/persistable state of a shared model: ``value``, ``rev`` and the merge
        clock (``merge_state``). Used by the relay to catch up a joining worker, and by users to
        checkpoint for durability."""
        sh = self._shared[sid]
        snapshot = {
            "type_name": sh.type_name,
            "value": sh.value,
            "rev": sh.rev,
            "merge_state": sh.merge.state(),
        }
        if sh.crdt is not None:
            crdt_spec = sh.crdt.spec.to_dict()
            crdt_state = sh.crdt.state
            snapshot["crdt_spec"] = crdt_spec
            snapshot["crdt_state"] = crdt_state
            snapshot["merge_state"] = {"crdt_spec": crdt_spec, "crdt_state": crdt_state}
        return snapshot

    def since_shared(self, sid: int, since_rev: int) -> list[dict] | None:
        """Patches after `since_rev` for a delta catch-up, or ``None`` if it is outside the kept log (the
        caller should fall back to a snapshot). Requires ``share(replay=True)``. Mirrors `Session.since`."""
        sh = self._shared.get(sid)
        if sh is None or not sh.replay:
            return None
        if since_rev >= sh.rev:
            return []
        if not sh.log or sh.log[0][0] > since_rev + 1:
            return None  # the needed delta has scrolled out of the bounded log
        return [p for r, p in sh.log if r > since_rev]

    def apply_snapshot_shared(
        self,
        sid: int,
        value: dict,
        rev: int,
        merge_state: dict | None,
        crdt_spec: dict | None = None,
        crdt_state: dict | None = None,
    ) -> bool:
        """Adopt a peer's snapshot of a shared model: set value/rev and restore the merge clock, so later
        merges respect the transferred causal stamps."""
        sh = self._shared.get(sid)
        if sh is None:
            return False
        if sh.crdt is not None and (crdt_spec is None or crdt_state is None):
            return False
        sh.value = value
        sh.rev = max(sh.rev, rev)
        if crdt_spec is not None and crdt_state is not None:
            replica = sh.crdt.replica if sh.crdt is not None else f"{self._replica}-{sid}"
            sh.crdt = CrdtDocument.from_state(CrdtSpec.from_dict(crdt_spec), crdt_state, replica)
            sh.value = _value_of(sh.crdt.value)
        else:
            sh.merge.restore(merge_state or {})
        self._snapshot_outbox.add(sid)
        return True

    def apply_delta_shared(self, sid: int, patches: list[dict], rev: int, merge_state: dict | None) -> bool:
        """Catch up by applying replay patches onto the current (restored) value, then restoring the merge
        clock — cheaper than a snapshot when a recent checkpoint is held."""
        sh = self._shared.get(sid)
        if sh is None or sh.crdt is not None:
            return False
        v = sh.value
        for patch in patches:
            v = json.loads(_apply(json.dumps(v), json.dumps(patch)))
        sh.value = v
        sh.rev = max(sh.rev, rev)
        sh.merge.restore(merge_state or {})
        return True

    def close(self, conn: Any) -> dict[Any, list[Wire]]:
        peer = self._peer_ids.pop(conn, None)
        removed = [sid for sid, states in self._awareness.items() if conn in states]
        for sid in removed:
            states = self._awareness[sid]
            states.pop(conn, None)
            if not states:
                self._awareness.pop(sid)
        sentinel = object()
        key = self._conn_key.pop(conn, sentinel)
        if key is not sentinel:
            conns = self._conns_by_key.get(key)
            if conns is not None:
                conns.discard(conn)
                if not conns:
                    self._conns_by_key.pop(key)
        self._codecs.pop(conn, None)
        out: dict[Any, list[Wire]] = {}
        if peer is not None:
            for sid in removed:
                shared = self._shared.get(sid)
                if shared is None:
                    continue
                targets = (target for tenant in shared.subs for target in self._conns_by_key.get(tenant, ()))
                for target, messages in self._encode_many(targets, [protocol.awareness_msg(sid, None, peer)]).items():
                    out.setdefault(target, []).extend(messages)
        return out

    def _write_shared(self, sid: int, patch: dict, origin: Any) -> dict | None:
        """Merge a write into a shared model; return the authoritative fan-out patch (or None)."""
        sh = self._shared[sid]
        new = sh.merge.merge(sh.value, patch, origin)
        fan = json.loads(_diff(json.dumps(sh.value), json.dumps(new)))
        if not fan["ops"]:
            return None
        sh.value = new
        sh.rev += 1
        fan["rev"] = sh.rev
        if sh.replay:
            sh.log.append((sh.rev, fan))
            if len(sh.log) > sh.log_cap:
                del sh.log[: len(sh.log) - sh.log_cap]
        if self._on_shared_write is not None:
            # durability seam: the user persists this so the model survives a full-cluster restart.
            self._on_shared_write(sid, sh.type_name, sh.value, sh.rev, fan, sh.merge.state())
        return fan

    def _write_shared_crdt(self, sid: int, ops: list[dict]) -> dict | None:
        sh = self._shared[sid]
        if sh.crdt is None:
            raise ValueError("shared model is not CRDT-backed")
        effect = sh.crdt.apply(ops)
        if effect["applied"] == 0:
            return None
        self._commit_shared_crdt(sid, ops, effect)
        return effect

    def _commit_shared_crdt(self, sid: int, ops: list[dict], effect: dict) -> None:
        sh = self._shared[sid]
        if sh.crdt is None:
            raise ValueError("shared model is not CRDT-backed")
        sh.value = _value_of(sh.crdt.value)
        sh.rev += 1
        if self._on_shared_write is not None:
            state = {"crdt_spec": sh.crdt.spec.to_dict(), "crdt_state": sh.crdt.state}
            change = {"crdt_ops": ops, "effect": effect}
            self._on_shared_write(sid, sh.type_name, sh.value, sh.rev, change, state)

    def _fanout(self, sid: int, fan: dict, *, origin: Any = None, proposal: str | None = None) -> dict[Any, list[Wire]]:
        sh = self._shared[sid]
        msg = protocol.patch_msg(sid, fan)
        conns: list[Any] = []
        for key in sh.subs:
            conns.extend(self._conns_by_key.get(key, ()))
        if proposal is None or origin is None:
            return self._encode_many(conns, [msg])
        out = self._encode_many((conn for conn in conns if conn != origin), [msg])
        out.update(self._encode_many([origin], [protocol.patch_msg(sid, fan, proposal)]))
        return out

    def _fanout_crdt(
        self,
        sid: int,
        ops: list[dict],
        *,
        origin: Any = None,
        proposal: str | None = None,
    ) -> dict[Any, list[Wire]]:
        sh = self._shared[sid]
        message = protocol.crdt_msg(sid, ops, rev=sh.rev)
        conns: list[Any] = []
        for key in sh.subs:
            conns.extend(self._conns_by_key.get(key, ()))
        if proposal is None or origin is None:
            return self._encode_many(conns, [message])
        out = self._encode_many((conn for conn in conns if conn != origin), [message])
        origin_message = protocol.crdt_msg(sid, ops, rev=sh.rev, proposal=proposal)
        out.update(self._encode_many([origin], [origin_message]))
        return out
