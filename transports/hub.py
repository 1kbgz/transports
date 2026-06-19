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
connection — so it is unit-testable without a network; `Hub.endpoint()` adapts it to Starlette.

Shared models are **server-authoritative**: a writer sends its edit and receives the authoritative
patch back.
"""

import json
from typing import Any, Callable, Dict, List, Optional, Union

from . import protocol
from ._bridge import to_value
from .server import Wire, _send
from .session import Session
from .transports import apply as _apply, diff as _diff

READ = "read"
WRITE = "write"

#: Shared-model wire ids live above this base so they never collide with per-`Session` model ids
#: (which start at 1 in each tenant's own store).
SHARED_ID_BASE = 1 << 40


# --- merge strategies ----------------------------------------------------------------------------


class MergeStrategy:
    """How a write to a shared model is reconciled into its authoritative value.

    `merge(current, patch, origin)` returns the new core `Value`. Implementations may be stateful;
    pass the **class** (not an instance) to `Hub.share(merge=...)` so each shared model gets its own
    instance and its own state.
    """

    def merge(self, current: Any, patch: dict, origin: Any) -> Any:  # pragma: no cover - interface
        raise NotImplementedError


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
        self._clock: Dict[str, tuple] = {}

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


# --- hub ------------------------------------------------------------------------------------------


class _Shared:
    """Authoritative state for a shared data structure."""

    def __init__(self, type_name: str, value: dict, merge: MergeStrategy) -> None:
        self.type_name = type_name
        self.value = value
        self.rev = 0
        self.merge = merge
        self.subs: Dict[Any, str] = {}  # tenant key -> mode


class Hub:
    """Route connections to per-tenant `Session` objects and fan shared data structures to subscribers.

    Construct with `key`, a function mapping a connection handle to its tenant key. Register shared
    models with `share()` and connect tenants to them with `subscribe()`. Like `Server`, the methods
    return the messages to send keyed by connection; `endpoint()` performs the I/O over Starlette.
    """

    def __init__(self, key: Callable[[Any], Any], *, default_codec: str = protocol.JSON) -> None:
        self._key = key
        self._default_codec = protocol.normalize_codec(default_codec)
        self._tenants: Dict[Any, Session] = {}
        self._shared: Dict[int, _Shared] = {}
        self._next_shared = 0
        self._conn_key: Dict[Any, Any] = {}
        self._codecs: Dict[Any, str] = {}
        self._shared_outbox: List[tuple] = []  # (sid, fan_patch) from host-side writes

    # --- registration ----------------------------------------------------------------------------

    def tenant(self, key: Any) -> Session:
        """Get (or create) the `Session` holding a tenant's private models."""
        sess = self._tenants.get(key)
        if sess is None:
            sess = self._tenants[key] = Session()
        return sess

    def share(self, model_or_value: Any, type_name: Optional[str] = None, *, merge: Any = LastWriteWins) -> int:
        """Register a shared data structure; returns its shared id.

        Pass a model instance (pydantic/dataclass/msgspec) to capture its value and type name, or a
        core `Value` dict together with `type_name`. `merge` is a `MergeStrategy` subclass (each
        shared model gets its own instance) or an instance to reuse.
        """
        if type_name is None:
            type_name = type(model_or_value).__name__
            value = to_value(model_or_value)
        else:
            value = model_or_value
        strategy = merge() if isinstance(merge, type) else merge
        sid = SHARED_ID_BASE + self._next_shared
        self._next_shared += 1
        self._shared[sid] = _Shared(type_name, value, strategy)
        return sid

    def subscribe(self, tenant_key: Any, sid: int, mode: str = READ) -> None:
        """Subscribe a tenant to a shared model with `READ` or `WRITE` access."""
        if mode not in (READ, WRITE):
            raise ValueError(f"unknown mode: {mode}")
        self.tenant(tenant_key)  # ensure the tenant exists
        self._shared[sid].subs[tenant_key] = mode

    # --- per-connection encoding -----------------------------------------------------------------

    def _encode_for(self, conn: Any, msg_json: str) -> Wire:
        return protocol.encode(msg_json, self._codecs.get(conn, self._default_codec))

    # --- connection lifecycle --------------------------------------------------------------------

    def open(self, conn: Any, codec: Optional[str] = None) -> List[Wire]:
        """Register a connection; returns the snapshots of its tenant's private + subscribed shared models."""
        key = self._key(conn)
        self._conn_key[conn] = key
        self._codecs[conn] = protocol.normalize_codec(codec or self._default_codec)
        sess = self.tenant(key)
        out: List[Wire] = []
        for mid in sess.ids():
            snap = sess.snapshot(mid)
            out.append(self._encode_for(conn, protocol.snapshot_msg(mid, snap["type_name"], snap["rev"], snap["value"])))
        for sid, sh in self._shared.items():
            if key in sh.subs:
                out.append(self._encode_for(conn, protocol.snapshot_msg(sid, sh.type_name, sh.rev, sh.value)))
        return out

    def recv(self, conn: Any, data: Wire) -> Dict[Any, List[Wire]]:
        """Handle an inbound patch; returns messages to send, keyed by connection.

        A patch to a private model is applied to the tenant's session and relayed to that tenant's
        *other* connections. A patch to a shared model (from a `WRITE` subscriber) is merged into the
        authoritative value and the resulting patch is broadcast to every subscriber connection.
        """
        msg = protocol.decode(data)
        if msg.get("t") != "patch":
            return {}
        wire_id = msg["id"]
        key = self._conn_key.get(conn)
        if wire_id >= SHARED_ID_BASE:
            sh = self._shared.get(wire_id)
            if sh is None or sh.subs.get(key) != WRITE:
                return {}  # unknown shared model, or this tenant may not write it
            fan = self._write_shared(wire_id, msg["patch"], origin=key)
            return self._fanout(wire_id, fan) if fan else {}
        sess = self._tenants.get(key)
        if sess is None:
            return {}
        sess.apply_patch(wire_id, msg["patch"])
        relay = protocol.patch_msg(wire_id, msg["patch"])
        return {c: [self._encode_for(c, relay)] for c, k in self._conn_key.items() if k == key and c is not conn}

    def flush(self) -> Dict[Any, List[Wire]]:
        """Drain every tenant session and any host-side shared writes; route the patches per tenant/subscription."""
        out: Dict[Any, List[Wire]] = {}
        for key, sess in self._tenants.items():
            drained = sess.drain()
            if not drained:
                continue
            conns = [c for c, k in self._conn_key.items() if k == key]
            for c in conns:
                for mid, patch in drained:
                    out.setdefault(c, []).append(self._encode_for(c, protocol.patch_msg(mid, patch)))
        for sid, fan in self._shared_outbox:
            for c, msgs in self._fanout(sid, fan).items():
                out.setdefault(c, []).extend(msgs)
        self._shared_outbox.clear()
        return out

    def set_shared(self, sid: int, new_value_or_model: Any) -> None:
        """Write to a shared model from the host side; the change is broadcast on the next `flush()`."""
        value = new_value_or_model if isinstance(new_value_or_model, dict) else to_value(new_value_or_model)
        patch = json.loads(_diff(json.dumps(self._shared[sid].value), json.dumps(value)))
        if not patch["ops"]:
            return
        fan = self._write_shared(sid, patch, origin="<host>")
        if fan:
            self._shared_outbox.append((sid, fan))

    def close(self, conn: Any) -> None:
        self._conn_key.pop(conn, None)
        self._codecs.pop(conn, None)

    # --- shared write internals ------------------------------------------------------------------

    def _write_shared(self, sid: int, patch: dict, origin: Any) -> Optional[dict]:
        """Merge a write into a shared model; return the authoritative fan-out patch (or None)."""
        sh = self._shared[sid]
        new = sh.merge.merge(sh.value, patch, origin)
        fan = json.loads(_diff(json.dumps(sh.value), json.dumps(new)))
        if not fan["ops"]:
            return None
        sh.value = new
        sh.rev += 1
        fan["rev"] = sh.rev
        return fan

    def _fanout(self, sid: int, fan: dict) -> Dict[Any, List[Wire]]:
        sh = self._shared[sid]
        msg = protocol.patch_msg(sid, fan)
        return {c: [self._encode_for(c, msg)] for c, k in self._conn_key.items() if k in sh.subs}

    # --- async I/O adapter -----------------------------------------------------------------------

    def endpoint(self):
        """Build a Starlette WebSocket endpoint that serves this hub (tenant from `key(websocket)`)."""

        async def endpoint(websocket: Any) -> None:
            from starlette.websockets import WebSocketDisconnect

            codec = websocket.query_params.get("codec", self._default_codec)
            await websocket.accept()
            for msg in self.open(websocket, codec):
                await _send(websocket, msg)
            try:
                while True:
                    frame = await websocket.receive()
                    if frame.get("type") == "websocket.disconnect":
                        break
                    payload: Union[str, bytes, None] = frame.get("text")
                    if payload is None:
                        payload = frame.get("bytes")
                    if payload is None:
                        continue
                    for conn, msgs in self.recv(websocket, payload).items():
                        for msg in msgs:
                            await _send(conn, msg)
            except WebSocketDisconnect:
                pass
            finally:
                self.close(websocket)

        return endpoint
