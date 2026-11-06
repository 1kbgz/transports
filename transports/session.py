"""Reactive session: host pydantic models, observe mutations with bigbrother, emit core patches.

Mutations are observed recursively via `bigbrother.watch(model, cb, deepstate=True)`. bigbrother's
callback fires *before* the underlying mutation is applied, so the callback only marks the model
dirty; `flush()` (also invoked by `drain()`/`snapshot()`) recomputes each dirty model's `Value`,
diffs it against the core's held value, and emits the minimal `Patch`. Because emission is deferred
to a flush, several writes between flushes coalesce into a single patch.

This is the single-owner reactive nucleus; the multi-tenant session (in the Rust core) will
back it with fan-out, backpressure, and authorization.
"""

import json
import logging
import weakref
from collections.abc import Callable
from typing import Any, cast

from bigbrother import watch
from pydantic import BaseModel

from ._bridge import _annotations, _field_names, _py_of, from_value, schema_of, to_value
from ._owned_models import decode_owned, server_validator
from .transports import FieldAuthority, OwnedFields, ReplayLog, Store as _CoreStore, apply as _apply, diff as _diff


class Session:
    def __init__(self, *, log_cap: int = 512, log_bytes: int = 64 * 1024 * 1024, blob_threshold: int = 64 * 1024, command_cap: int = 1024) -> None:
        if min(log_cap, log_bytes, blob_threshold, command_cap) < 0:
            raise ValueError("replay limits must be non-negative")
        self._log_bytes = log_bytes
        self._blob_threshold = blob_threshold
        self._store = _CoreStore()
        self._models: dict[int, Any] = {}
        self._authorities: dict[int, FieldAuthority] = {}
        self._owned_fields: dict[int, set[str]] = {}
        self._owned_mirrors: dict[int, dict] = {}
        self._owned_names: dict[int, dict[str, str]] = {}
        self._field_guard = OwnedFields()
        self._command_cap = command_cap
        self._list_keys: dict[int, str | None] = {}
        self._schemas: dict[str, dict] = {}
        self._dirty: set[int] = set()
        self._suppress: set[int] = set()  # ids whose observation is suspended (during inbound apply)
        self.outbox: list[tuple[int, dict]] = []
        self._on_patch: Callable[[int, dict], None] | None = None
        self._log: dict[int, ReplayLog] = {}  # per-model replay log of (rev, patch), bounded
        self._log_cap = log_cap  # patches retained per model for resume; older are evicted (resume → snapshot)
        #: why the last `submit` was rejected (None after a successful submit) — the model's validation
        #: message where available, so callers can put it on the wire (the `reject` frame)
        self.reject_reason: str | None = None

    def host(self, model: Any, *, list_keys: list[dict] | None = None, owner: str = "server") -> int:
        """Host and watch a model. Optional list_keys records define exact list and item-key paths."""
        type_name = type(model).__name__
        schema = self._schemas[type_name] = schema_of(type(model), owner=owner)
        policy = json.dumps(list_keys) if list_keys is not None else None
        mid = self._store.host(type_name, json.dumps(to_value(model)), list_keys=policy)
        self._list_keys[mid] = policy
        fields = {field["name"] for field in schema["fields"] if field.get("owner") == "client"}
        if fields:
            self._owned_fields[mid] = fields
            self._owned_names[mid] = {wire: name for wire, name in _field_names(type(model)).items() if wire in fields}
            self._authorities[mid] = FieldAuthority(mid, json.dumps(sorted(fields)), self._command_cap)
            self._field_guard.receive(self._authorities[mid].grant(""))

        session_ref = weakref.ref(self)

        def _watcher(obj: object, method: str, ref: object, call_args: tuple, call_kwargs: dict, _mid: int = mid) -> None:
            session = session_ref()
            if session is not None and _mid not in session._suppress:
                if call_args and isinstance(call_args[0], str) and call_args[0] in session._owned_names.get(_mid, {}).values():
                    hosted = session._models.get(_mid)
                    if obj is hosted or obj is getattr(hosted, "__dict__", None):
                        raise ValueError(f"{call_args[0]!r} is client-owned; use Session.command()")
                session._dirty.add(_mid)

        self._models[mid] = watch(model, _watcher, deepstate=True)
        if fields:
            self._owned_mirrors[mid] = to_value(model, include=set(self._owned_names[mid].values()))["Map"]
        return mid

    def ids(self) -> list[int]:
        """The ids of all hosted models."""
        return list(self._models.keys())

    def on_patch(self, fn: Callable[[int, dict], None]) -> None:
        """Register a callback invoked as `fn(model_id, patch)` for each emitted patch."""
        self._on_patch = fn

    def flush(self) -> list[tuple[int, dict]]:
        """Diff every dirty model against the core and emit the minimal patches. Returns them."""
        for mid in self._dirty & self._owned_fields.keys():
            self._check_owned(mid)
        emitted: list[tuple[int, dict]] = []
        for mid in sorted(self._dirty):
            value = to_value(self._models[mid])
            for name in self._owned_fields.get(mid, ()):
                value["Map"][name] = json.loads(cast(str, self._store.field(mid, name)))
            patch_json = self._store.mutate(mid, json.dumps(value))
            if patch_json is None:
                continue
            patch = json.loads(patch_json)
            if patch["ops"]:
                self.outbox.append((mid, patch))
                emitted.append((mid, patch))
                self._record(mid, patch)
                if self._on_patch is not None:
                    self._on_patch(mid, patch)
        self._dirty.clear()
        return emitted

    def _check_owned(self, mid: int, *, strict: bool = False) -> None:
        fields = self._owned_fields.get(mid)
        if not fields:
            return
        live = to_value(self._models[mid], include=set(self._owned_names[mid].values()))["Map"]
        changed = [name for name in fields if live[name] != self._owned_mirrors[mid][name]]
        if changed:
            self._refresh_owned(mid, {name: json.loads(cast(str, self._store.field(mid, name))) for name in changed})
            message = f"client-owned fields cannot be assigned from Python: {changed}; use Session.command()"
            if strict:
                raise ValueError(message)
            import warnings

            warnings.warn(message, RuntimeWarning, stacklevel=2)

    def update(self, mid: int) -> list[tuple[int, dict]]:
        """Force a diff+emit for one hosted model and flush.

        Automatic emission needs bigbrother to observe the model; models without a ``__dict__``
        (``msgspec.Struct`` and other ``__slots__`` types) can't be watched, so mutate them and then
        call ``update(id)`` explicitly.
        """
        self._dirty.add(mid)
        self._check_owned(mid, strict=True)
        return self.flush()

    def drain(self) -> list[tuple[int, dict]]:
        """Flush, then return and clear the accumulated outbox."""
        self.flush()
        out, self.outbox = self.outbox, []
        return out

    def snapshot(self, mid: int) -> dict:
        """`{"type_name":.., "rev":.., "value":..}` for a hosted model (flushes pending changes)."""
        self.flush()
        snap = self._store.snapshot(mid)
        if snap is None:
            raise KeyError(mid)
        return json.loads(snap)

    def value(self, mid: int) -> dict:
        """The current core `Value` of a hosted model."""
        return self.snapshot(mid)["value"]

    def apply_patch(self, mid: int, patch: dict) -> bool:
        """Apply an authoritative remote patch to a hosted/mirrored model (adopts the patch's `rev`).

        Also refreshes the hosted Python object so the caller's own reference stays in sync — the
        update is made under observation suppression so it doesn't echo back as a new patch.
        """
        return self._apply_authoritative(mid, patch)

    def submit(self, mid: int, patch: dict) -> dict | None:
        """Apply a client-proposed patch *as the server*: the server owns `rev`.

        The proposal's ops are applied to the hosted value, the server's `rev` is bumped (not the
        client's guess), the hosted Python object is refreshed, and the authoritative patch (with the
        server `rev`) is returned to broadcast to every connection. `None` if the id is unknown or the
        proposal is rejected — then `reject_reason` says why.
        """
        self.reject_reason = None
        self.flush()
        snap = self._store.snapshot(mid)
        if snap is None:
            self.reject_reason = f"unknown model {mid}"
            return None
        parsed = json.loads(snap)
        cur = parsed["value"]
        # Validate AND canonicalize the proposal through the hosted model (on a copy, so a bad edit never
        # touches the store): pydantic/msgspec reject an invalid value, and a *coercible* one is normalized
        # to its canonical typed form — e.g. a number control sends the string "80", and the stored value
        # becomes the int 80, so the core value, the Python model, and every client agree. The authoritative
        # patch is the diff to that canonical value (not the client's raw ops), keeping all three in sync.
        try:
            self._field_guard.classify(mid, json.dumps(patch.get("ops", [])))
        except ValueError as error:
            self.reject_reason = str(error)
            return None
        canonical, error = self._canonical(mid, cur, patch.get("ops", []))
        if canonical is None:
            # invalid / malformed — rejected (the caller re-sends state so the proposer reverts)
            self.reject_reason = error or "invalid edit"
            return None
        ops = json.loads(_diff(json.dumps(cur), json.dumps(canonical), list_keys=self._list_keys.get(mid))).get("ops", [])
        authoritative = {"rev": parsed["rev"] + 1, "ops": ops}
        if not self._apply_authoritative(mid, authoritative):
            self.reject_reason = "malformed patch"
            return None
        self._record(mid, authoritative)
        return authoritative

    def command(self, mid: int, field: str, value: Any) -> int:
        """Queue an ordered command for the field's owner, including before any client connects.

        Delivery is at least once across ownership handover. The owner acknowledges after applying
        the command; commands remain queued until then. A full queue raises instead of dropping work.
        """
        authority = self._authorities.get(mid)
        if authority is None:
            raise ValueError("model has no client-owned fields")
        wire = next((wire for wire, name in self._owned_names[mid].items() if name == field), field)
        return authority.command(wire, json.dumps(to_value(value)))

    def _submit_owned(self, mid: int, patch: dict) -> dict:
        self.flush()
        authoritative = {"rev": cast(int, self._store.revision(mid)) + 1, "ops": patch["ops"]}
        values = {op["Set"]["path"][0]["Key"]: op["Set"]["value"] for op in patch["ops"]}
        decoded = self._decode_owned(mid, values)
        mirror = self._owned_projection(mid, decoded)
        self._store.apply(mid, json.dumps(authoritative))
        self._refresh_owned(mid, values, decoded=decoded, mirror=mirror)
        self._record(mid, authoritative)
        if self._on_patch is not None:
            try:
                self._on_patch(mid, authoritative)
            except Exception:
                logging.getLogger(__name__).exception("owned update callback failed for model %s", mid)
        return authoritative

    def _decode_owned(self, mid: int, values: dict) -> dict:
        obj = self._models[mid]
        annotations = _annotations(type(obj))
        return {
            name: decode_owned(_py_of(value), annotations[name], getattr(obj, name))
            for wire, value in values.items()
            for name in (self._owned_names[mid][wire],)
        }

    def _owned_projection(self, mid: int, decoded: dict) -> dict:
        from copy import copy

        import msgspec

        suppressed = mid in self._suppress
        self._suppress.add(mid)
        try:
            obj = copy(self._models[mid])
            if hasattr(obj, "__dict__"):
                object.__setattr__(obj, "__dict__", dict(obj.__dict__))
            for name, value in decoded.items():
                if isinstance(obj, BaseModel):
                    obj.__dict__[name] = value
                elif isinstance(obj, msgspec.Struct):
                    msgspec.structs.force_setattr(obj, name, value)
                else:
                    object.__setattr__(obj, name, value)
            return to_value(obj, include=set(decoded))["Map"]
        finally:
            if not suppressed:
                self._suppress.discard(mid)

    def _refresh_owned(self, mid: int, values: dict, *, decoded: dict | None = None, mirror: dict | None = None) -> None:
        obj = self._models[mid]
        decoded = self._decode_owned(mid, values) if decoded is None else decoded
        mirror = self._owned_projection(mid, decoded) if mirror is None else mirror
        self._suppress.add(mid)
        try:
            for name, value in decoded.items():
                if isinstance(obj, BaseModel):
                    obj.__dict__[name] = value
                    obj.__pydantic_fields_set__.add(name)
                else:
                    setattr(obj, name, value)
            self._owned_mirrors[mid].update(mirror)
        finally:
            self._suppress.discard(mid)

    def _record(self, mid: int, patch: dict) -> None:
        """Append a patch to the model's bounded replay log (for resume)."""
        if mid in self._authorities:
            return
        log = self._log.get(mid)
        if log is None:
            log = self._log[mid] = ReplayLog(self._log_cap, self._log_bytes, self._blob_threshold)
        log.append(json.dumps(patch))

    def since(self, mid: int, since_rev: int) -> list[dict] | None:
        """Patches to advance a mirror from `since_rev` to current, or `None` if the log can't bridge the
        gap (the next needed patch was evicted) — then the caller should send a fresh snapshot instead."""
        cur = self.snapshot(mid)["rev"]  # flush pending changes (recording them) before reading the log
        if since_rev >= cur:
            return []  # already current
        log = self._log.get(mid)
        replay = log.since(since_rev, cur) if log is not None else None
        return json.loads(replay) if replay is not None else None

    def replay_stats(self, mid: int) -> dict[str, int]:
        """Retained patch count, unique blob count, and payload bytes (excluding object overhead)."""
        log = self._log.get(mid)
        return {"patches": log.count(), "blobs": log.blobs(), "bytes": log.bytes()} if log is not None else {"patches": 0, "blobs": 0, "bytes": 0}

    def _canonical(self, mid: int, current_value: dict, ops: list) -> tuple[dict | None, str | None]:
        """The proposal applied to ``current_value`` and normalized through the hosted model — its canonical
        typed `Value` — as ``(value, None)``, or ``(None, error)`` if the core rejects the patch or the
        model rejects the value.

        Computed on a *copy* via the pure ``apply`` — so a rejected proposal never touches the store — then
        round-tripped through the model bridge: pydantic / msgspec validate (rejecting bad input) and coerce
        (so ``"80"`` for an int field comes back as ``80``). A model with no typed object hosted here (a plain
        mirror) returns the candidate unchanged; dataclasses accept per their own semantics."""
        try:
            candidate = json.loads(_apply(json.dumps(current_value), json.dumps({"rev": 0, "ops": ops})))
        except ValueError as e:
            return None, f"malformed patch: {e}"  # the core rejected it (bad path/type/index)
        obj = self._models.get(mid)
        if obj is None:
            return candidate, None  # untyped mirror — nothing to canonicalize against
        try:
            canonical = to_value(self._validated_model(mid, candidate))
            for name in self._owned_fields.get(mid, ()):
                canonical["Map"][name] = current_value["Map"][name]
            return canonical, None
        except Exception as e:  # noqa: BLE001
            return None, str(e)  # the model rejected the value (pydantic / msgspec / ...) — its message travels

    def _validated_model(self, mid: int, value: dict) -> Any:
        obj = self._models[mid]
        fields = self._owned_fields.get(mid)
        if not fields:
            return from_value(value, type(obj))
        names = _field_names(type(obj))
        plain = {names.get(name, name): item for name, item in _py_of(value).items()}
        plain.update(self._decode_owned(mid, {name: value["Map"][name] for name in fields}))
        if isinstance(obj, BaseModel):
            return server_validator(type(obj), frozenset(self._owned_names[mid].values())).validate_python(plain, by_name=True)
        from copy import copy

        import msgspec

        fresh = copy(obj)
        for name, annotation in _annotations(type(obj)).items():
            item = plain[name] if name in self._owned_names[mid].values() else msgspec.convert(plain[name], annotation, strict=False)
            if isinstance(fresh, msgspec.Struct):
                msgspec.structs.force_setattr(fresh, name, item)
            else:
                object.__setattr__(fresh, name, item)
        if hasattr(fresh, "__post_init__"):
            fresh.__post_init__()
        return fresh

    def _apply_authoritative(self, mid: int, patch: dict) -> bool:
        snapshot = self._store.snapshot(mid)
        if snapshot is None:
            return False
        try:
            value = json.loads(_apply(json.dumps(json.loads(snapshot)["value"]), json.dumps(patch)))
            fresh = self._validated_model(mid, value)
        except Exception:  # noqa: BLE001
            return False
        if not self._store.apply(mid, json.dumps(patch)):
            return False
        obj = self._models[mid]
        self._suppress.add(mid)
        try:
            for name in _annotations(type(obj)):
                if name not in self._owned_names.get(mid, {}).values():
                    setattr(obj, name, getattr(fresh, name))
            if mid in self._owned_fields:
                self._refresh_owned(mid, {name: value["Map"][name] for name in self._owned_fields[mid]})
        finally:
            self._suppress.discard(mid)
        return True

    def schema(self, type_name: str) -> dict | None:
        return self._schemas.get(type_name)
