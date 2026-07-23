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
from collections.abc import Callable
from typing import Any

from bigbrother import watch

from ._bridge import _annotations, from_value, schema_of, to_value
from .transports import Store as _CoreStore, apply as _apply, diff as _diff


class Session:
    def __init__(self) -> None:
        self._store = _CoreStore()
        self._models: dict[int, Any] = {}
        self._schemas: dict[str, dict] = {}
        self._dirty: set[int] = set()
        self._suppress: set[int] = set()  # ids whose observation is suspended (during inbound apply)
        self.outbox: list[tuple[int, dict]] = []
        self._on_patch: Callable[[int, dict], None] | None = None
        self._log: dict[int, list[tuple[int, dict]]] = {}  # per-model replay log of (rev, patch), bounded
        self._log_cap = 512  # patches retained per model for resume; older are evicted (resume → snapshot)

    def host(self, model: Any) -> int:
        """Host a model: register its schema, store its value in the core, and watch it. Returns id."""
        type_name = type(model).__name__
        self._schemas[type_name] = schema_of(type(model))
        mid = self._store.host(type_name, json.dumps(to_value(model)))

        def _watcher(obj: object, method: str, ref: object, call_args: tuple, call_kwargs: dict, _mid: int = mid) -> None:
            if _mid not in self._suppress:  # ignore writes we make ourselves while applying inbound patches
                self._dirty.add(_mid)

        self._models[mid] = watch(model, _watcher, deepstate=True)
        return mid

    def ids(self) -> list[int]:
        """The ids of all hosted models."""
        return list(self._models.keys())

    def on_patch(self, fn: Callable[[int, dict], None]) -> None:
        """Register a callback invoked as `fn(model_id, patch)` for each emitted patch."""
        self._on_patch = fn

    def flush(self) -> list[tuple[int, dict]]:
        """Diff every dirty model against the core and emit the minimal patches. Returns them."""
        emitted: list[tuple[int, dict]] = []
        for mid in sorted(self._dirty):
            patch_json = self._store.mutate(mid, json.dumps(to_value(self._models[mid])))
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

    def update(self, mid: int) -> list[tuple[int, dict]]:
        """Force a diff+emit for one hosted model and flush.

        Automatic emission needs bigbrother to observe the model; models without a ``__dict__``
        (``msgspec.Struct`` and other ``__slots__`` types) can't be watched, so mutate them and then
        call ``update(id)`` explicitly.
        """
        self._dirty.add(mid)
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
        server `rev`) is returned to broadcast to every connection. `None` if the id is unknown.
        """
        snap = self._store.snapshot(mid)
        if snap is None:
            return None
        parsed = json.loads(snap)
        cur = parsed["value"]
        # Validate AND canonicalize the proposal through the hosted model (on a copy, so a bad edit never
        # touches the store): pydantic/msgspec reject an invalid value, and a *coercible* one is normalized
        # to its canonical typed form — e.g. a number control sends the string "80", and the stored value
        # becomes the int 80, so the core value, the Python model, and every client agree. The authoritative
        # patch is the diff to that canonical value (not the client's raw ops), keeping all three in sync.
        canonical = self._canonical(mid, cur, patch.get("ops", []))
        if canonical is None:
            return None  # invalid / malformed — rejected (the caller re-sends state so the proposer reverts)
        ops = json.loads(_diff(json.dumps(cur), json.dumps(canonical))).get("ops", [])
        authoritative = {"rev": parsed["rev"] + 1, "ops": ops}
        if not self._apply_authoritative(mid, authoritative):
            return None
        self._record(mid, authoritative)
        return authoritative

    def _record(self, mid: int, patch: dict) -> None:
        """Append a patch to the model's bounded replay log (for resume)."""
        log = self._log.setdefault(mid, [])
        log.append((patch["rev"], patch))
        if len(log) > self._log_cap:
            del log[: len(log) - self._log_cap]

    def since(self, mid: int, since_rev: int) -> list[dict] | None:
        """Patches to advance a mirror from `since_rev` to current, or `None` if the log can't bridge the
        gap (the next needed patch was evicted) — then the caller should send a fresh snapshot instead."""
        cur = self.snapshot(mid)["rev"]  # flush pending changes (recording them) before reading the log
        if since_rev >= cur:
            return []  # already current
        log = self._log.get(mid, [])
        if not any(rev == since_rev + 1 for rev, _ in log):
            return None  # the next needed patch was evicted — gap, can't replay
        return [patch for rev, patch in log if rev > since_rev]

    def _canonical(self, mid: int, current_value: dict, ops: list) -> dict | None:
        """The proposal applied to ``current_value`` and normalized through the hosted model — its canonical
        typed `Value` — or ``None`` if the core rejects the patch or the model rejects the value.

        Computed on a *copy* via the pure ``apply`` — so a rejected proposal never touches the store — then
        round-tripped through the model bridge: pydantic / msgspec validate (rejecting bad input) and coerce
        (so ``"80"`` for an int field comes back as ``80``). A model with no typed object hosted here (a plain
        mirror) returns the candidate unchanged; dataclasses accept per their own semantics."""
        try:
            candidate = json.loads(_apply(json.dumps(current_value), json.dumps({"rev": 0, "ops": ops})))
        except ValueError:
            return None  # the core rejected a malformed patch (bad path/type/index)
        obj = self._models.get(mid)
        if obj is None:
            return candidate  # untyped mirror — nothing to canonicalize against
        try:
            return to_value(from_value(candidate, type(obj)))  # validate + coerce to the canonical typed value
        except Exception:  # noqa: BLE001
            return None  # the result doesn't validate against the model (pydantic / msgspec / ...)

    def _apply_authoritative(self, mid: int, patch: dict) -> bool:
        try:
            if not self._store.apply(mid, json.dumps(patch)):
                return False
        except ValueError:
            return False  # the core rejected a malformed patch (bad path/type/index)
        try:
            self._refresh_model(mid)
        except Exception:  # noqa: BLE001
            return False  # never crash the host: a model that rejects the value (caught pre-commit by
            # `_validates`, but belt-and-suspenders for any caller that didn't pre-validate)
        return True

    def _refresh_model(self, mid: int) -> None:
        """Rewrite the hosted Python object from the core value, without re-triggering observation."""
        obj = self._models.get(mid)
        snap = self._store.snapshot(mid)
        if obj is None or snap is None:
            return
        fresh = from_value(json.loads(snap)["value"], type(obj))
        self._suppress.add(mid)
        try:
            for name in _annotations(type(obj)):
                setattr(obj, name, getattr(fresh, name))
        finally:
            self._suppress.discard(mid)

    def schema(self, type_name: str) -> dict | None:
        return self._schemas.get(type_name)
