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
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from bigbrother import watch

from ._bridge import _annotations, from_value, schema_of, to_value
from .transports import Store as _CoreStore


class Session:
    def __init__(self) -> None:
        self._store = _CoreStore()
        self._models: Dict[int, Any] = {}
        self._schemas: Dict[str, dict] = {}
        self._dirty: Set[int] = set()
        self._suppress: Set[int] = set()  # ids whose observation is suspended (during inbound apply)
        self.outbox: List[Tuple[int, dict]] = []
        self._on_patch: Optional[Callable[[int, dict], None]] = None

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

    def ids(self) -> List[int]:
        """The ids of all hosted models."""
        return list(self._models.keys())

    def on_patch(self, fn: Callable[[int, dict], None]) -> None:
        """Register a callback invoked as `fn(model_id, patch)` for each emitted patch."""
        self._on_patch = fn

    def flush(self) -> List[Tuple[int, dict]]:
        """Diff every dirty model against the core and emit the minimal patches. Returns them."""
        emitted: List[Tuple[int, dict]] = []
        for mid in sorted(self._dirty):
            patch_json = self._store.mutate(mid, json.dumps(to_value(self._models[mid])))
            if patch_json is None:
                continue
            patch = json.loads(patch_json)
            if patch["ops"]:
                self.outbox.append((mid, patch))
                emitted.append((mid, patch))
                if self._on_patch is not None:
                    self._on_patch(mid, patch)
        self._dirty.clear()
        return emitted

    def update(self, mid: int) -> List[Tuple[int, dict]]:
        """Force a diff+emit for one hosted model and flush.

        Automatic emission needs bigbrother to observe the model; models without a ``__dict__``
        (``msgspec.Struct`` and other ``__slots__`` types) can't be watched, so mutate them and then
        call ``update(id)`` explicitly.
        """
        self._dirty.add(mid)
        return self.flush()

    def drain(self) -> List[Tuple[int, dict]]:
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

    def submit(self, mid: int, patch: dict) -> Optional[dict]:
        """Apply a client-proposed patch *as the server*: the server owns `rev`.

        The proposal's ops are applied to the hosted value, the server's `rev` is bumped (not the
        client's guess), the hosted Python object is refreshed, and the authoritative patch (with the
        server `rev`) is returned to broadcast to every connection. `None` if the id is unknown.
        """
        snap = self._store.snapshot(mid)
        if snap is None:
            return None
        cur_rev = json.loads(snap)["rev"]
        authoritative = {"rev": cur_rev + 1, "ops": patch.get("ops", [])}
        if not self._apply_authoritative(mid, authoritative):
            return None  # reject a malformed proposal (bad path/type/index) without crashing the host
        return authoritative

    def _apply_authoritative(self, mid: int, patch: dict) -> bool:
        try:
            if not self._store.apply(mid, json.dumps(patch)):
                return False
        except ValueError:
            return False  # the core rejected a malformed patch (bad path/type/index)
        self._refresh_model(mid)
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

    def schema(self, type_name: str) -> Optional[dict]:
        return self._schemas.get(type_name)
