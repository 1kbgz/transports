"""An order-free **sequence CRDT** (Logoot / fractional-index style) for `Hub` shared models.

A sequence is a JSON string of *entries* — ``{"k": position, "o": origin, "v": value, "x": deleted}`` —
where ``k`` is a rational position (``"num/den"``) that orders the entry and ``o`` is the writer that
created it (a tiebreaker, so concurrent inserts into the *same* gap both survive).

Storing the sequence as one opaque **string** (not a structured list) is deliberate: the core diffs lists
*positionally* — field-level index sets that scramble entry identity under concurrent edits — but a
string diffs atomically, so a writer's edit arrives as the whole new sequence. :class:`SeqCrdt` then
merges it as the **union** of entries, with deletions **monotonic** (a tombstone, never resurrected) and
the result ordered by ``(k, o)`` — so concurrent inserts, deletes, and arbitrary-position edits converge
regardless of arrival order.

Positions are exact rationals, so there is always a midpoint between any two (:func:`seq_key_between` =
``(left + right) / 2``). Hold the sequence in a ``str`` model field; edit it with :func:`seq_insert` /
:func:`seq_delete`; read it with :func:`seq_materialize`. (Values are immutable once placed — an edit is
delete + insert, the standard sequence-CRDT model. Full RGA with after-references would need keyed-by-id
list diffs; see ROADMAP 6.2.)
"""

import json
from fractions import Fraction
from typing import Any, List, Optional

from .hub import MergeStrategy
from .transports import apply as _apply

START, END = Fraction(0), Fraction(1)


def seq_key_between(left: Optional[str], right: Optional[str]) -> str:
    """A rational position strictly between `left` and `right` (`None` = the sequence start / end)."""
    lo = Fraction(left) if left else START
    hi = Fraction(right) if right else END
    mid = (lo + hi) / 2
    return f"{mid.numerator}/{mid.denominator}"


def _ordered(entries: Any) -> list:
    return sorted(entries, key=lambda e: (Fraction(e["k"]), e["o"]))


def seq_new(items: List[Any], origin: Any) -> str:
    """Build a sequence from `items`, each placed in order and attributed to `origin`."""
    entries: List[dict] = []
    left: Optional[str] = None
    for it in items:
        k = seq_key_between(left, None)
        entries.append({"k": k, "o": str(origin), "v": it, "x": False})
        left = k
    return json.dumps(entries)


def seq_insert(seq: str, item: Any, index: int, origin: Any) -> str:
    """Insert `item` at live `index` (0 = front, len = end), attributed to `origin`."""
    entries = _ordered(json.loads(seq))
    live = [e for e in entries if not e["x"]]
    left = live[index - 1]["k"] if index > 0 else None
    right = live[index]["k"] if index < len(live) else None
    entries.append({"k": seq_key_between(left, right), "o": str(origin), "v": item, "x": False})
    return json.dumps(_ordered(entries))


def seq_delete(seq: str, index: int) -> str:
    """Tombstone the live item at `index`."""
    entries = _ordered(json.loads(seq))
    live = [e for e in entries if not e["x"]]
    target = live[index]["k"]
    for e in entries:
        if e["k"] == target:
            e["x"] = True
    return json.dumps(_ordered(entries))


def seq_materialize(seq: str) -> list:
    """The live (non-deleted) values of a sequence, in order."""
    return [e["v"] for e in _ordered(json.loads(seq)) if not e["x"]]


class SeqCrdt(MergeStrategy):
    """Conflict-free sequence merge: union of positioned entries, deletes monotonic, ordered by `(k, o)`."""

    def merge(self, current: Any, patch: dict, origin: Any) -> Any:
        applied = json.loads(_apply(json.dumps(current), json.dumps(patch)))  # the writer's whole new sequence
        merged = {(e["k"], e["o"]): e for e in json.loads(current.get("Str", "[]"))}
        for e in json.loads(applied.get("Str", "[]")):
            key = (e["k"], e["o"])
            if key not in merged:
                merged[key] = e  # a new insert (its position key is globally unique)
            elif e["x"]:
                merged[key] = {**merged[key], "x": True}  # delete-wins (monotonic)
        return {"Str": json.dumps(_ordered(merged.values()))}
