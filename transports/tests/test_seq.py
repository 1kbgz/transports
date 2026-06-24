import itertools
import json

from transports import SeqCrdt, diff, seq_delete, seq_insert, seq_materialize, seq_new


def _val(seq: str) -> dict:
    return {"Str": seq}  # the core Value for a sequence-holding str field


def _patch(old: str, new: str) -> dict:
    return json.loads(diff(json.dumps(_val(old)), json.dumps(_val(new))))


def test_seq_new_and_materialize_round_trip():
    assert seq_materialize(seq_new(["a", "b", "c"], "base")) == ["a", "b", "c"]


def test_concurrent_inserts_into_the_same_gap_both_survive_and_converge():
    base = seq_new(["a", "c"], "base")
    pa = _patch(base, seq_insert(base, "X", 1, "A"))  # between a and c
    pb = _patch(base, seq_insert(base, "Y", 1, "B"))  # same gap, concurrent
    c1 = SeqCrdt()
    v1 = c1.merge(c1.merge(_val(base), pa, "A"), pb, "B")
    c2 = SeqCrdt()
    v2 = c2.merge(c2.merge(_val(base), pb, "B"), pa, "A")
    assert seq_materialize(v1["Str"]) == seq_materialize(v2["Str"])  # conflict-free
    assert seq_materialize(v1["Str"]) == ["a", "X", "Y", "c"]  # both kept; tie broken by origin A < B


def test_seq_converges_over_every_order():
    base = seq_new(["a", "b"], "base")
    edits = [
        (_patch(base, seq_insert(base, "1", 0, "A")), "A"),  # front
        (_patch(base, seq_insert(base, "2", 1, "B")), "B"),  # middle
        (_patch(base, seq_insert(base, "3", 2, "C")), "C"),  # end
    ]
    results = []
    for perm in itertools.permutations(edits):
        crdt = SeqCrdt()
        v = _val(base)
        for patch, origin in perm:
            v = crdt.merge(v, patch, origin)
        results.append(seq_materialize(v["Str"]))
    assert all(r == results[0] for r in results)  # order-independent
    assert set(results[0]) == {"a", "b", "1", "2", "3"}  # every insert landed
    assert results[0][0] == "1" and results[0][-1] == "3"  # front/end placed correctly


def test_delete_is_monotonic_and_converges_with_a_concurrent_insert():
    base = seq_new(["a", "b", "c"], "base")
    pd = _patch(base, seq_delete(base, 1))  # tombstone "b"
    pi = _patch(base, seq_insert(base, "X", 2, "B"))  # insert between b and c, concurrently
    c1 = SeqCrdt()
    v1 = c1.merge(c1.merge(_val(base), pd, "A"), pi, "B")
    c2 = SeqCrdt()
    v2 = c2.merge(c2.merge(_val(base), pi, "B"), pd, "A")
    assert seq_materialize(v1["Str"]) == seq_materialize(v2["Str"])
    assert seq_materialize(v1["Str"]) == ["a", "X", "c"]  # b deleted, X kept


def test_lww_would_lose_a_concurrent_insert_but_seqcrdt_does_not():
    base = seq_new(["a", "b"], "base")
    pa = _patch(base, seq_insert(base, "X", 1, "A"))
    pb = _patch(base, seq_insert(base, "Y", 1, "B"))
    crdt = SeqCrdt()
    merged = crdt.merge(crdt.merge(_val(base), pa, "A"), pb, "B")
    assert set(seq_materialize(merged["Str"])) == {"a", "b", "X", "Y"}  # neither clobbered
