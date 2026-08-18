//! Structural diff/patch over [`Value`] — the incremental-update engine.
//!
//! This is the piece the prototype never had: `BaseModel.onUpdate()` was a stub, so the old
//! library shipped whole models. [`diff`] computes the ops that turn one value into another and
//! [`apply`] replays them, guaranteeing the round-trip property
//!
//! ```text
//! apply(old.clone(), diff(old, new)) == new
//! ```
//!
//! exercised by a deterministic fuzz below. Maps diff by key; lists preserve exact-value matches
//! across moves, then diff unmatched values positionally. A type change at a path replaces the
//! value there wholesale.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::value::Value;

/// One step into a value: a map key or a list index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathSeg {
    Key(String),
    Index(usize),
}

/// A path from the model root to a value.
pub type Path = Vec<PathSeg>;

/// A single mutation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Op {
    /// Set (or replace) the value at `path`. Empty path replaces the whole model.
    Set { path: Path, value: Value },
    /// Remove the map entry at `path` (the last segment is a [`PathSeg::Key`]).
    Remove { path: Path },
    /// Insert `value` into the list at `path`, at `index`.
    Insert {
        path: Path,
        index: usize,
        value: Value,
    },
    /// Remove the element at `index` from the list at `path`.
    RemoveAt { path: Path, index: usize },
    /// Move an existing list element. `from` addresses the list before this op and `to` is the
    /// element's final index after the move.
    Move { path: Path, from: usize, to: usize },
    /// Reorder a complete list in one operation. `order[new_index]` is the element's index before
    /// this op, and `order` must be a permutation of every list index.
    Reorder { path: Path, order: Vec<usize> },
}

/// An ordered set of ops plus the revision they advance the model to.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Patch {
    pub rev: u64,
    pub ops: Vec<Op>,
}

impl Patch {
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
    pub fn len(&self) -> usize {
        self.ops.len()
    }
}

/// Compute the patch that turns `old` into `new`. The returned patch has `rev == 0`; callers that
/// track revisions (e.g. [`crate::Store`]) set it.
pub fn diff(old: &Value, new: &Value) -> Patch {
    let mut ops = Vec::new();
    diff_value(&Vec::new(), old, new, &mut ops);
    Patch { rev: 0, ops }
}

/// Apply a patch to `value` in place. Returns `Err` if the patch is malformed (a path descends into
/// the wrong type, an index is out of bounds, or a key is missing) so callers can reject untrusted
/// proposals instead of panicking.
pub fn apply(value: &mut Value, patch: &Patch) -> Result<(), String> {
    for op in &patch.ops {
        apply_op(value, op)?;
    }
    Ok(())
}

fn child_path(path: &Path, seg: PathSeg) -> Path {
    let mut p = path.to_vec();
    p.push(seg);
    p
}

fn diff_list_positionally(path: &Path, old: &[Value], new: &[Value], ops: &mut Vec<Op>) {
    let shared = old.len().min(new.len());
    for i in 0..shared {
        diff_value(&child_path(path, PathSeg::Index(i)), &old[i], &new[i], ops);
    }
    if new.len() > old.len() {
        for (i, value) in new.iter().enumerate().skip(old.len()) {
            ops.push(Op::Insert {
                path: path.to_vec(),
                index: i,
                value: value.clone(),
            });
        }
    } else {
        for i in (new.len()..old.len()).rev() {
            ops.push(Op::RemoveAt {
                path: path.to_vec(),
                index: i,
            });
        }
    }
}

fn exact_permutation(old: &[Value], new: &[Value]) -> Option<Vec<usize>> {
    if old.len() != new.len() {
        return None;
    }
    let mut positions: BTreeMap<String, VecDeque<usize>> = BTreeMap::new();
    for (index, value) in old.iter().enumerate() {
        positions
            .entry(serde_json::to_string(value).ok()?)
            .or_default()
            .push_back(index);
    }
    let mut order = Vec::with_capacity(new.len());
    for value in new {
        let encoded = serde_json::to_string(value).ok()?;
        let index = positions.get_mut(&encoded)?.pop_front()?;
        if old[index] != *value {
            return None;
        }
        order.push(index);
    }
    Some(order)
}

fn permutation_move_count(order: &[usize]) -> usize {
    // Minimum arbitrary moves = sequence length minus its longest increasing subsequence.
    let mut tails = Vec::new();
    for &index in order {
        let position = tails
            .binary_search(&index)
            .unwrap_or_else(|position| position);
        if position == tails.len() {
            tails.push(index);
        } else {
            tails[position] = index;
        }
    }
    order.len() - tails.len()
}

#[allow(clippy::needless_range_loop)] // indices here are op positions
fn diff_value(path: &Path, old: &Value, new: &Value, ops: &mut Vec<Op>) {
    match (old, new) {
        (Value::Map(om), Value::Map(nm)) => {
            for (k, nv) in nm {
                match om.get(k) {
                    Some(ov) => diff_value(&child_path(path, PathSeg::Key(k.clone())), ov, nv, ops),
                    None => ops.push(Op::Set {
                        path: child_path(path, PathSeg::Key(k.clone())),
                        value: nv.clone(),
                    }),
                }
            }
            for k in om.keys() {
                if !nm.contains_key(k) {
                    ops.push(Op::Remove {
                        path: child_path(path, PathSeg::Key(k.clone())),
                    });
                }
            }
        }
        (Value::List(ol), Value::List(nl)) => {
            // Reconcile exact-value matches first so a reorder becomes moves rather than positional
            // replacement. Unmatched values still recurse positionally: generic Value lists carry
            // no application key metadata, so guessing that a changed record is the same item would
            // be incorrect.
            // Matching plus Vec moves can be quadratic for a dense permutation. Bound staged
            // reconciliation to a linear multiple of sequence length; if it is exhausted, use the
            // exact permutation when available and otherwise retain linear positional diff behavior.
            let work_limit = ol.len().max(nl.len()).saturating_mul(8).max(64);
            let exact_order = exact_permutation(ol, nl);
            if let Some(order) = &exact_order {
                let move_work = permutation_move_count(order).saturating_mul(ol.len());
                if move_work > work_limit {
                    ops.push(Op::Reorder {
                        path: path.to_vec(),
                        order: order.clone(),
                    });
                    return;
                }
            }
            let mut work = 0usize;
            let mut exhausted = false;
            let mut list_ops = Vec::new();
            let mut working = ol.clone();
            let mut i = 0;
            while i < nl.len() {
                let target = &nl[i];
                if i >= working.len() {
                    list_ops.push(Op::Insert {
                        path: path.to_vec(),
                        index: i,
                        value: target.clone(),
                    });
                    working.insert(i, target.clone());
                    i += 1;
                    continue;
                }

                if working[i] == *target {
                    i += 1;
                    continue;
                }

                let remaining = working.len() - i - 1;
                work = work.saturating_add(remaining);
                if work > work_limit {
                    exhausted = true;
                    break;
                }
                if let Some(offset) = working[i + 1..].iter().position(|value| value == target) {
                    let from = i + 1 + offset;
                    work = work.saturating_add(working.len());
                    if work > work_limit {
                        exhausted = true;
                        break;
                    }
                    list_ops.push(Op::Move {
                        path: path.to_vec(),
                        from,
                        to: i,
                    });
                    let moved = working.remove(from);
                    working.insert(i, moved);
                    i += 1;
                    continue;
                }

                let remaining = nl.len() - i - 1;
                work = work.saturating_add(remaining);
                if work > work_limit {
                    exhausted = true;
                    break;
                }
                if let Some(offset) = nl[i + 1..].iter().position(|value| value == &working[i]) {
                    let to = i + 1 + offset;
                    work = work.saturating_add(working.len());
                    if work > work_limit {
                        exhausted = true;
                        break;
                    }
                    if working.len() < nl.len() {
                        // The list is growing and the current value has a later destination, so this
                        // target is an insertion.
                        list_ops.push(Op::Insert {
                            path: path.to_vec(),
                            index: i,
                            value: target.clone(),
                        });
                        working.insert(i, target.clone());
                        i += 1;
                        continue;
                    } else if working[i + 1] != working[i] {
                        // Preserve the unchanged current value as an anchor even when the target at
                        // this position changed. Re-evaluate this index after moving the anchor.
                        list_ops.push(Op::Move {
                            path: path.to_vec(),
                            from: i,
                            to,
                        });
                        let moved = working.remove(i);
                        working.insert(to, moved);
                        continue;
                    }
                }

                diff_value(
                    &child_path(path, PathSeg::Index(i)),
                    &working[i],
                    target,
                    &mut list_ops,
                );
                working[i] = target.clone();
                i += 1;
            }

            if exhausted {
                if let Some(order) = exact_order {
                    ops.push(Op::Reorder {
                        path: path.to_vec(),
                        order,
                    });
                } else {
                    diff_list_positionally(path, ol, nl, ops);
                }
            } else {
                for i in (nl.len()..working.len()).rev() {
                    list_ops.push(Op::RemoveAt {
                        path: path.to_vec(),
                        index: i,
                    });
                }
                ops.extend(list_ops);
            }
        }
        _ => {
            // Scalars, submodel refs, or a type change: replace wholesale if not already equal.
            if old != new {
                ops.push(Op::Set {
                    path: path.to_vec(),
                    value: new.clone(),
                });
            }
        }
    }
}

fn value_at_mut<'a>(root: &'a mut Value, path: &[PathSeg]) -> Result<&'a mut Value, String> {
    let mut cur = root;
    for seg in path {
        cur = match seg {
            PathSeg::Key(k) => cur
                .try_as_map_mut()?
                .get_mut(k)
                .ok_or_else(|| format!("path key {k:?} not found"))?,
            PathSeg::Index(i) => cur
                .try_as_list_mut()?
                .get_mut(*i)
                .ok_or_else(|| format!("path index {i} out of bounds"))?,
        };
    }
    Ok(cur)
}

fn apply_op(root: &mut Value, op: &Op) -> Result<(), String> {
    match op {
        Op::Set { path, value } => {
            if path.is_empty() {
                *root = value.clone();
                return Ok(());
            }
            let (last, parent) = path.split_last().unwrap(); // non-empty: checked just above
            let container = value_at_mut(root, parent)?;
            match last {
                PathSeg::Key(k) => {
                    container.try_as_map_mut()?.insert(k.clone(), value.clone());
                }
                PathSeg::Index(i) => {
                    let slot = container
                        .try_as_list_mut()?
                        .get_mut(*i)
                        .ok_or_else(|| format!("set index {i} out of bounds"))?;
                    *slot = value.clone();
                }
            }
        }
        Op::Remove { path } => {
            let (last, parent) = path.split_last().ok_or("remove path is empty")?;
            let container = value_at_mut(root, parent)?;
            if let PathSeg::Key(k) = last {
                container.try_as_map_mut()?.remove(k);
            }
        }
        Op::Insert { path, index, value } => {
            let list = value_at_mut(root, path)?.try_as_list_mut()?;
            if *index > list.len() {
                return Err(format!(
                    "insert index {index} out of bounds (len {})",
                    list.len()
                ));
            }
            list.insert(*index, value.clone());
        }
        Op::RemoveAt { path, index } => {
            let list = value_at_mut(root, path)?.try_as_list_mut()?;
            if *index >= list.len() {
                return Err(format!(
                    "remove index {index} out of bounds (len {})",
                    list.len()
                ));
            }
            list.remove(*index);
        }
        Op::Move { path, from, to } => {
            let list = value_at_mut(root, path)?.try_as_list_mut()?;
            if *from >= list.len() {
                return Err(format!(
                    "move source index {from} out of bounds (len {})",
                    list.len()
                ));
            }
            if *to >= list.len() {
                return Err(format!(
                    "move destination index {to} out of bounds (len {})",
                    list.len()
                ));
            }
            if from != to {
                let moved = list.remove(*from);
                list.insert(*to, moved);
            }
        }
        Op::Reorder { path, order } => {
            let list = value_at_mut(root, path)?.try_as_list_mut()?;
            if order.len() != list.len() {
                return Err(format!(
                    "reorder length {} does not match list length {}",
                    order.len(),
                    list.len()
                ));
            }
            let mut seen = vec![false; list.len()];
            for &index in order {
                if index >= list.len() {
                    return Err(format!(
                        "reorder index {index} out of bounds (len {})",
                        list.len()
                    ));
                }
                if seen[index] {
                    return Err(format!("reorder index {index} is duplicated"));
                }
                seen[index] = true;
            }
            if order.iter().enumerate().all(|(new, &old)| new == old) {
                return Ok(());
            }
            let mut old = std::mem::take(list)
                .into_iter()
                .map(Some)
                .collect::<Vec<_>>();
            *list = order
                .iter()
                .map(|&index| old[index].take().expect("permutation was validated"))
                .collect();
        }
    }
    Ok(())
}

#[cfg(test)]
mod diff_tests {
    use super::*;
    use crate::value::ModelId;

    fn round_trip(old: &Value, new: &Value) -> Patch {
        let patch = diff(old, new);
        let mut got = old.clone();
        apply(&mut got, &patch).unwrap();
        assert_eq!(
            &got, new,
            "round-trip failed\n old={old:#?}\n new={new:#?}\n patch={patch:#?}"
        );
        patch
    }

    #[test]
    fn test_scalar_field_change() {
        let old = Value::map([("on", Value::from(false))]);
        let new = Value::map([("on", Value::from(true))]);
        let patch = round_trip(&old, &new);
        assert!(matches!(patch.ops.as_slice(), [Op::Set { .. }]));
    }

    #[test]
    fn test_add_and_remove_keys() {
        let old = Value::map([("a", Value::from(1i64)), ("b", Value::from(2i64))]);
        let new = Value::map([("a", Value::from(1i64)), ("c", Value::from(3i64))]);
        let patch = round_trip(&old, &new);
        assert_eq!(patch.len(), 2); // set c, remove b
    }

    #[test]
    fn test_nested_map() {
        let old = Value::map([("dev", Value::map([("on", Value::from(false))]))]);
        let new = Value::map([("dev", Value::map([("on", Value::from(true))]))]);
        round_trip(&old, &new);
    }

    #[test]
    fn test_list_grow_shrink_recurse() {
        let l = |xs: Vec<i64>| Value::List(xs.into_iter().map(Value::Int).collect());
        round_trip(
            &Value::map([("xs", l(vec![1]))]),
            &Value::map([("xs", l(vec![1, 2, 3]))]),
        );
        round_trip(
            &Value::map([("xs", l(vec![1, 2, 3]))]),
            &Value::map([("xs", l(vec![1]))]),
        );
        round_trip(
            &Value::map([("xs", l(vec![1, 2]))]),
            &Value::map([("xs", l(vec![9, 2]))]),
        );
    }

    #[test]
    fn test_apply_move_forward_backward_and_noop() {
        let mut value = Value::List(vec![1i64.into(), 2i64.into(), 3i64.into()]);
        apply(
            &mut value,
            &Patch {
                rev: 1,
                ops: vec![Op::Move {
                    path: vec![],
                    from: 0,
                    to: 2,
                }],
            },
        )
        .unwrap();
        assert_eq!(
            value,
            Value::List(vec![2i64.into(), 3i64.into(), 1i64.into()])
        );

        apply(
            &mut value,
            &Patch {
                rev: 2,
                ops: vec![
                    Op::Move {
                        path: vec![],
                        from: 2,
                        to: 0,
                    },
                    Op::Move {
                        path: vec![],
                        from: 1,
                        to: 1,
                    },
                ],
            },
        )
        .unwrap();
        assert_eq!(
            value,
            Value::List(vec![1i64.into(), 2i64.into(), 3i64.into()])
        );
    }

    #[test]
    fn test_diff_emits_moves_for_arbitrary_reorder_and_duplicates() {
        let values = |items: &[&str]| {
            Value::List(items.iter().copied().map(Value::from).collect::<Vec<_>>())
        };
        let old = values(&["a", "b", "c", "d"]);
        let new = values(&["d", "b", "a", "c"]);
        let patch = round_trip(&old, &new);
        assert_eq!(
            patch.ops,
            vec![
                Op::Move {
                    path: vec![],
                    from: 3,
                    to: 0,
                },
                Op::Move {
                    path: vec![],
                    from: 2,
                    to: 1,
                },
            ]
        );

        let old = values(&["a", "a", "b"]);
        let new = values(&["a", "b", "a"]);
        let patch = round_trip(&old, &new);
        assert_eq!(
            patch.ops,
            vec![Op::Move {
                path: vec![],
                from: 2,
                to: 1,
            }]
        );
    }

    #[test]
    fn test_reorder_composes_with_insert_remove_and_update() {
        let row = |id: i64, label: &str| {
            Value::map([("id", Value::from(id)), ("label", Value::from(label))])
        };
        let mut value = Value::List(vec![row(1, "old"), row(2, "same"), row(3, "same")]);
        let patch = Patch {
            rev: 1,
            ops: vec![
                Op::Reorder {
                    path: vec![],
                    order: vec![1, 2, 0],
                },
                Op::Insert {
                    path: vec![],
                    index: 1,
                    value: row(4, "inserted"),
                },
                Op::Set {
                    path: vec![PathSeg::Index(2), PathSeg::Key("label".into())],
                    value: "updated".into(),
                },
                Op::RemoveAt {
                    path: vec![],
                    index: 3,
                },
            ],
        };
        apply(&mut value, &patch).unwrap();
        assert_eq!(
            value,
            Value::List(vec![row(2, "same"), row(4, "inserted"), row(3, "updated")])
        );
    }

    #[test]
    fn test_changed_moved_record_uses_an_unchanged_anchor() {
        let row = |id: i64, label: &str| {
            Value::map([("id", Value::from(id)), ("label", Value::from(label))])
        };
        let old = Value::List(vec![row(1, "a"), row(2, "old")]);
        let new = Value::List(vec![row(2, "new"), row(1, "a")]);
        let patch = round_trip(&old, &new);

        assert_eq!(
            patch.ops,
            vec![
                Op::Move {
                    path: vec![],
                    from: 0,
                    to: 1,
                },
                Op::Set {
                    path: vec![PathSeg::Index(0), PathSeg::Key("label".into())],
                    value: "new".into(),
                },
            ]
        );
    }

    #[test]
    fn test_dense_large_reorder_uses_one_permutation() {
        let old = Value::List((0..256).map(Value::Int).collect());
        let new = Value::List((0..256).rev().map(Value::Int).collect());
        let patch = round_trip(&old, &new);
        assert!(matches!(
            patch.ops.as_slice(),
            [Op::Reorder { order, .. }] if order.len() == 256
        ));

        let mut rotated = (0..256).map(Value::Int).collect::<Vec<_>>();
        rotated.rotate_right(1);
        let patch = round_trip(&old, &Value::List(rotated));
        assert!(matches!(
            patch.ops.as_slice(),
            [Op::Move {
                from: 255,
                to: 0,
                ..
            }]
        ));

        let mut rotated = (0..256).map(Value::Int).collect::<Vec<_>>();
        rotated.rotate_left(1);
        let patch = round_trip(&old, &Value::List(rotated));
        assert!(matches!(patch.ops.as_slice(), [Op::Reorder { .. }]));
    }

    #[test]
    fn test_reorder_rejects_invalid_permutations_without_mutation() {
        let original = Value::List(vec![1i64.into(), 2i64.into(), 3i64.into()]);
        for (order, message) in [
            (vec![0, 1], "reorder length 2 does not match list length 3"),
            (vec![0, 1, 3], "reorder index 3 out of bounds"),
            (vec![0, 1, 1], "reorder index 1 is duplicated"),
        ] {
            let mut value = original.clone();
            let error = apply(
                &mut value,
                &Patch {
                    rev: 1,
                    ops: vec![Op::Reorder {
                        path: vec![],
                        order,
                    }],
                },
            )
            .unwrap_err();
            assert!(error.contains(message));
            assert_eq!(value, original);
        }
    }

    #[test]
    fn test_move_rejects_invalid_indices_without_mutation() {
        let original = Value::List(vec![1i64.into(), 2i64.into()]);
        for (from, to, message) in [
            (2, 0, "move source index 2 out of bounds"),
            (0, 2, "move destination index 2 out of bounds"),
        ] {
            let mut value = original.clone();
            let error = apply(
                &mut value,
                &Patch {
                    rev: 1,
                    ops: vec![Op::Move {
                        path: vec![],
                        from,
                        to,
                    }],
                },
            )
            .unwrap_err();
            assert!(error.contains(message));
            assert_eq!(value, original);
        }
    }

    #[test]
    fn test_type_change_replaces() {
        round_trip(
            &Value::map([("x", Value::from(1i64))]),
            &Value::map([("x", Value::map([("nested", Value::from(true))]))]),
        );
    }

    #[test]
    fn test_submodel_ref_change() {
        let old = Value::map([("child", Value::Submodel(ModelId(1)))]);
        let new = Value::map([("child", Value::Submodel(ModelId(2)))]);
        let patch = round_trip(&old, &new);
        assert!(matches!(patch.ops.as_slice(), [Op::Set { .. }]));
    }

    #[test]
    fn test_identical_is_empty() {
        let v = Value::map([("a", Value::from(1i64)), ("b", Value::from(vec!["x"]))]);
        assert!(diff(&v, &v).is_empty());
    }

    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0
        }
        fn below(&mut self, n: usize) -> usize {
            ((self.next() >> 33) as usize) % n
        }
    }

    fn gen_value(rng: &mut Lcg, depth: usize) -> Value {
        match rng.below(if depth == 0 { 5 } else { 7 }) {
            0 => Value::Null,
            1 => Value::Bool(rng.below(2) == 1),
            2 => Value::Int(rng.below(5) as i64),
            3 => Value::Str(["", "a", "bb"][rng.below(3)].to_string()),
            4 => Value::Submodel(ModelId(rng.below(3) as u64)),
            5 => {
                let n = rng.below(4);
                Value::List((0..n).map(|_| gen_value(rng, depth - 1)).collect())
            }
            _ => {
                let keys = ["x", "y", "z", "w"];
                let n = rng.below(keys.len() + 1);
                Value::map((0..n).map(|i| (keys[i], gen_value(rng, depth - 1))))
            }
        }
    }

    #[test]
    fn test_fuzz_round_trip() {
        let mut rng = Lcg(0xC0FFEE_1234_5678);
        for _ in 0..3000 {
            let old = gen_value(&mut rng, 4);
            let new = gen_value(&mut rng, 4);
            let patch = diff(&old, &new);
            let mut got = old.clone();
            apply(&mut got, &patch).unwrap();
            assert_eq!(
                got, new,
                "fuzz failed\n old={old:#?}\n new={new:#?}\n patch={patch:#?}"
            );
            assert!(diff(&new, &new).is_empty());
        }
    }
}
