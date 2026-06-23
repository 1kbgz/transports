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
//! exercised by a deterministic fuzz below. Maps diff by key; lists diff positionally (keyed-by-id
//! list reconciliation for `Submodel` lists is a later refinement). A type change at a path
//! replaces the value there wholesale.

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
            let n = ol.len().min(nl.len());
            for i in 0..n {
                diff_value(&child_path(path, PathSeg::Index(i)), &ol[i], &nl[i], ops);
            }
            if nl.len() > ol.len() {
                for i in ol.len()..nl.len() {
                    ops.push(Op::Insert {
                        path: path.to_vec(),
                        index: i,
                        value: nl[i].clone(),
                    });
                }
            } else {
                for i in (nl.len()..ol.len()).rev() {
                    ops.push(Op::RemoveAt {
                        path: path.to_vec(),
                        index: i,
                    });
                }
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
