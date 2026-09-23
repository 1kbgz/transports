//! Generic CRDT operations and reducer state.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{CrdtPolicy, CrdtSpec, SequenceMaterialization};
use crate::{diff, Patch, Value};

pub type VersionVector = BTreeMap<String, u64>;
pub type CrdtPath = Vec<CrdtPathSegment>;

/// One causally unique operation identifier. Ordering is counter, then replica identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dot {
    pub counter: u64,
    pub replica: String,
}

impl Dot {
    fn initial() -> Self {
        Self {
            counter: 0,
            replica: "$init".into(),
        }
    }
}

/// Stable identity for one sequence element. One insert operation can allocate adjacent elements.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElementId {
    pub dot: Dot,
    pub index: u32,
}

/// A path through CRDT identity, never through a positional list index.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CrdtPathSegment {
    Key { key: String },
    Member { key: String },
    Element { id: ElementId },
}

/// Exact causal knowledge. `compacted` suppresses already-stable operations without retaining each
/// dot; `dots` retains newer and out-of-order operations individually.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CausalContext {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub compacted: VersionVector,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub dots: BTreeSet<Dot>,
}

impl CausalContext {
    pub fn contains(&self, dot: &Dot) -> bool {
        self.compacted.get(&dot.replica).copied().unwrap_or(0) >= dot.counter
            || self.dots.contains(dot)
    }

    fn observe(&mut self, dot: Dot) {
        if !self.contains(&dot) {
            self.dots.insert(dot);
        }
    }

    fn compact(&mut self, frontier: &VersionVector) {
        for (replica, counter) in frontier {
            self.compacted
                .entry(replica.clone())
                .and_modify(|current| *current = (*current).max(*counter))
                .or_insert(*counter);
        }
        self.dots.retain(|dot| !dot_covered(dot, frontier));
    }

    fn max_counter_any(&self) -> u64 {
        self.dots
            .iter()
            .map(|dot| dot.counter)
            .chain(self.compacted.values().copied())
            .max()
            .unwrap_or(0)
    }
}

/// Typed CRDT operations. These carry causal identity and remain distinct from positional patches.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CrdtOp {
    RegisterSet {
        dot: Dot,
        path: CrdtPath,
        value: Value,
    },
    MapSet {
        dot: Dot,
        path: CrdtPath,
        key: String,
        value: Value,
    },
    MapRemove {
        dot: Dot,
        path: CrdtPath,
        key: String,
        removed: Vec<Dot>,
    },
    SetAdd {
        dot: Dot,
        path: CrdtPath,
        value: Value,
    },
    SetRemove {
        dot: Dot,
        path: CrdtPath,
        key: String,
        removed: Vec<Dot>,
    },
    SequenceInsert {
        dot: Dot,
        path: CrdtPath,
        after: Option<ElementId>,
        values: Vec<Value>,
    },
    SequenceDelete {
        dot: Dot,
        path: CrdtPath,
        ids: Vec<ElementId>,
    },
}

impl CrdtOp {
    fn dot(&self) -> &Dot {
        match self {
            Self::RegisterSet { dot, .. }
            | Self::MapSet { dot, .. }
            | Self::MapRemove { dot, .. }
            | Self::SetAdd { dot, .. }
            | Self::SetRemove { dot, .. }
            | Self::SequenceInsert { dot, .. }
            | Self::SequenceDelete { dot, .. } => dot,
        }
    }

    fn target_path(&self) -> CrdtPath {
        match self {
            Self::MapSet { path, key, .. } | Self::MapRemove { path, key, .. } => {
                let mut target = path.clone();
                target.push(CrdtPathSegment::Key { key: key.clone() });
                target
            }
            Self::RegisterSet { path, .. }
            | Self::SetAdd { path, .. }
            | Self::SetRemove { path, .. }
            | Self::SequenceInsert { path, .. }
            | Self::SequenceDelete { path, .. } => path.clone(),
        }
    }
}

/// A local mutation before the document assigns its causal dot and observed identities.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CrdtMutation {
    RegisterSet {
        path: CrdtPath,
        value: Value,
    },
    MapSet {
        path: CrdtPath,
        key: String,
        value: Value,
    },
    MapRemove {
        path: CrdtPath,
        key: String,
    },
    SetAdd {
        path: CrdtPath,
        value: Value,
    },
    SetRemove {
        path: CrdtPath,
        key: String,
    },
    SequenceInsert {
        path: CrdtPath,
        after: Option<ElementId>,
        values: Vec<Value>,
    },
    SequenceDelete {
        path: CrdtPath,
        ids: Vec<ElementId>,
    },
    /// Positional local convenience that expands to identity-based delete / insert operations.
    /// It is never sent on the wire.
    SequenceSplice {
        path: CrdtPath,
        index: usize,
        delete_count: usize,
        values: Vec<Value>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SequenceDeltaElement {
    pub id: ElementId,
    pub value: Value,
}

/// Identity-preserving changes for consumers that cannot infer identity from positional patches.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CrdtDelta {
    RegisterSet {
        path: CrdtPath,
        value: Value,
    },
    MapSet {
        path: CrdtPath,
        key: String,
        value: Value,
    },
    MapRemove {
        path: CrdtPath,
        key: String,
    },
    SetAdd {
        path: CrdtPath,
        key: String,
        value: Value,
    },
    SetRemove {
        path: CrdtPath,
        key: String,
    },
    SequenceInsert {
        path: CrdtPath,
        after: Option<ElementId>,
        elements: Vec<SequenceDeltaElement>,
    },
    SequenceDelete {
        path: CrdtPath,
        ids: Vec<ElementId>,
    },
}

/// Materialized positional patch plus identity-preserving deltas.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrdtEffect {
    pub patch: Patch,
    pub deltas: Vec<CrdtDelta>,
    /// Operations newly accepted by this replica. Duplicate causal dots do not count.
    pub applied: usize,
}

/// Locally generated operations and their already-applied materialized effect.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrdtChange {
    pub ops: Vec<CrdtOp>,
    pub effect: CrdtEffect,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct MapEntryState {
    node: NodeState,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    dots: BTreeSet<Dot>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    removed: BTreeSet<Dot>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    removal_ops: BTreeSet<Dot>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct SetEntryState {
    node: NodeState,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    adds: BTreeSet<Dot>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    removed: BTreeSet<Dot>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    removal_ops: BTreeSet<Dot>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct SequenceEntryState {
    after: Option<ElementId>,
    node: Option<NodeState>,
    inserted: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum NodeState {
    Register {
        value: Option<Value>,
        dot: Option<Dot>,
    },
    Map {
        entries: BTreeMap<String, MapEntryState>,
    },
    Set {
        entries: BTreeMap<String, SetEntryState>,
    },
    Sequence {
        #[serde(with = "sequence_entries")]
        entries: BTreeMap<ElementId, SequenceEntryState>,
        #[serde(with = "sequence_deletes")]
        deletes: BTreeMap<ElementId, BTreeSet<Dot>>,
    },
}

mod sequence_entries {
    use super::{BTreeMap, ElementId, SequenceEntryState};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        value: &BTreeMap<ElementId, SequenceEntryState>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<ElementId, SequenceEntryState>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut result = BTreeMap::new();
        for (id, entry) in Vec::<(ElementId, SequenceEntryState)>::deserialize(deserializer)? {
            if result.insert(id, entry).is_some() {
                return Err(serde::de::Error::custom("duplicate sequence element ID"));
            }
        }
        Ok(result)
    }
}

mod sequence_deletes {
    use super::{BTreeMap, BTreeSet, Dot, ElementId};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        value: &BTreeMap<ElementId, BTreeSet<Dot>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<ElementId, BTreeSet<Dot>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut result = BTreeMap::new();
        for (id, dots) in Vec::<(ElementId, BTreeSet<Dot>)>::deserialize(deserializer)? {
            if result.insert(id, dots).is_some() {
                return Err(serde::de::Error::custom("duplicate sequence deletion ID"));
            }
        }
        Ok(result)
    }
}

/// Transferable reducer state, including duplicate-suppression metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrdtState {
    pub state_version: u32,
    pub spec_hash: String,
    pub context: CausalContext,
    #[serde(
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        with = "dot_fingerprints"
    )]
    fingerprints: BTreeMap<Dot, String>,
    root: NodeState,
}

mod dot_fingerprints {
    use super::{BTreeMap, Dot};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &BTreeMap<Dot, String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<Dot, String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut result = BTreeMap::new();
        for (dot, fingerprint) in Vec::<(Dot, String)>::deserialize(deserializer)? {
            if result.insert(dot, fingerprint).is_some() {
                return Err(serde::de::Error::custom(
                    "duplicate operation fingerprint dot",
                ));
            }
        }
        Ok(result)
    }
}

/// One schema-directed CRDT replica.
#[derive(Clone, Debug)]
pub struct CrdtDocument {
    spec: CrdtSpec,
    replica: String,
    counter: u64,
    state: CrdtState,
}

impl CrdtDocument {
    pub fn new(spec: CrdtSpec, value: Value, replica: impl Into<String>) -> Result<Self, String> {
        let replica = validate_replica(replica.into())?;
        let spec_hash = spec.hash()?;
        let root = node_from_value(&spec.root, &value, &Dot::initial())?;
        Ok(Self {
            spec,
            replica,
            counter: 0,
            state: CrdtState {
                spec_hash,
                state_version: 1,
                context: CausalContext::default(),
                fingerprints: BTreeMap::new(),
                root,
            },
        })
    }

    pub fn from_state(
        spec: CrdtSpec,
        state: CrdtState,
        replica: impl Into<String>,
    ) -> Result<Self, String> {
        let expected = spec.hash()?;
        if state.state_version != 1 {
            return Err(format!(
                "unsupported CRDT state version {}; expected 1",
                state.state_version
            ));
        }
        if state.spec_hash != expected {
            return Err(format!(
                "incompatible CRDT state: spec hash {}, expected {expected}",
                state.spec_hash
            ));
        }
        let fingerprint_dots = state.fingerprints.keys().cloned().collect::<BTreeSet<_>>();
        if fingerprint_dots != state.context.dots {
            return Err("CRDT state fingerprints do not match its uncompacted causal dots".into());
        }
        for dot in &state.context.dots {
            validate_operation_dot(dot)?;
        }
        if state
            .fingerprints
            .values()
            .any(|fingerprint| fingerprint.len() != 71 || !fingerprint.starts_with("sha256:"))
        {
            return Err("CRDT state contains an invalid operation fingerprint".into());
        }
        let replica = validate_replica(replica.into())?;
        let counter = state.context.max_counter_any();
        let document = Self {
            spec,
            replica,
            counter,
            state,
        };
        document.value()?;
        Ok(document)
    }

    pub fn value(&self) -> Result<Value, String> {
        materialize_node(&self.state.root, &self.spec.root)
    }

    pub fn state(&self) -> &CrdtState {
        &self.state
    }

    pub fn apply(&mut self, ops: &[CrdtOp]) -> Result<CrdtEffect, String> {
        let fingerprints = ops
            .iter()
            .map(|op| {
                validate_operation_dot(op.dot())?;
                op_fingerprint(op)
            })
            .collect::<Result<Vec<_>, String>>()?;
        let before = self.value()?;
        let mut current = before.clone();
        let mut working = self.clone();
        let mut deltas = Vec::new();
        let mut applied = 0;
        for (op, fingerprint) in ops.iter().zip(fingerprints) {
            if working.state.context.contains(op.dot()) {
                if working
                    .state
                    .fingerprints
                    .get(op.dot())
                    .is_some_and(|seen| seen != &fingerprint)
                {
                    return Err(format!(
                        "operation dot {:?} was reused with different content",
                        op.dot()
                    ));
                }
                continue;
            }
            working.apply_one(op)?;
            applied += 1;
            working.state.context.observe(op.dot().clone());
            working
                .state
                .fingerprints
                .insert(op.dot().clone(), fingerprint);
            working.counter = working.counter.max(op.dot().counter);
            let after = working.value()?;
            if current != after {
                deltas.push(delta_for(op, &working.spec, &working.state.root)?);
                current = after;
            }
        }
        let effect = CrdtEffect {
            patch: diff(&before, &current),
            deltas,
            applied,
        };
        *self = working;
        Ok(effect)
    }

    pub fn mutate(&mut self, mutations: &[CrdtMutation]) -> Result<CrdtChange, String> {
        let before = self.value()?;
        let mut current = before.clone();
        let mut working = self.clone();
        let mut ops = Vec::with_capacity(mutations.len());
        let mut deltas = Vec::new();
        for mutation in mutations {
            for op in working.operations_for(mutation)? {
                let fingerprint = op_fingerprint(&op)?;
                working.apply_one(&op)?;
                working.state.context.observe(op.dot().clone());
                working
                    .state
                    .fingerprints
                    .insert(op.dot().clone(), fingerprint);
                let after = working.value()?;
                if current != after {
                    deltas.push(delta_for(&op, &working.spec, &working.state.root)?);
                    current = after;
                }
                ops.push(op);
            }
        }
        let applied = ops.len();
        let change = CrdtChange {
            ops,
            effect: CrdtEffect {
                patch: diff(&before, &current),
                deltas,
                applied,
            },
        };
        *self = working;
        Ok(change)
    }

    pub fn member_key(&self, path: &[CrdtPathSegment], value: &Value) -> Result<String, String> {
        let policy = policy_at_path(&self.spec.root, path)?;
        match policy {
            CrdtPolicy::Set { keys, .. } => member_key(keys, value),
            _ => Err("path does not address a set policy".into()),
        }
    }

    /// Compact causally stable metadata. Sequence anchors remain, but their deleted payload state is
    /// discarded so later inserts can still reference the stable element identity.
    pub fn compact(&mut self, frontier: &VersionVector) -> Result<usize, String> {
        for (replica, counter) in frontier {
            let compacted = self
                .state
                .context
                .compacted
                .get(replica)
                .copied()
                .unwrap_or(0);
            let observed = self
                .state
                .context
                .dots
                .iter()
                .filter(|dot| {
                    &dot.replica == replica && dot.counter > compacted && dot.counter <= *counter
                })
                .count() as u64;
            if *counter > compacted && observed != *counter - compacted {
                return Err(format!(
                    "compaction frontier {replica}:{counter} is not covered by contiguous observed dots after {compacted}"
                ));
            }
        }
        let compacted = compact_node(&mut self.state.root, &self.spec.root, frontier);
        self.state.context.compact(frontier);
        self.state
            .fingerprints
            .retain(|dot, _| !dot_covered(dot, frontier));
        Ok(compacted)
    }

    fn next_dot(&mut self) -> Dot {
        self.counter = self.counter.max(self.state.context.max_counter_any());
        self.counter += 1;
        Dot {
            counter: self.counter,
            replica: self.replica.clone(),
        }
    }

    fn operations_for(&mut self, mutation: &CrdtMutation) -> Result<Vec<CrdtOp>, String> {
        let CrdtMutation::SequenceSplice {
            path,
            index,
            delete_count,
            values,
        } = mutation
        else {
            return Ok(vec![self.operation_for(mutation)?]);
        };
        let ids = visible_sequence_ids_at(&self.state.root, &self.spec.root, path)?;
        if *index > ids.len() {
            return Err(format!(
                "sequence splice index {index} out of bounds (len {})",
                ids.len()
            ));
        }
        let end = index
            .checked_add(*delete_count)
            .filter(|end| *end <= ids.len())
            .ok_or_else(|| {
                format!(
                    "sequence splice delete range {index}..{} out of bounds (len {})",
                    index.saturating_add(*delete_count),
                    ids.len()
                )
            })?;
        let after = index.checked_sub(1).map(|previous| ids[previous].clone());
        let mut ops = Vec::with_capacity(2);
        if *delete_count > 0 {
            ops.push(CrdtOp::SequenceDelete {
                dot: self.next_dot(),
                path: path.clone(),
                ids: ids[*index..end].to_vec(),
            });
        }
        if !values.is_empty() {
            ops.push(CrdtOp::SequenceInsert {
                dot: self.next_dot(),
                path: path.clone(),
                after,
                values: values.clone(),
            });
        }
        Ok(ops)
    }

    fn operation_for(&mut self, mutation: &CrdtMutation) -> Result<CrdtOp, String> {
        let dot = self.next_dot();
        Ok(match mutation {
            CrdtMutation::RegisterSet { path, value } => CrdtOp::RegisterSet {
                dot,
                path: path.clone(),
                value: value.clone(),
            },
            CrdtMutation::MapSet { path, key, value } => CrdtOp::MapSet {
                dot,
                path: path.clone(),
                key: key.clone(),
                value: value.clone(),
            },
            CrdtMutation::MapRemove { path, key } => {
                let removed = map_entry_live_dots(&self.state.root, &self.spec.root, path, key)?;
                CrdtOp::MapRemove {
                    dot,
                    path: path.clone(),
                    key: key.clone(),
                    removed,
                }
            }
            CrdtMutation::SetAdd { path, value } => CrdtOp::SetAdd {
                dot,
                path: path.clone(),
                value: value.clone(),
            },
            CrdtMutation::SetRemove { path, key } => {
                let removed = set_member_active_adds(&self.state.root, &self.spec.root, path, key)?;
                CrdtOp::SetRemove {
                    dot,
                    path: path.clone(),
                    key: key.clone(),
                    removed,
                }
            }
            CrdtMutation::SequenceInsert {
                path,
                after,
                values,
            } => CrdtOp::SequenceInsert {
                dot,
                path: path.clone(),
                after: after.clone(),
                values: values.clone(),
            },
            CrdtMutation::SequenceDelete { path, ids } => CrdtOp::SequenceDelete {
                dot,
                path: path.clone(),
                ids: ids.clone(),
            },
            CrdtMutation::SequenceSplice { .. } => {
                return Err("sequence_splice must expand before operation encoding".into());
            }
        })
    }

    fn apply_one(&mut self, op: &CrdtOp) -> Result<(), String> {
        if mutates_set_identity(&self.spec.root, &op.target_path())? {
            return Err(
                "set identity fields are immutable; remove and add the member instead".into(),
            );
        }
        match op {
            CrdtOp::RegisterSet { dot, path, value } => {
                let (node, policy) =
                    node_at_path_mut(&mut self.state.root, &self.spec.root, path, dot)?;
                if !matches!(policy, CrdtPolicy::Register {}) {
                    return Err("register_set path does not address a register".into());
                }
                set_register(node, value.clone(), dot.clone())
            }
            CrdtOp::MapSet {
                dot,
                path,
                key,
                value,
            } => {
                let (node, policy) =
                    node_at_path_mut(&mut self.state.root, &self.spec.root, path, dot)?;
                let CrdtPolicy::Map { fields, values } = policy else {
                    return Err("map_set path does not address a map".into());
                };
                let NodeState::Map { entries } = node else {
                    return Err("map policy has incompatible reducer state".into());
                };
                let child_policy = fields.get(key).unwrap_or(values);
                let entry = entries.entry(key.clone()).or_insert_with(|| MapEntryState {
                    node: empty_node(child_policy),
                    dots: BTreeSet::new(),
                    removed: BTreeSet::new(),
                    removal_ops: BTreeSet::new(),
                });
                entry.dots.insert(dot.clone());
                merge_value(&mut entry.node, child_policy, value, dot)
            }
            CrdtOp::MapRemove {
                dot,
                path,
                key,
                removed,
            } => {
                let (node, policy) =
                    node_at_path_mut(&mut self.state.root, &self.spec.root, path, dot)?;
                let CrdtPolicy::Map { fields, values } = policy else {
                    return Err("map_remove path does not address a map".into());
                };
                let NodeState::Map { entries } = node else {
                    return Err("map policy has incompatible reducer state".into());
                };
                let child_policy = fields.get(key).unwrap_or(values);
                let entry = entries.entry(key.clone()).or_insert_with(|| MapEntryState {
                    node: empty_node(child_policy),
                    dots: BTreeSet::new(),
                    removed: BTreeSet::new(),
                    removal_ops: BTreeSet::new(),
                });
                entry.removed.extend(removed.iter().cloned());
                entry.removal_ops.insert(dot.clone());
                Ok(())
            }
            CrdtOp::SetAdd { dot, path, value } => {
                let (node, policy) =
                    node_at_path_mut(&mut self.state.root, &self.spec.root, path, dot)?;
                let CrdtPolicy::Set { keys, element } = policy else {
                    return Err("set_add path does not address a set".into());
                };
                let NodeState::Set { entries } = node else {
                    return Err("set policy has incompatible reducer state".into());
                };
                let key = member_key(keys, value)?;
                let entry = entries.entry(key).or_insert_with(|| SetEntryState {
                    node: empty_node(element),
                    adds: BTreeSet::new(),
                    removed: BTreeSet::new(),
                    removal_ops: BTreeSet::new(),
                });
                entry.adds.insert(dot.clone());
                merge_value(&mut entry.node, element, value, dot)
            }
            CrdtOp::SetRemove {
                dot,
                path,
                key,
                removed,
            } => {
                let (node, policy) =
                    node_at_path_mut(&mut self.state.root, &self.spec.root, path, dot)?;
                let CrdtPolicy::Set { element, .. } = policy else {
                    return Err("set_remove path does not address a set".into());
                };
                let NodeState::Set { entries } = node else {
                    return Err("set policy has incompatible reducer state".into());
                };
                let entry = entries.entry(key.clone()).or_insert_with(|| SetEntryState {
                    node: empty_node(element),
                    adds: BTreeSet::new(),
                    removed: BTreeSet::new(),
                    removal_ops: BTreeSet::new(),
                });
                entry.removed.extend(removed.iter().cloned());
                entry.removal_ops.insert(dot.clone());
                Ok(())
            }
            CrdtOp::SequenceInsert {
                dot,
                path,
                after,
                values,
            } => {
                let (node, policy) =
                    node_at_path_mut(&mut self.state.root, &self.spec.root, path, dot)?;
                let CrdtPolicy::Sequence { element, .. } = policy else {
                    return Err("sequence_insert path does not address a sequence".into());
                };
                let NodeState::Sequence { entries, .. } = node else {
                    return Err("sequence policy has incompatible reducer state".into());
                };
                let mut predecessor = after.clone();
                for (index, value) in values.iter().enumerate() {
                    let index = u32::try_from(index)
                        .map_err(|_| "sequence insert contains too many elements")?;
                    let id = ElementId {
                        dot: dot.clone(),
                        index,
                    };
                    let entry = entries
                        .entry(id.clone())
                        .or_insert_with(|| SequenceEntryState {
                            after: predecessor.clone(),
                            node: None,
                            inserted: false,
                        });
                    if entry.inserted && entry.after != predecessor {
                        return Err(format!(
                            "sequence element {id:?} has conflicting predecessors"
                        ));
                    }
                    entry.after = predecessor;
                    let child = entry.node.get_or_insert_with(|| empty_node(element));
                    merge_value(child, element, value, dot)?;
                    entry.inserted = true;
                    predecessor = Some(id);
                }
                Ok(())
            }
            CrdtOp::SequenceDelete { dot, path, ids } => {
                let (node, policy) =
                    node_at_path_mut(&mut self.state.root, &self.spec.root, path, dot)?;
                if !matches!(policy, CrdtPolicy::Sequence { .. }) {
                    return Err("sequence_delete path does not address a sequence".into());
                }
                let NodeState::Sequence { deletes, .. } = node else {
                    return Err("sequence policy has incompatible reducer state".into());
                };
                for id in ids {
                    deletes.entry(id.clone()).or_default().insert(dot.clone());
                }
                Ok(())
            }
        }
    }
}

fn mutates_set_identity(policy: &CrdtPolicy, path: &[CrdtPathSegment]) -> Result<bool, String> {
    let mut policy = policy;
    for (index, segment) in path.iter().enumerate() {
        policy = match (segment, policy) {
            (CrdtPathSegment::Key { key }, CrdtPolicy::Map { fields, values }) => {
                fields.get(key).unwrap_or(values)
            }
            (CrdtPathSegment::Member { .. }, CrdtPolicy::Set { keys, element }) => {
                let remaining = &path[index + 1..];
                if keys.is_empty() {
                    return Ok(true);
                }
                let remaining_keys = remaining
                    .iter()
                    .map_while(|segment| match segment {
                        CrdtPathSegment::Key { key } => Some(key.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if keys.iter().any(|key_path| {
                    let key_path = key_path.iter().map(String::as_str).collect::<Vec<_>>();
                    key_path.starts_with(&remaining_keys) || remaining_keys.starts_with(&key_path)
                }) {
                    return Ok(true);
                }
                element
            }
            (CrdtPathSegment::Element { .. }, CrdtPolicy::Sequence { element, .. }) => element,
            _ => return Err("CRDT path segment does not match its policy".into()),
        };
    }
    Ok(false)
}

fn dot_covered(dot: &Dot, frontier: &VersionVector) -> bool {
    dot.replica == "$init" || frontier.get(&dot.replica).copied().unwrap_or(0) >= dot.counter
}

fn validate_replica(replica: String) -> Result<String, String> {
    if replica.is_empty() || replica == "$init" {
        Err("replica identifier must be non-empty and cannot be reserved $init".into())
    } else {
        Ok(replica)
    }
}

fn validate_operation_dot(dot: &Dot) -> Result<(), String> {
    if dot.counter == 0 || dot.replica.is_empty() || dot.replica == "$init" {
        Err(format!("invalid operation dot {dot:?}"))
    } else {
        Ok(())
    }
}

fn op_fingerprint(op: &CrdtOp) -> Result<String, String> {
    let encoded = serde_json::to_vec(op).map_err(|error| error.to_string())?;
    Ok(format!("sha256:{:x}", Sha256::digest(encoded)))
}

fn empty_node(policy: &CrdtPolicy) -> NodeState {
    match policy {
        CrdtPolicy::Register {} => NodeState::Register {
            value: None,
            dot: None,
        },
        CrdtPolicy::Map { .. } => NodeState::Map {
            entries: BTreeMap::new(),
        },
        CrdtPolicy::Set { .. } => NodeState::Set {
            entries: BTreeMap::new(),
        },
        CrdtPolicy::Sequence { .. } => NodeState::Sequence {
            entries: BTreeMap::new(),
            deletes: BTreeMap::new(),
        },
    }
}

fn node_from_value(policy: &CrdtPolicy, value: &Value, dot: &Dot) -> Result<NodeState, String> {
    let mut node = empty_node(policy);
    merge_value(&mut node, policy, value, dot)?;
    Ok(node)
}

fn merge_value(
    node: &mut NodeState,
    policy: &CrdtPolicy,
    value: &Value,
    dot: &Dot,
) -> Result<(), String> {
    match (node, policy) {
        (
            NodeState::Register {
                value: held,
                dot: stamp,
            },
            CrdtPolicy::Register {},
        ) => {
            if stamp.as_ref().is_none_or(|current| dot >= current) {
                *held = Some(value.clone());
                *stamp = Some(dot.clone());
            }
            Ok(())
        }
        (NodeState::Map { entries }, CrdtPolicy::Map { fields, values }) => {
            let Value::Map(map) = value else {
                return Err("map policy requires a map value".into());
            };
            for (key, child_value) in map {
                let child_policy = fields.get(key).unwrap_or(values);
                let entry = entries.entry(key.clone()).or_insert_with(|| MapEntryState {
                    node: empty_node(child_policy),
                    dots: BTreeSet::new(),
                    removed: BTreeSet::new(),
                    removal_ops: BTreeSet::new(),
                });
                entry.dots.insert(dot.clone());
                merge_value(&mut entry.node, child_policy, child_value, dot)?;
            }
            Ok(())
        }
        (NodeState::Set { entries }, CrdtPolicy::Set { keys, element }) => {
            let Value::List(items) = value else {
                return Err("set policy requires a list value".into());
            };
            for item in items {
                let key = member_key(keys, item)?;
                let entry = entries.entry(key).or_insert_with(|| SetEntryState {
                    node: empty_node(element),
                    adds: BTreeSet::new(),
                    removed: BTreeSet::new(),
                    removal_ops: BTreeSet::new(),
                });
                entry.adds.insert(dot.clone());
                merge_value(&mut entry.node, element, item, dot)?;
            }
            Ok(())
        }
        (
            NodeState::Sequence { entries, .. },
            CrdtPolicy::Sequence {
                materialization,
                element,
            },
        ) => {
            let values = match (materialization, value) {
                (SequenceMaterialization::List, Value::List(values)) => values.clone(),
                (SequenceMaterialization::String, Value::Str(value)) => value
                    .chars()
                    .map(|character| Value::Str(character.to_string()))
                    .collect(),
                (SequenceMaterialization::List, _) => {
                    return Err("list sequence policy requires a list value".into());
                }
                (SequenceMaterialization::String, _) => {
                    return Err("string sequence policy requires a string value".into());
                }
            };
            let mut predecessor = None;
            for (index, child_value) in values.iter().enumerate() {
                let index = u32::try_from(index)
                    .map_err(|_| "sequence contains too many initial elements")?;
                let id = ElementId {
                    dot: dot.clone(),
                    index,
                };
                let entry = entries
                    .entry(id.clone())
                    .or_insert_with(|| SequenceEntryState {
                        after: predecessor.clone(),
                        node: None,
                        inserted: false,
                    });
                entry.after = predecessor;
                let child = entry.node.get_or_insert_with(|| empty_node(element));
                merge_value(child, element, child_value, dot)?;
                entry.inserted = true;
                predecessor = Some(id);
            }
            Ok(())
        }
        _ => Err("CRDT policy has incompatible reducer state".into()),
    }
}

fn set_register(node: &mut NodeState, value: Value, dot: Dot) -> Result<(), String> {
    let NodeState::Register {
        value: held,
        dot: stamp,
    } = node
    else {
        return Err("register policy has incompatible reducer state".into());
    };
    if stamp.as_ref().is_none_or(|current| &dot >= current) {
        *held = Some(value);
        *stamp = Some(dot);
    }
    Ok(())
}

fn map_entry_visible(entry: &MapEntryState) -> bool {
    entry.dots.iter().any(|dot| !entry.removed.contains(dot))
}

fn set_entry_active_adds(entry: &SetEntryState) -> BTreeSet<Dot> {
    entry.adds.difference(&entry.removed).cloned().collect()
}

fn live_dots(node: &NodeState) -> BTreeSet<Dot> {
    match node {
        NodeState::Register { dot, .. } => dot.iter().cloned().collect(),
        NodeState::Map { entries } => entries
            .values()
            .filter(|entry| map_entry_visible(entry))
            .flat_map(|entry| entry.dots.iter().cloned().chain(live_dots(&entry.node)))
            .collect(),
        NodeState::Set { entries } => entries
            .values()
            .filter(|entry| !set_entry_active_adds(entry).is_empty())
            .flat_map(|entry| {
                set_entry_active_adds(entry)
                    .into_iter()
                    .chain(live_dots(&entry.node))
            })
            .collect(),
        NodeState::Sequence {
            entries, deletes, ..
        } => entries
            .iter()
            .filter(|(id, entry)| entry.inserted && !deletes.contains_key(*id))
            .flat_map(|(id, _)| [id.dot.clone()])
            .collect(),
    }
}

fn materialize_node(node: &NodeState, policy: &CrdtPolicy) -> Result<Value, String> {
    materialize_node_filtered(node, policy, &BTreeSet::new())?
        .ok_or_else(|| "root value is removed".into())
}

fn materialize_node_filtered(
    node: &NodeState,
    policy: &CrdtPolicy,
    removed: &BTreeSet<Dot>,
) -> Result<Option<Value>, String> {
    match (node, policy) {
        (NodeState::Register { value, dot }, CrdtPolicy::Register {}) => Ok(dot
            .as_ref()
            .filter(|dot| !removed.contains(*dot))
            .map(|_| value.clone().unwrap_or_default())),
        (NodeState::Map { entries }, CrdtPolicy::Map { fields, values }) => {
            let mut map = BTreeMap::new();
            for (key, entry) in entries {
                let child_policy = fields.get(key).unwrap_or(values);
                let removed: BTreeSet<_> = removed.union(&entry.removed).cloned().collect();
                if entry.dots.iter().any(|dot| !removed.contains(dot)) {
                    let value = materialize_node_filtered(&entry.node, child_policy, &removed)?
                        .unwrap_or_default();
                    map.insert(key.clone(), value);
                }
            }
            Ok(Some(Value::Map(map)))
        }
        (NodeState::Set { entries }, CrdtPolicy::Set { element, .. }) => {
            let mut values = Vec::new();
            for entry in entries.values() {
                let removed: BTreeSet<_> = removed.union(&entry.removed).cloned().collect();
                if entry.adds.iter().any(|dot| !removed.contains(dot)) {
                    if let Some(value) = materialize_node_filtered(&entry.node, element, &removed)?
                    {
                        values.push(value);
                    }
                }
            }
            Ok(Some(Value::List(values)))
        }
        (
            NodeState::Sequence { entries, deletes },
            CrdtPolicy::Sequence {
                materialization,
                element,
            },
        ) => {
            let values = ordered_sequence_ids(entries)
                .into_iter()
                .filter(|id| !deletes.contains_key(id) && !removed.contains(&id.dot))
                .filter_map(|id| entries.get(&id).and_then(|entry| entry.node.as_ref()))
                .map(|node| materialize_node_filtered(node, element, removed))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            match materialization {
                SequenceMaterialization::List => Ok(Some(Value::List(values))),
                SequenceMaterialization::String => {
                    let mut string = String::new();
                    for value in values {
                        let Value::Str(character) = value else {
                            return Err(
                                "string sequence element did not materialize as a string".into()
                            );
                        };
                        if character.chars().count() != 1 {
                            return Err(
                                "string sequence element must contain one Unicode scalar value"
                                    .into(),
                            );
                        }
                        string.push_str(&character);
                    }
                    Ok(Some(Value::Str(string)))
                }
            }
        }
        _ => Err("CRDT policy has incompatible reducer state".into()),
    }
}

fn ordered_sequence_ids(entries: &BTreeMap<ElementId, SequenceEntryState>) -> Vec<ElementId> {
    let mut children: BTreeMap<Option<ElementId>, Vec<ElementId>> = BTreeMap::new();
    for (id, entry) in entries.iter().filter(|(_, entry)| entry.inserted) {
        children
            .entry(entry.after.clone())
            .or_default()
            .push(id.clone());
    }
    let mut ordered = Vec::with_capacity(entries.len());
    let mut visited = BTreeSet::new();
    append_sequence_children(None, &children, &mut visited, &mut ordered);
    let unvisited = entries
        .keys()
        .filter(|id| !visited.contains(*id))
        .cloned()
        .collect::<Vec<_>>();
    for id in unvisited {
        if entries.get(&id).is_some_and(|entry| entry.inserted) && visited.insert(id.clone()) {
            ordered.push(id.clone());
            append_sequence_children(Some(id), &children, &mut visited, &mut ordered);
        }
    }
    ordered
}

fn visible_sequence_ids_at(
    root: &NodeState,
    policy: &CrdtPolicy,
    path: &[CrdtPathSegment],
) -> Result<Vec<ElementId>, String> {
    let (node, policy) = node_at_path(root, policy, path)?;
    if !matches!(policy, CrdtPolicy::Sequence { .. }) {
        return Err("sequence_splice path does not address a sequence".into());
    }
    let NodeState::Sequence { entries, deletes } = node else {
        return Err("sequence policy has incompatible reducer state".into());
    };
    Ok(ordered_sequence_ids(entries)
        .into_iter()
        .filter(|id| !deletes.contains_key(id))
        .collect())
}

fn append_sequence_children(
    parent: Option<ElementId>,
    children: &BTreeMap<Option<ElementId>, Vec<ElementId>>,
    visited: &mut BTreeSet<ElementId>,
    ordered: &mut Vec<ElementId>,
) {
    let mut stack = children
        .get(&parent)
        .into_iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    while let Some(id) = stack.pop() {
        if visited.insert(id.clone()) {
            ordered.push(id.clone());
            if let Some(descendants) = children.get(&Some(id)) {
                stack.extend(descendants.iter().cloned());
            }
        }
    }
}

fn policy_at_path<'a>(
    mut policy: &'a CrdtPolicy,
    path: &[CrdtPathSegment],
) -> Result<&'a CrdtPolicy, String> {
    for segment in path {
        policy = match (segment, policy) {
            (CrdtPathSegment::Key { key }, CrdtPolicy::Map { fields, values }) => {
                fields.get(key).unwrap_or(values)
            }
            (CrdtPathSegment::Member { .. }, CrdtPolicy::Set { element, .. }) => element,
            (CrdtPathSegment::Element { .. }, CrdtPolicy::Sequence { element, .. }) => element,
            _ => return Err("CRDT path segment does not match its policy".into()),
        };
    }
    Ok(policy)
}

fn node_at_path_mut<'a>(
    node: &'a mut NodeState,
    policy: &'a CrdtPolicy,
    path: &[CrdtPathSegment],
    dot: &Dot,
) -> Result<(&'a mut NodeState, &'a CrdtPolicy), String> {
    let Some((segment, rest)) = path.split_first() else {
        return Ok((node, policy));
    };
    match (segment, node, policy) {
        (
            CrdtPathSegment::Key { key },
            NodeState::Map { entries },
            CrdtPolicy::Map { fields, values },
        ) => {
            let child_policy = fields.get(key).unwrap_or(values);
            let entry = entries.entry(key.clone()).or_insert_with(|| MapEntryState {
                node: empty_node(child_policy),
                dots: BTreeSet::new(),
                removed: BTreeSet::new(),
                removal_ops: BTreeSet::new(),
            });
            entry.dots.insert(dot.clone());
            node_at_path_mut(&mut entry.node, child_policy, rest, dot)
        }
        (
            CrdtPathSegment::Member { key },
            NodeState::Set { entries },
            CrdtPolicy::Set { element, .. },
        ) => {
            let entry = entries.entry(key.clone()).or_insert_with(|| SetEntryState {
                node: empty_node(element),
                adds: BTreeSet::new(),
                removed: BTreeSet::new(),
                removal_ops: BTreeSet::new(),
            });
            node_at_path_mut(&mut entry.node, element, rest, dot)
        }
        (
            CrdtPathSegment::Element { id },
            NodeState::Sequence { entries, .. },
            CrdtPolicy::Sequence { element, .. },
        ) => {
            let entry = entries
                .entry(id.clone())
                .or_insert_with(|| SequenceEntryState {
                    after: None,
                    node: None,
                    inserted: false,
                });
            let child = entry.node.get_or_insert_with(|| empty_node(element));
            node_at_path_mut(child, element, rest, dot)
        }
        _ => Err("CRDT path segment does not match reducer state".into()),
    }
}

fn node_at_path<'a>(
    mut node: &'a NodeState,
    mut policy: &'a CrdtPolicy,
    path: &[CrdtPathSegment],
) -> Result<(&'a NodeState, &'a CrdtPolicy), String> {
    for segment in path {
        match (segment, node, policy) {
            (
                CrdtPathSegment::Key { key },
                NodeState::Map { entries },
                CrdtPolicy::Map { fields, values },
            ) => {
                let entry = entries
                    .get(key)
                    .ok_or_else(|| format!("unknown map key {key:?}"))?;
                node = &entry.node;
                policy = fields.get(key).unwrap_or(values);
            }
            (
                CrdtPathSegment::Member { key },
                NodeState::Set { entries },
                CrdtPolicy::Set { element, .. },
            ) => {
                node = &entries
                    .get(key)
                    .ok_or_else(|| format!("unknown set member {key:?}"))?
                    .node;
                policy = element;
            }
            (
                CrdtPathSegment::Element { id },
                NodeState::Sequence { entries, .. },
                CrdtPolicy::Sequence { element, .. },
            ) => {
                node = entries
                    .get(id)
                    .and_then(|entry| entry.node.as_ref())
                    .ok_or_else(|| format!("unknown sequence element {id:?}"))?;
                policy = element;
            }
            _ => return Err("CRDT path segment does not match reducer state".into()),
        }
    }
    Ok((node, policy))
}

fn map_entry_live_dots(
    root: &NodeState,
    policy: &CrdtPolicy,
    path: &[CrdtPathSegment],
    key: &str,
) -> Result<Vec<Dot>, String> {
    let (node, policy) = node_at_path(root, policy, path)?;
    let CrdtPolicy::Map { .. } = policy else {
        return Err("map_remove path does not address a map".into());
    };
    let NodeState::Map { entries } = node else {
        return Err("map policy has incompatible reducer state".into());
    };
    Ok(entries.get(key).map_or_else(Vec::new, |entry| {
        entry
            .dots
            .iter()
            .cloned()
            .chain(live_dots(&entry.node))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }))
}

fn set_member_active_adds(
    root: &NodeState,
    policy: &CrdtPolicy,
    path: &[CrdtPathSegment],
    key: &str,
) -> Result<Vec<Dot>, String> {
    let (node, policy) = node_at_path(root, policy, path)?;
    let CrdtPolicy::Set { .. } = policy else {
        return Err("set_remove path does not address a set".into());
    };
    let NodeState::Set { entries } = node else {
        return Err("set policy has incompatible reducer state".into());
    };
    Ok(entries.get(key).map_or_else(Vec::new, |entry| {
        entry
            .adds
            .iter()
            .chain(live_dots(&entry.node).iter())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }))
}

fn value_at_key_path<'a>(mut value: &'a Value, path: &[String]) -> Result<&'a Value, String> {
    for segment in path {
        let Value::Map(map) = value else {
            return Err(format!(
                "set key path {path:?} descends through a non-map value"
            ));
        };
        value = map
            .get(segment)
            .ok_or_else(|| format!("set key path {path:?} is missing segment {segment:?}"))?;
    }
    Ok(value)
}

fn member_key(keys: &[Vec<String>], value: &Value) -> Result<String, String> {
    if keys.is_empty() {
        return serde_json::to_string(value).map_err(|error| error.to_string());
    }
    let identity = keys
        .iter()
        .map(|path| Ok((path.clone(), value_at_key_path(value, path)?.clone())))
        .collect::<Result<Vec<_>, String>>()?;
    serde_json::to_string(&identity).map_err(|error| error.to_string())
}

fn delta_for(op: &CrdtOp, spec: &CrdtSpec, root: &NodeState) -> Result<CrdtDelta, String> {
    Ok(match op {
        CrdtOp::RegisterSet { path, value, .. } => CrdtDelta::RegisterSet {
            path: path.clone(),
            value: value.clone(),
        },
        CrdtOp::MapSet {
            path, key, value, ..
        } => CrdtDelta::MapSet {
            path: path.clone(),
            key: key.clone(),
            value: value.clone(),
        },
        CrdtOp::MapRemove { path, key, .. } => CrdtDelta::MapRemove {
            path: path.clone(),
            key: key.clone(),
        },
        CrdtOp::SetAdd { path, value, .. } => {
            let CrdtPolicy::Set { keys, .. } = policy_at_path(&spec.root, path)? else {
                return Err("set_add path does not address a set".into());
            };
            CrdtDelta::SetAdd {
                path: path.clone(),
                key: member_key(keys, value)?,
                value: value.clone(),
            }
        }
        CrdtOp::SetRemove { path, key, .. } => CrdtDelta::SetRemove {
            path: path.clone(),
            key: key.clone(),
        },
        CrdtOp::SequenceInsert {
            dot,
            path,
            after,
            values,
        } => {
            let (node, policy) = node_at_path(root, &spec.root, path)?;
            let CrdtPolicy::Sequence { element, .. } = policy else {
                return Err("sequence_insert path does not address a sequence".into());
            };
            let NodeState::Sequence { entries, .. } = node else {
                return Err("sequence policy has incompatible reducer state".into());
            };
            let elements = values
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    let id = ElementId {
                        dot: dot.clone(),
                        index: u32::try_from(index)
                            .map_err(|_| "sequence insert contains too many elements")?,
                    };
                    let value = entries
                        .get(&id)
                        .and_then(|entry| entry.node.as_ref())
                        .map(|node| materialize_node(node, element))
                        .transpose()?
                        .ok_or_else(|| format!("missing inserted sequence element {id:?}"))?;
                    Ok(SequenceDeltaElement { id, value })
                })
                .collect::<Result<Vec<_>, String>>()?;
            CrdtDelta::SequenceInsert {
                path: path.clone(),
                after: after.clone(),
                elements,
            }
        }
        CrdtOp::SequenceDelete { path, ids, .. } => CrdtDelta::SequenceDelete {
            path: path.clone(),
            ids: ids.clone(),
        },
    })
}

fn compact_node(node: &mut NodeState, policy: &CrdtPolicy, frontier: &VersionVector) -> usize {
    match (node, policy) {
        (NodeState::Register { .. }, CrdtPolicy::Register {}) => 0,
        (NodeState::Map { entries }, CrdtPolicy::Map { fields, values }) => {
            let mut compacted = 0;
            for (key, entry) in entries.iter_mut() {
                let child_policy = fields.get(key).unwrap_or(values);
                compacted += compact_node(&mut entry.node, child_policy, frontier);
            }
            let before = entries.len();
            entries.retain(|_, entry| {
                map_entry_visible(entry)
                    || entry.removal_ops.is_empty()
                    || !entry
                        .removal_ops
                        .iter()
                        .all(|dot| dot_covered(dot, frontier))
            });
            compacted + before - entries.len()
        }
        (NodeState::Set { entries }, CrdtPolicy::Set { element, .. }) => {
            let mut compacted = 0;
            for entry in entries.values_mut() {
                compacted += compact_node(&mut entry.node, element, frontier);
            }
            let before = entries.len();
            entries.retain(|_, entry| {
                !set_entry_active_adds(entry).is_empty()
                    || entry.removal_ops.is_empty()
                    || !entry
                        .removal_ops
                        .iter()
                        .all(|dot| dot_covered(dot, frontier))
            });
            compacted + before - entries.len()
        }
        (NodeState::Sequence { entries, deletes }, CrdtPolicy::Sequence { element, .. }) => {
            let mut compacted = 0;
            for (id, entry) in entries.iter_mut() {
                if let Some(child) = entry.node.as_mut() {
                    compacted += compact_node(child, element, frontier);
                    if deletes.get(id).is_some_and(|delete_ops| {
                        dot_covered(&id.dot, frontier)
                            && delete_ops.iter().all(|dot| dot_covered(dot, frontier))
                    }) {
                        entry.node = None;
                        compacted += 1;
                    }
                }
            }
            compacted
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply;

    fn register_spec() -> CrdtSpec {
        CrdtSpec {
            version: 1,
            root: CrdtPolicy::Register {},
        }
    }

    fn string_spec() -> CrdtSpec {
        CrdtSpec {
            version: 1,
            root: CrdtPolicy::Sequence {
                materialization: SequenceMaterialization::String,
                element: Box::new(CrdtPolicy::Register {}),
            },
        }
    }

    fn record_policy(fields: &[&str]) -> CrdtPolicy {
        CrdtPolicy::Map {
            fields: fields
                .iter()
                .map(|field| ((*field).into(), CrdtPolicy::Register {}))
                .collect(),
            values: Box::new(CrdtPolicy::Register {}),
        }
    }

    fn map(entries: &[(&str, Value)]) -> Value {
        Value::map(entries.iter().cloned())
    }

    fn assert_effect_applies(before: Value, effect: &CrdtEffect, after: Value) {
        let mut patched = before;
        apply(&mut patched, &effect.patch).unwrap();
        assert_eq!(patched, after);
    }

    #[test]
    fn register_ops_converge_and_duplicates_are_ignored() {
        let mut a = CrdtDocument::new(register_spec(), 0.into(), "a").unwrap();
        let mut b = CrdtDocument::new(register_spec(), 0.into(), "b").unwrap();
        let a_change = a
            .mutate(&[CrdtMutation::RegisterSet {
                path: vec![],
                value: 1.into(),
            }])
            .unwrap();
        let b_change = b
            .mutate(&[CrdtMutation::RegisterSet {
                path: vec![],
                value: 2.into(),
            }])
            .unwrap();

        let before = a.value().unwrap();
        let effect = a.apply(&b_change.ops).unwrap();
        assert_eq!(effect.applied, 1);
        assert_effect_applies(before, &effect, a.value().unwrap());
        assert_eq!(a.apply(&b_change.ops).unwrap().applied, 0);
        b.apply(&a_change.ops).unwrap();

        assert_eq!(a.value().unwrap(), 2.into());
        assert_eq!(a.value(), b.value());
        assert!(a.apply(&b_change.ops).unwrap().patch.ops.is_empty());
    }

    #[test]
    fn reusing_a_dot_with_different_content_is_rejected_atomically() {
        let mut document = CrdtDocument::new(register_spec(), 0.into(), "receiver").unwrap();
        let dot = Dot {
            counter: 1,
            replica: "sender".into(),
        };
        document
            .apply(&[CrdtOp::RegisterSet {
                dot: dot.clone(),
                path: vec![],
                value: 1.into(),
            }])
            .unwrap();
        let error = document
            .apply(&[CrdtOp::RegisterSet {
                dot,
                path: vec![],
                value: 2.into(),
            }])
            .unwrap_err();
        assert!(error.contains("reused with different content"));
        assert_eq!(document.value().unwrap(), 1.into());
    }

    #[test]
    fn nested_map_updates_win_over_concurrent_remove_without_resurrecting_old_fields() {
        let spec = CrdtSpec {
            version: 1,
            root: CrdtPolicy::Map {
                fields: [("profile".into(), record_policy(&["name", "city"]))]
                    .into_iter()
                    .collect(),
                values: Box::new(CrdtPolicy::Register {}),
            },
        };
        let initial = map(&[(
            "profile",
            map(&[("name", "Ada".into()), ("city", "London".into())]),
        )]);
        let mut a = CrdtDocument::new(spec.clone(), initial.clone(), "a").unwrap();
        let mut b = CrdtDocument::new(spec, initial, "b").unwrap();
        let remove = a
            .mutate(&[CrdtMutation::MapRemove {
                path: vec![],
                key: "profile".into(),
            }])
            .unwrap();
        let update = b
            .mutate(&[CrdtMutation::RegisterSet {
                path: vec![
                    CrdtPathSegment::Key {
                        key: "profile".into(),
                    },
                    CrdtPathSegment::Key { key: "name".into() },
                ],
                value: "Grace".into(),
            }])
            .unwrap();

        a.apply(&update.ops).unwrap();
        b.apply(&remove.ops).unwrap();
        let expected = map(&[("profile", map(&[("name", "Grace".into())]))]);
        assert_eq!(a.value().unwrap(), expected);
        assert_eq!(a.value(), b.value());
    }

    #[test]
    fn ancestor_map_remove_does_not_resurrect_observed_set_members() {
        let spec = CrdtSpec {
            version: 1,
            root: CrdtPolicy::Map {
                fields: [(
                    "tags".into(),
                    CrdtPolicy::Set {
                        keys: vec![],
                        element: Box::new(CrdtPolicy::Register {}),
                    },
                )]
                .into_iter()
                .collect(),
                values: Box::new(CrdtPolicy::Register {}),
            },
        };
        let initial = map(&[("tags", Value::List(vec!["x".into()]))]);
        let mut a = CrdtDocument::new(spec.clone(), initial.clone(), "a").unwrap();
        let mut b = CrdtDocument::new(spec, initial, "b").unwrap();
        let remove = a
            .mutate(&[CrdtMutation::MapRemove {
                path: vec![],
                key: "tags".into(),
            }])
            .unwrap();
        let update = b
            .mutate(&[CrdtMutation::MapSet {
                path: vec![],
                key: "tags".into(),
                value: Value::List(vec!["y".into()]),
            }])
            .unwrap();

        a.apply(&update.ops).unwrap();
        b.apply(&remove.ops).unwrap();
        let expected = map(&[("tags", Value::List(vec!["y".into()]))]);
        assert_eq!(a.value().unwrap(), expected);
        assert_eq!(a.value(), b.value());
    }

    #[test]
    fn keyed_set_merges_fields_and_preserves_add_wins_membership() {
        let spec = CrdtSpec {
            version: 1,
            root: CrdtPolicy::Set {
                keys: vec![vec!["id".into()]],
                element: Box::new(record_policy(&["id", "name", "city"])),
            },
        };
        let initial_member = map(&[
            ("id", 1.into()),
            ("name", "Ada".into()),
            ("city", "London".into()),
        ]);
        let initial = Value::List(vec![initial_member.clone()]);
        let mut a = CrdtDocument::new(spec.clone(), initial.clone(), "a").unwrap();
        let mut b = CrdtDocument::new(spec, initial, "b").unwrap();
        let key = a.member_key(&[], &initial_member).unwrap();
        let remove = a
            .mutate(&[CrdtMutation::SetRemove { path: vec![], key }])
            .unwrap();
        let added = map(&[
            ("id", 1.into()),
            ("name", "Grace".into()),
            ("city", "Paris".into()),
        ]);
        let add = b
            .mutate(&[CrdtMutation::SetAdd {
                path: vec![],
                value: added.clone(),
            }])
            .unwrap();

        a.apply(&add.ops).unwrap();
        b.apply(&remove.ops).unwrap();
        assert_eq!(a.value().unwrap(), Value::List(vec![added]));
        assert_eq!(a.value(), b.value());
        assert!(matches!(
            add.effect.deltas.as_slice(),
            [CrdtDelta::SetAdd { key: delta_key, .. }] if !delta_key.is_empty()
        ));
    }

    #[test]
    fn set_remove_wins_over_a_concurrent_field_only_update() {
        let spec = CrdtSpec {
            version: 1,
            root: CrdtPolicy::Set {
                keys: vec![vec!["id".into()]],
                element: Box::new(record_policy(&["id", "name"])),
            },
        };
        let member = map(&[("id", 1.into()), ("name", "Ada".into())]);
        let initial = Value::List(vec![member.clone()]);
        let mut a = CrdtDocument::new(spec.clone(), initial.clone(), "a").unwrap();
        let mut b = CrdtDocument::new(spec, initial, "b").unwrap();
        let key = a.member_key(&[], &member).unwrap();
        let remove = a
            .mutate(&[CrdtMutation::SetRemove {
                path: vec![],
                key: key.clone(),
            }])
            .unwrap();
        let update = b
            .mutate(&[CrdtMutation::RegisterSet {
                path: vec![
                    CrdtPathSegment::Member { key },
                    CrdtPathSegment::Key { key: "name".into() },
                ],
                value: "Grace".into(),
            }])
            .unwrap();

        a.apply(&update.ops).unwrap();
        b.apply(&remove.ops).unwrap();
        assert_eq!(a.value().unwrap(), Value::List(vec![]));
        assert_eq!(a.value(), b.value());
    }

    #[test]
    fn set_identity_fields_cannot_be_changed_in_place() {
        let spec = CrdtSpec {
            version: 1,
            root: CrdtPolicy::Set {
                keys: vec![vec!["id".into()]],
                element: Box::new(record_policy(&["id", "name"])),
            },
        };
        let member = map(&[("id", 1.into()), ("name", "Ada".into())]);
        let mut document = CrdtDocument::new(spec, Value::List(vec![member.clone()]), "a").unwrap();
        let key = document.member_key(&[], &member).unwrap();
        let error = document
            .mutate(&[CrdtMutation::RegisterSet {
                path: vec![
                    CrdtPathSegment::Member { key },
                    CrdtPathSegment::Key { key: "id".into() },
                ],
                value: 2.into(),
            }])
            .unwrap_err();
        assert!(error.contains("identity fields are immutable"));
        assert_eq!(document.value().unwrap(), Value::List(vec![member]));
    }

    #[test]
    fn local_dot_advances_past_observed_remote_counters() {
        let mut document = CrdtDocument::new(register_spec(), 0.into(), "local").unwrap();
        document
            .apply(&[CrdtOp::RegisterSet {
                dot: Dot {
                    counter: 10,
                    replica: "remote".into(),
                },
                path: vec![],
                value: 1.into(),
            }])
            .unwrap();
        let change = document
            .mutate(&[CrdtMutation::RegisterSet {
                path: vec![],
                value: 2.into(),
            }])
            .unwrap();
        assert!(matches!(
            &change.ops[0],
            CrdtOp::RegisterSet { dot, .. } if dot.counter == 11
        ));
        assert_eq!(document.value().unwrap(), 2.into());
    }

    #[test]
    fn sequence_ops_converge_when_insert_delete_and_duplicates_are_reordered() {
        let mut a = CrdtDocument::new(string_spec(), "".into(), "a").unwrap();
        let mut b = CrdtDocument::new(string_spec(), "".into(), "b").unwrap();
        let a_insert = a
            .mutate(&[CrdtMutation::SequenceInsert {
                path: vec![],
                after: None,
                values: vec!["A".into(), "λ".into()],
            }])
            .unwrap();
        let b_insert = b
            .mutate(&[CrdtMutation::SequenceInsert {
                path: vec![],
                after: None,
                values: vec!["B".into()],
            }])
            .unwrap();
        a.apply(&b_insert.ops).unwrap();
        b.apply(&a_insert.ops).unwrap();
        assert_eq!(a.value(), b.value());
        assert_eq!(a.value().unwrap(), "BAλ".into());

        let first_id = match &a_insert.ops[0] {
            CrdtOp::SequenceInsert { dot, .. } => ElementId {
                dot: dot.clone(),
                index: 0,
            },
            _ => unreachable!(),
        };
        let delete = a
            .mutate(&[CrdtMutation::SequenceDelete {
                path: vec![],
                ids: vec![first_id],
            }])
            .unwrap();
        let mut reordered = CrdtDocument::new(string_spec(), "".into(), "c").unwrap();
        reordered.apply(&delete.ops).unwrap();
        reordered.apply(&b_insert.ops).unwrap();
        reordered.apply(&a_insert.ops).unwrap();
        reordered.apply(&a_insert.ops).unwrap();
        a.apply(&b_insert.ops).unwrap();
        assert_eq!(reordered.value().unwrap(), "Bλ".into());
        assert!(matches!(
            a_insert.effect.deltas.as_slice(),
            [CrdtDelta::SequenceInsert { elements, .. }] if elements.len() == 2
        ));
    }

    #[test]
    fn sequence_splice_expands_positions_to_stable_element_ids_atomically() {
        let mut document = CrdtDocument::new(string_spec(), "AλB".into(), "editor").unwrap();
        let change = document
            .mutate(&[CrdtMutation::SequenceSplice {
                path: vec![],
                index: 1,
                delete_count: 1,
                values: vec!["🙂".into()],
            }])
            .unwrap();

        assert_eq!(document.value().unwrap(), "A🙂B".into());
        assert_eq!(change.ops.len(), 2);
        assert_eq!(change.effect.applied, 2);
        assert!(matches!(&change.ops[0], CrdtOp::SequenceDelete { ids, .. } if ids.len() == 1));
        assert!(
            matches!(&change.ops[1], CrdtOp::SequenceInsert { after: Some(_), values, .. } if values == &vec!["🙂".into()])
        );

        let before = document.value().unwrap();
        assert!(document
            .mutate(&[CrdtMutation::SequenceSplice {
                path: vec![],
                index: 4,
                delete_count: 0,
                values: vec!["!".into()],
            }])
            .unwrap_err()
            .contains("out of bounds"));
        assert_eq!(document.value().unwrap(), before);

        let no_op = document
            .mutate(&[CrdtMutation::SequenceSplice {
                path: vec![],
                index: 3,
                delete_count: 0,
                values: vec![],
            }])
            .unwrap();
        assert!(no_op.ops.is_empty());
        assert_eq!(no_op.effect.applied, 0);
    }

    #[test]
    fn long_sequences_materialize_without_recursive_traversal() {
        let text = "x".repeat(20_000);
        let document = CrdtDocument::new(string_spec(), text.clone().into(), "a").unwrap();
        assert_eq!(document.value().unwrap(), text.into());
    }

    #[test]
    fn state_transfer_rejects_other_specs_and_compaction_preserves_sequence_anchors() {
        let mut source = CrdtDocument::new(string_spec(), "".into(), "a").unwrap();
        let insert = source
            .mutate(&[CrdtMutation::SequenceInsert {
                path: vec![],
                after: None,
                values: vec!["x".into()],
            }])
            .unwrap();
        let id = match &insert.ops[0] {
            CrdtOp::SequenceInsert { dot, .. } => ElementId {
                dot: dot.clone(),
                index: 0,
            },
            _ => unreachable!(),
        };
        source
            .mutate(&[CrdtMutation::SequenceDelete {
                path: vec![],
                ids: vec![id.clone()],
            }])
            .unwrap();
        let state: CrdtState =
            serde_json::from_str(&serde_json::to_string(source.state()).unwrap()).unwrap();
        let restored = CrdtDocument::from_state(string_spec(), state.clone(), "b").unwrap();
        assert_eq!(restored.value(), source.value());
        assert!(CrdtDocument::from_state(register_spec(), state, "b").is_err());

        let compacted = source
            .compact(&[("a".into(), 2)].into_iter().collect())
            .unwrap();
        assert_eq!(compacted, 1);
        let NodeState::Sequence { entries, .. } = &source.state.root else {
            unreachable!()
        };
        assert!(entries.contains_key(&id));
        assert!(entries[&id].node.is_none());
        assert_eq!(source.value().unwrap(), "".into());
    }

    #[test]
    fn map_tombstones_compact_after_their_remove_is_stable() {
        let spec = CrdtSpec {
            version: 1,
            root: CrdtPolicy::Map {
                fields: BTreeMap::new(),
                values: Box::new(CrdtPolicy::Register {}),
            },
        };
        let mut document = CrdtDocument::new(spec, map(&[("obsolete", 1.into())]), "a").unwrap();
        document
            .mutate(&[CrdtMutation::MapRemove {
                path: vec![],
                key: "obsolete".into(),
            }])
            .unwrap();
        assert_eq!(
            document.compact(&[("a".into(), 1)].into_iter().collect()),
            Ok(1)
        );
        assert_eq!(document.value().unwrap(), map(&[]));
        assert!(document
            .compact(&[("a".into(), 2)].into_iter().collect())
            .unwrap_err()
            .contains("not covered by contiguous observed dots"));
    }

    #[test]
    fn compaction_rejects_gaps_in_observed_replica_counters() {
        let mut document = CrdtDocument::new(register_spec(), 0.into(), "receiver").unwrap();
        document
            .apply(&[CrdtOp::RegisterSet {
                dot: Dot {
                    counter: 5,
                    replica: "sender".into(),
                },
                path: vec![],
                value: 5.into(),
            }])
            .unwrap();
        assert!(document
            .compact(&[("sender".into(), 5)].into_iter().collect())
            .unwrap_err()
            .contains("not covered by contiguous observed dots"));
    }

    #[test]
    fn reserved_replica_identifiers_are_rejected() {
        assert!(CrdtDocument::new(register_spec(), Value::Null, "").is_err());
        assert!(CrdtDocument::new(register_spec(), Value::Null, "$init").is_err());
    }
}
