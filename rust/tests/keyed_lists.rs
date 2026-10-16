use transports::{apply, diff, diff_with_list_keys, ListKey, Op, PathSeg, Value};

fn rules() -> Vec<ListKey> {
    vec![ListKey {
        path: vec![PathSeg::Key("rows".into())],
        key: vec![PathSeg::Key("id".into())],
    }]
}

fn value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Object(map) => {
            Value::Map(map.iter().map(|(k, v)| (k.clone(), value(v))).collect())
        }
        serde_json::Value::Array(items) => Value::List(items.iter().map(value).collect()),
        serde_json::Value::String(s) => Value::Str(s.clone()),
        serde_json::Value::Number(n) => Value::Int(n.as_i64().unwrap()),
        _ => panic!("unsupported fixture value"),
    }
}

#[test]
fn shared_keyed_fixtures() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/keyed_lists.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let old = value(&case["old"]);
        let new = value(&case["new"]);
        let patch = diff_with_list_keys(&old, &new, &rules());
        let mut actual = old.clone();
        apply(&mut actual, &patch).unwrap();
        assert_eq!(actual, new, "{}", case["name"]);
        if case["fallback"] == true {
            assert_eq!(patch, diff(&old, &new));
        } else {
            let encoded = serde_json::to_value(&patch).unwrap();
            let kinds: Vec<_> = encoded["ops"]
                .as_array()
                .unwrap()
                .iter()
                .map(|op| op.as_object().unwrap().keys().next().unwrap().clone())
                .collect();
            assert_eq!(
                serde_json::to_value(kinds).unwrap(),
                case["kinds"],
                "{}",
                case["name"]
            );
        }
    }
}

fn rows(ids: impl Iterator<Item = i64>, changed: bool) -> Value {
    Value::map([(
        "rows",
        Value::List(
            ids.map(|id| {
                Value::map([
                    ("id", Value::Int(id)),
                    (
                        "label",
                        Value::Str(if changed {
                            format!("new-{id}")
                        } else {
                            id.to_string()
                        }),
                    ),
                ])
            })
            .collect(),
        ),
    )])
}

#[test]
fn dense_keyed_reorder_is_bounded_and_preserves_identity() {
    let old = rows(0..10000, false);
    let new = rows((0..10000).rev(), true);
    let patch = diff_with_list_keys(&old, &new, &rules());
    assert!(matches!(patch.ops[0], Op::Reorder { .. }));
    assert_eq!(patch.ops.len(), 10001);
    let mut actual = old;
    apply(&mut actual, &patch).unwrap();
    assert_eq!(actual, new);
}

#[test]
fn keyed_membership_and_order_round_trip() {
    let mut seed = 123u64;
    for _ in 0..300 {
        let mut ids: Vec<_> = (0..32).collect();
        for i in (1..ids.len()).rev() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            ids.swap(i, seed as usize % (i + 1));
        }
        let old = rows(ids[..24].iter().copied(), false);
        let new = rows(ids[8..].iter().rev().copied(), true);
        let patch = diff_with_list_keys(&old, &new, &rules());
        let mut actual = old;
        apply(&mut actual, &patch).unwrap();
        assert_eq!(actual, new);
    }
}
