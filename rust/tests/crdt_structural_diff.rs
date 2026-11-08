use transports::{CrdtDocument, CrdtSpec, Value};

#[test]
fn structural_diff_preserves_concurrent_members_and_fields() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/crdt_structural_diff.json")).unwrap();
    let spec = CrdtSpec::from_json(&fixture["spec"].to_string()).unwrap();
    let value = |key: &str| serde_json::from_value::<Value>(fixture[key].clone()).unwrap();
    let mut left = CrdtDocument::new(spec.clone(), value("initial"), "a").unwrap();
    let mut right = CrdtDocument::new(spec, value("initial"), "b").unwrap();
    let state = serde_json::to_value(left.state()).unwrap();
    let plan = left.diff(&[], &value("left")).unwrap();
    assert_eq!(serde_json::to_value(left.state()).unwrap(), state);
    let l = left.mutate(&plan).unwrap();
    let r = right
        .mutate(&right.diff(&[], &value("right")).unwrap())
        .unwrap();
    left.apply(&r.ops).unwrap();
    right.apply(&l.ops).unwrap();
    left.apply(&r.ops).unwrap();
    assert_eq!(left.value().unwrap(), value("merged"));
    assert_eq!(right.value().unwrap(), value("merged"));
    assert!(left.diff(&[], &value("merged")).unwrap().is_empty());
}

#[test]
fn structural_diff_rejects_duplicate_keys_without_changing_state() {
    let spec = CrdtSpec::from_json(r#"{"root":{"kind":"map","values":{"kind":"set","keys":[["id"]],"element":{"kind":"map"}}}}"#).unwrap();
    let initial = Value::Map(Default::default());
    let mut doc = CrdtDocument::new(spec, initial.clone(), "a").unwrap();
    let row = Value::Map(std::collections::BTreeMap::from([(
        "id".into(),
        Value::Str("a".into()),
    )]));
    assert!(doc
        .diff(
            &["rows".into()],
            &Value::List(vec![row.clone(), row.clone()])
        )
        .unwrap_err()
        .contains("duplicate"));
    assert_eq!(doc.value().unwrap(), initial);
    let plan = doc.diff(&["rows".into()], &Value::List(vec![row])).unwrap();
    doc.mutate(&plan).unwrap();
    assert!(doc.diff(&[], &doc.value().unwrap()).unwrap().is_empty());
}
