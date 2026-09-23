use serde::Deserialize;
use transports::{CrdtDocument, CrdtMutation, CrdtOp, CrdtSpec, Value};

#[derive(Deserialize)]
struct Fixture {
    spec: CrdtSpec,
    initial: Value,
    mutations: Vec<CrdtMutation>,
    ops: Vec<CrdtOp>,
    expected: Value,
}

#[test]
fn shared_crdt_reducer_fixture() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("fixtures/crdt_reducer.json")).unwrap();
    let mut source = CrdtDocument::new(fixture.spec.clone(), fixture.initial.clone(), "a").unwrap();
    let change = source.mutate(&fixture.mutations).unwrap();
    assert_eq!(change.ops, fixture.ops);
    assert_eq!(source.value().unwrap(), fixture.expected);

    let mut receiver = CrdtDocument::new(fixture.spec, fixture.initial, "b").unwrap();
    receiver.apply(&change.ops).unwrap();
    receiver.apply(&change.ops).unwrap();
    assert_eq!(receiver.value().unwrap(), fixture.expected);
}
