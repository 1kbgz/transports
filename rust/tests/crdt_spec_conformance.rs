use transports::CrdtSpec;

#[test]
fn shared_crdt_spec_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/crdt_spec.json")).unwrap();
    let spec = CrdtSpec::from_json(&fixture["spec"].to_string()).unwrap();

    assert_eq!(spec.to_json().unwrap(), fixture["canonical"]);
    assert_eq!(spec.hash().unwrap(), fixture["hash"]);
    spec.require_hash(fixture["hash"].as_str().unwrap())
        .unwrap();
}
