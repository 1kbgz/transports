use transports::{
    decode_message, encode_message, json_to_cbor, json_to_msgpack, ClientEffect, ClientState,
    Message,
};

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/live_protocol.json")).unwrap()
}

fn json(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap()
}

fn assert_effect(
    state: &ClientState,
    message: &serde_json::Value,
    expected: &serde_json::Value,
) -> String {
    let effect = state.prepare_json(&json(message)).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&effect).unwrap(),
        *expected
    );
    effect
}

#[test]
fn shared_client_state_trace() {
    let fixture = fixture();
    let effects = &fixture["effects"];
    let mut state = ClientState::new();

    let effect = assert_effect(&state, &fixture["snapshot"], &effects["snapshot"]);
    state.commit_json(&effect).unwrap();
    state.proposal_json(7, "[]", Some("editor-1")).unwrap();
    state.proposal_json(7, "[]", None).unwrap();

    let effect = assert_effect(&state, &fixture["patch"], &effects["patch"]);
    state.commit_json(&effect).unwrap();
    assert_eq!(state.pending(), vec!["auto-1"]);

    let mut stale = fixture["patch"].clone();
    stale["proposal"] = "auto-1".into();
    let effect = assert_effect(&state, &stale, &effects["stale_patch"]);
    state.commit_json(&effect).unwrap();

    state.proposal_json(7, "[]", Some("editor-2")).unwrap();
    let effect = assert_effect(&state, &fixture["ack"], &effects["ack"]);
    state.commit_json(&effect).unwrap();
    state.proposal_json(7, "[]", Some("editor-3")).unwrap();
    let effect = assert_effect(&state, &fixture["reject"], &effects["reject"]);
    state.commit_json(&effect).unwrap();
    let effect = assert_effect(&state, &fixture["unknown"], &effects["unknown"]);
    state.commit_json(&effect).unwrap();

    state.proposal_json(7, "[]", Some("editor-4")).unwrap();
    assert_eq!(
        state.disconnect(),
        ClientEffect::from_json(&json(&effects["disconnect"])).unwrap()
    );
    assert_eq!(state.revisions_json().unwrap(), r#"{"7":3}"#);
    assert!(state.pending().is_empty());
}

#[test]
fn shared_message_codec_trace() {
    let fixture = fixture();
    for name in ["snapshot", "patch", "ack", "reject", "batch", "unknown"] {
        let message = json(&fixture[name]);
        for codec in ["json", "msgpack", "cbor"] {
            let decoded = decode_message(&encode_message(&message, codec).unwrap(), codec).unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&decoded).unwrap(),
                fixture[name]
            );
        }
        assert_eq!(
            encode_message(&message, "msgpack").unwrap(),
            json_to_msgpack(&message).unwrap()
        );
        assert_eq!(
            encode_message(&message, "cbor").unwrap(),
            json_to_cbor(&message).unwrap()
        );
        assert!(Message::from_json(&message).is_ok());
    }
}
