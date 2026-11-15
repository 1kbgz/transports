use transports::{ClientEffect, ClientState, Message, ModelId, Store, Value};

#[test]
fn retirement_clears_only_missing_models_and_their_proposals() {
    let mut state = ClientState::new();
    for id in [1, 2] {
        let message = Message::Snapshot {
            id,
            model_type: "Counter".into(),
            rev: 3,
            value: Value::from(1_i64),
        };
        state.commit(&state.prepare(&message).unwrap());
        state
            .proposal_json(id, "[]", Some(&format!("edit-{id}")))
            .unwrap();
    }
    let inventory = Message::Models { ids: vec![2, 3] };
    let effect = state.prepare(&inventory).unwrap();
    assert_eq!(
        effect,
        ClientEffect::from_json(&state.prepare_json(&inventory.to_json().unwrap()).unwrap())
            .unwrap()
    );
    assert_eq!(
        effect,
        ClientEffect::Removed {
            ids: vec![1],
            proposals: vec!["edit-1".into()]
        }
    );
    assert_eq!(state.type_name(1), Some("Counter")); // prepare is transactional
    state.commit(&effect);
    assert_eq!(state.type_name(1), None);
    assert_eq!(state.type_name(2), Some("Counter"));
    assert_eq!(state.revisions_json().unwrap(), r#"{"2":3}"#);
    assert_eq!(state.pending(), vec!["edit-2"]);
    assert_eq!(
        state.prepare(&inventory).unwrap(),
        ClientEffect::Removed {
            ids: vec![],
            proposals: vec![]
        }
    );
    for codec in ["json", "msgpack", "cbor"] {
        assert_eq!(
            Message::decode(&inventory.encode(codec).unwrap(), codec).unwrap(),
            inventory
        );
    }
    assert!(state.prepare_json(r#"{"t":"models","ids":[-1]}"#).is_err());
}

#[test]
fn unhost_releases_state_without_reusing_ids() {
    let mut store = Store::new();
    let id = store.host("Counter", Value::from(1_i64));
    assert!(store.unhost(id));
    assert!(!store.unhost(id));
    assert!(store.is_empty());
    assert!(store.snapshot(id).is_none());
    assert!(store.mutate(id, Value::from(2_i64)).is_none());
    assert_eq!(store.host("Counter", Value::from(3_i64)), ModelId(id.0 + 1));
}
