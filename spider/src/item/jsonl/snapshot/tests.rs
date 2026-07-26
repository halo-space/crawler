use super::*;

#[test]
fn canonical_json_sorts_nested_object_keys() {
    let first = serde_json::json!({
        "outer": {
            "z": 1,
            "a": {"d": 4, "b": 2}
        }
    });
    let second = serde_json::json!({
        "outer": {
            "a": {"b": 2, "d": 4},
            "z": 1
        }
    });

    assert_eq!(canonical(&first).unwrap(), canonical(&second).unwrap());
}
