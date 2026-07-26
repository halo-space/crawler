use std::collections::HashMap;

use serde::Serialize;
use serde_json::{Value, json};

use super::operation::digest;
use super::request::validate_new_snapshot;
use super::task::next_period;

#[derive(Serialize)]
struct Body {
    values: HashMap<String, Value>,
}

#[test]
fn operation_digest_ignores_map_insertion_order_recursively() {
    let mut left_nested = HashMap::new();
    left_nested.insert("z".to_string(), json!(3));
    left_nested.insert("a".to_string(), json!(1));
    let mut left = HashMap::new();
    left.insert("second".to_string(), json!(left_nested));
    left.insert("first".to_string(), json!(true));

    let mut right_nested = HashMap::new();
    right_nested.insert("a".to_string(), json!(1));
    right_nested.insert("z".to_string(), json!(3));
    let mut right = HashMap::new();
    right.insert("first".to_string(), json!(true));
    right.insert("second".to_string(), json!(right_nested));

    assert_eq!(
        digest(&Body { values: left }).unwrap(),
        digest(&Body { values: right }).unwrap(),
    );
}

#[test]
fn new_snapshot_requires_initial_execution_state() {
    let mut request = spider::net::Request::follow("https://example.com/article")
        .unwrap()
        .node("detail");
    request.task_id = "task-a".to_string();
    request.trace_id = "trace-a".to_string();
    let mut snapshot = spider::net::request::Snapshot::try_from(request).unwrap();
    let trace = spider::trace::Snapshot::code("task-a");

    assert!(validate_new_snapshot(&snapshot, &trace).is_ok());

    snapshot.version = 1;
    assert!(
        validate_new_snapshot(&snapshot, &trace)
            .unwrap_err()
            .to_string()
            .contains("new Request Snapshot version must be 0")
    );
}

#[test]
fn traced_snapshot_must_restore_against_its_trace() {
    let mut request = spider::net::Request::follow("https://example.com/article")
        .unwrap()
        .node("detail");
    request.task_id = "task-a".to_string();
    request.trace_id = "trace-a".to_string();
    let snapshot = spider::net::request::Snapshot::try_from(request).unwrap();
    let trace = spider::trace::Snapshot::code("task-a");
    let other = spider::trace::Snapshot::code("task-b");

    assert!(validate_new_snapshot(&snapshot, &trace).is_ok());
    assert!(validate_new_snapshot(&snapshot, &other).is_err());
}

#[test]
fn periodic_schedule_advances_to_the_next_boundary() {
    assert_eq!(next_period(0, 10), 10);
    assert_eq!(next_period(10, 10), 20);
    assert_eq!(next_period(19, 10), 20);
}
