use spider::scheduler::Error;

#[test]
fn scheduler_errors_have_stable_classes() {
    let unavailable = Error::Unavailable("offline".to_string());
    assert!(unavailable.is_transient());
    assert!(!unavailable.is_ownership_loss());

    for error in [
        Error::IdentityMismatch {
            id: "request".to_string(),
            field: "task_id",
        },
        Error::LeaseMismatch("request".to_string()),
        Error::LeaseExpired("request".to_string()),
        Error::NotAcknowledged("request".to_string()),
        Error::StateMismatch("request".to_string()),
        Error::VersionMismatch("request".to_string()),
        Error::RequestNotFound("request".to_string()),
    ] {
        assert!(!error.is_transient());
        assert!(error.is_ownership_loss());
    }

    for error in [
        Error::TraceNotFound("trace".to_string()),
        Error::InvalidTrace {
            id: "trace".to_string(),
            message: "invalid".to_string(),
        },
        Error::InvalidRequest {
            id: "request".to_string(),
            message: "invalid".to_string(),
        },
        Error::Message("invalid".to_string()),
    ] {
        assert!(!error.is_transient());
        assert!(!error.is_ownership_loss());
    }
}
