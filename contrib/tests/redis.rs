mod redis {
    #[path = "../support/scheduler/conformance.rs"]
    mod conformance;
    mod contract;
    mod coordination;
    mod eligibility;
    mod error;
    mod integrity;
    mod key;
    mod lifecycle;
    mod precision;
    mod processing;
    mod ready;
    mod request;
    mod server;
    mod settlement;
    mod worker;
}
