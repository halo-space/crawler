mod redis {
    #[path = "../support/scheduler/conformance.rs"]
    mod conformance;
    mod contract;
    mod coordination;
    mod error;
    mod integrity;
    mod items;
    mod key;
    mod lifecycle;
    mod precision;
    mod processing;
    mod request;
    mod server;
    mod settlement;
    mod worker;
}
