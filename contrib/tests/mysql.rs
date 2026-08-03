mod mysql {
    #[path = "../support/scheduler/conformance.rs"]
    mod conformance;
    mod contract;
    mod coordination;
    mod fixture;
    mod integrity;
    mod server;
    mod settlement;
    mod worker;
}
