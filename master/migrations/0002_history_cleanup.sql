ALTER TABLE requests
    ADD KEY requests_cleanup (namespace, state, updated_time, id);

ALTER TABLE request_completions
    ADD KEY request_completions_cleanup (namespace, created_time, request_id, version);

ALTER TABLE operations
    ADD KEY operations_cleanup (namespace, updated_time, kind, operation_key);

ALTER TABLE events
    ADD KEY events_cleanup (namespace, created_time, id);
