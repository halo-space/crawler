ALTER TABLE tasks
    ADD KEY tasks_history (namespace, updated_time, id);

ALTER TABLE traces
    ADD KEY traces_history (namespace, created_time, id);

ALTER TABLE requests
    ADD KEY requests_history (namespace, created_time, id),
    ADD KEY requests_trace_history (namespace, trace_id, created_time, id),
    ADD KEY requests_state_history (namespace, state, created_time, id),
    ADD KEY requests_worker_history (namespace, leased_by, created_time, id);

ALTER TABLE items
    ADD KEY items_history (namespace, created_time, id);
