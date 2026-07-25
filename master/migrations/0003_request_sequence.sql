ALTER TABLE requests
    ADD COLUMN sequence BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    ADD UNIQUE KEY requests_sequence (sequence),
    DROP KEY requests_claim,
    ADD KEY requests_claim (namespace, state, mode, next_time, priority, sequence);
