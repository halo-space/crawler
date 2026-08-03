-- MySQL Scheduler schema.
--
-- One Scheduler deployment owns one database selected by its DSN. The schema
-- intentionally has no namespace column and must be installed explicitly by
-- the operator before the Scheduler starts.

CREATE TABLE traces (
    id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    task_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    snapshot JSON NOT NULL,
    created_time DATETIME(3) NOT NULL,
    updated_time DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    KEY idx_traces_task (task_id, created_time, id),
    CONSTRAINT chk_traces_time CHECK (updated_time >= created_time)
) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_bin;

-- requests is the authoritative Request record. snapshot and snapshot_hash
-- never change after insertion; claim, lease, retry, and terminal fields are
-- the mutable execution overlay applied when the Snapshot is restored.
CREATE TABLE requests (
    id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    task_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    trace_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    node VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    mode VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    priority INT NOT NULL,
    snapshot JSON NOT NULL,
    snapshot_hash BINARY(32) NOT NULL,
    state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    version BIGINT NOT NULL,
    next_time BIGINT NOT NULL,
    leased_by VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '',
    lease_time BIGINT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL,
    max_retry_count INT NOT NULL,
    ack_version BIGINT NULL,
    created_time DATETIME(3) NOT NULL,
    updated_time DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    KEY idx_requests_trace (trace_id, created_time, id),
    KEY idx_requests_state (state, created_time, id),
    KEY idx_requests_lease (state, lease_time, id),
    KEY idx_requests_worker (state, leased_by, id),
    KEY idx_requests_cleanup (state, updated_time, id),
    CONSTRAINT chk_requests_mode CHECK (mode IN ('http', 'browser')),
    CONSTRAINT chk_requests_state CHECK (state IN ('pending', 'processing', 'done', 'failed')),
    CONSTRAINT chk_requests_version CHECK (version >= 0),
    CONSTRAINT chk_requests_next_time CHECK (next_time >= 0),
    CONSTRAINT chk_requests_retry CHECK (
        retry_count >= 0
        AND max_retry_count BETWEEN 1 AND 128
        AND retry_count <= max_retry_count
    ),
    CONSTRAINT chk_requests_ack CHECK (
        ack_version IS NULL OR (ack_version > 0 AND ack_version = version)
    ),
    CONSTRAINT chk_requests_lease CHECK (
        (state = 'processing' AND leased_by <> '' AND lease_time > 0)
        OR (state <> 'processing' AND leased_by = '' AND lease_time = 0 AND ack_version IS NULL)
    ),
    CONSTRAINT chk_requests_time CHECK (updated_time >= created_time)
) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_bin;

-- queues is a narrow pending projection. Deleting the row on claim and
-- inserting a new row on release or retry gives every enqueue a fresh global
-- FIFO sequence while delayed Requests retain their original sequence.
CREATE TABLE queues (
    sequence BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    request_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    mode VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    priority INT NOT NULL,
    next_time BIGINT NOT NULL,
    created_time DATETIME(3) NOT NULL,
    updated_time DATETIME(3) NOT NULL,
    PRIMARY KEY (sequence),
    UNIQUE KEY uq_queues_request (request_id),
    KEY idx_queues_claim (mode, priority DESC, sequence, next_time, request_id),
    KEY idx_queues_due (mode, next_time, priority DESC, sequence, request_id),
    CONSTRAINT chk_queues_mode CHECK (mode IN ('http', 'browser')),
    CONSTRAINT chk_queues_next_time CHECK (next_time >= 0),
    CONSTRAINT chk_queues_time CHECK (updated_time >= created_time)
) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_bin;

-- A Worker is excluded from claiming a Request after an acknowledged failed
-- execution. position preserves the order exposed as Request.failed_workers.
CREATE TABLE failed_workers (
    request_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    worker_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    position INT NOT NULL,
    created_time DATETIME(3) NOT NULL,
    updated_time DATETIME(3) NOT NULL,
    PRIMARY KEY (request_id, worker_id),
    UNIQUE KEY uq_failed_workers_position (request_id, position),
    KEY idx_failed_workers_worker (worker_id, request_id),
    CONSTRAINT chk_failed_workers_position CHECK (position BETWEEN 1 AND 128),
    CONSTRAINT chk_failed_workers_time CHECK (updated_time >= created_time)
) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_bin;

-- One completion per execution generation makes success/failure replay
-- idempotent. A failed completion can coexist with a pending Request when the
-- queue-level retry budget has not been exhausted.
CREATE TABLE completions (
    request_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    version BIGINT NOT NULL,
    task_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    trace_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    node VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    worker_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    error LONGTEXT NULL,
    start_time BIGINT NULL,
    end_time BIGINT NULL,
    created_time DATETIME(3) NOT NULL,
    updated_time DATETIME(3) NOT NULL,
    PRIMARY KEY (request_id, version),
    KEY idx_completions_trace (trace_id, created_time, request_id, version),
    KEY idx_completions_cleanup (updated_time, request_id, version),
    CONSTRAINT chk_completions_version CHECK (version >= 0),
    CONSTRAINT chk_completions_state CHECK (state IN ('done', 'failed')),
    CONSTRAINT chk_completions_error CHECK (
        (state = 'done' AND error IS NULL)
        OR (state = 'failed' AND error IS NOT NULL AND CHAR_LENGTH(error) > 0)
    ),
    CONSTRAINT chk_completions_execution_time CHECK (
        (start_time IS NULL AND end_time IS NULL)
        OR (start_time >= 0 AND end_time >= start_time)
    ),
    CONSTRAINT chk_completions_time CHECK (updated_time >= created_time)
) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE trace_stats (
    trace_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    name VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    total BIGINT NOT NULL DEFAULT 0,
    done BIGINT NOT NULL DEFAULT 0,
    `filter` BIGINT NOT NULL DEFAULT 0,
    dedup BIGINT NOT NULL DEFAULT 0,
    `validate` BIGINT NOT NULL DEFAULT 0,
    download BIGINT NOT NULL DEFAULT 0,
    created_time DATETIME(3) NOT NULL,
    updated_time DATETIME(3) NOT NULL,
    PRIMARY KEY (trace_id, name),
    KEY idx_trace_stats_updated (updated_time, trace_id, name),
    CONSTRAINT chk_trace_stats_values CHECK (
        total >= 0
        AND done >= 0
        AND `filter` >= 0
        AND dedup >= 0
        AND `validate` >= 0
        AND download >= 0
    ),
    CONSTRAINT chk_trace_stats_time CHECK (updated_time >= created_time)
) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE workers (
    worker_id VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    host VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    ip VARCHAR(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    version VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    modes JSON NOT NULL,
    concurrency INT UNSIGNED NOT NULL,
    heartbeat_timeout BIGINT NOT NULL,
    last_heartbeat BIGINT NOT NULL,
    token VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    offline_time BIGINT NULL,
    created_time DATETIME(3) NOT NULL,
    updated_time DATETIME(3) NOT NULL,
    PRIMARY KEY (worker_id),
    KEY idx_workers_online (offline_time, last_heartbeat, worker_id),
    CONSTRAINT chk_workers_concurrency CHECK (concurrency > 0),
    CONSTRAINT chk_workers_heartbeat CHECK (
        heartbeat_timeout > 0 AND last_heartbeat >= 0
    ),
    CONSTRAINT chk_workers_offline CHECK (
        offline_time IS NULL OR offline_time > 0
    ),
    CONSTRAINT chk_workers_time CHECK (updated_time >= created_time)
) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_bin;
