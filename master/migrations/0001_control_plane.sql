CREATE TABLE IF NOT EXISTS tasks (
    namespace VARCHAR(128) NOT NULL,
    id VARCHAR(191) NOT NULL,
    name VARCHAR(191) NOT NULL,
    config_version BIGINT NOT NULL DEFAULT 0,
    state TINYINT NOT NULL DEFAULT 1,
    run_mode TINYINT NOT NULL DEFAULT 0,
    interval_ms BIGINT NOT NULL DEFAULT 0,
    priority INT NOT NULL DEFAULT 0,
    params JSON NOT NULL,
    dsl JSON NULL,
    seed_specs JSON NULL,
    persister_id VARCHAR(191) NULL,
    attachment JSON NULL,
    next_time BIGINT NOT NULL DEFAULT 0,
    created_time BIGINT NOT NULL,
    updated_time BIGINT NOT NULL,
    PRIMARY KEY (namespace, id),
    UNIQUE KEY tasks_namespace_name (namespace, name),
    KEY tasks_due (namespace, state, next_time, priority, id)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS traces (
    namespace VARCHAR(128) NOT NULL,
    id VARCHAR(191) NOT NULL,
    task_id VARCHAR(191) NOT NULL,
    snapshot JSON NOT NULL,
    start_time BIGINT NULL,
    created_time BIGINT NOT NULL,
    updated_time BIGINT NOT NULL,
    PRIMARY KEY (namespace, id),
    KEY traces_task (namespace, task_id, created_time)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS requests (
    namespace VARCHAR(128) NOT NULL,
    id VARCHAR(191) NOT NULL,
    task_id VARCHAR(191) NOT NULL,
    trace_id VARCHAR(191) NOT NULL,
    node VARCHAR(191) NOT NULL,
    mode VARCHAR(32) NOT NULL,
    state TINYINT NOT NULL,
    version BIGINT NOT NULL,
    priority INT NOT NULL,
    snapshot JSON NOT NULL,
    snapshot_digest CHAR(64) NOT NULL,
    next_time BIGINT NOT NULL,
    leased_by VARCHAR(191) NOT NULL DEFAULT '',
    lease_time BIGINT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    max_retry_count INT NOT NULL,
    failed_workers JSON NOT NULL,
    ack_version BIGINT NULL,
    created_time BIGINT NOT NULL,
    updated_time BIGINT NOT NULL,
    PRIMARY KEY (namespace, id),
    KEY requests_claim (namespace, state, mode, next_time, priority, created_time),
    KEY requests_recovery (namespace, state, lease_time),
    KEY requests_worker (namespace, state, leased_by)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS request_completions (
    namespace VARCHAR(128) NOT NULL,
    request_id VARCHAR(191) NOT NULL,
    version BIGINT NOT NULL,
    task_id VARCHAR(191) NOT NULL,
    trace_id VARCHAR(191) NOT NULL,
    node VARCHAR(191) NOT NULL,
    worker_id VARCHAR(191) NOT NULL,
    state TINYINT NOT NULL,
    error TEXT NULL,
    payload_digest CHAR(64) NOT NULL,
    created_time BIGINT NOT NULL,
    PRIMARY KEY (namespace, request_id, version)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS operations (
    namespace VARCHAR(128) NOT NULL,
    kind VARCHAR(64) NOT NULL,
    operation_key VARCHAR(191) NOT NULL,
    request_digest CHAR(64) NOT NULL,
    result JSON NOT NULL,
    created_time BIGINT NOT NULL,
    updated_time BIGINT NOT NULL,
    PRIMARY KEY (namespace, kind, operation_key)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS workers (
    namespace VARCHAR(128) NOT NULL,
    id VARCHAR(191) NOT NULL,
    modes JSON NOT NULL,
    last_heartbeat BIGINT NOT NULL,
    created_time BIGINT NOT NULL,
    updated_time BIGINT NOT NULL,
    PRIMARY KEY (namespace, id),
    KEY workers_heartbeat (namespace, last_heartbeat)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS items (
    namespace VARCHAR(128) NOT NULL,
    id CHAR(36) NOT NULL,
    item_id VARCHAR(191) NOT NULL,
    task_id VARCHAR(191) NOT NULL,
    trace_id VARCHAR(191) NOT NULL,
    request_id VARCHAR(191) NOT NULL,
    persister_id VARCHAR(191) NULL,
    config_version VARCHAR(191) NULL,
    timezone VARCHAR(128) NULL,
    data JSON NOT NULL,
    created_time BIGINT NOT NULL,
    updated_time BIGINT NOT NULL,
    PRIMARY KEY (namespace, id),
    KEY items_trace (namespace, trace_id, created_time),
    KEY items_request (namespace, request_id, created_time)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS trace_stats (
    namespace VARCHAR(128) NOT NULL,
    trace_id VARCHAR(191) NOT NULL,
    name VARCHAR(191) NOT NULL,
    total BIGINT NOT NULL DEFAULT 0,
    done BIGINT NOT NULL DEFAULT 0,
    filter_count BIGINT NOT NULL DEFAULT 0,
    dedup BIGINT NOT NULL DEFAULT 0,
    validate_count BIGINT NOT NULL DEFAULT 0,
    download BIGINT NOT NULL DEFAULT 0,
    created_time BIGINT NOT NULL,
    updated_time BIGINT NOT NULL,
    PRIMARY KEY (namespace, trace_id, name)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS events (
    namespace VARCHAR(128) NOT NULL,
    id CHAR(36) NOT NULL,
    trace_id VARCHAR(191) NULL,
    task_id VARCHAR(191) NULL,
    request_id VARCHAR(191) NULL,
    worker_id VARCHAR(191) NULL,
    kind TINYINT NOT NULL,
    data JSON NOT NULL,
    created_time BIGINT NOT NULL,
    PRIMARY KEY (namespace, id),
    KEY events_request (namespace, request_id, created_time),
    KEY events_trace (namespace, trace_id, created_time)
) ENGINE = InnoDB;
