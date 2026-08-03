#[path = "mysql/server.rs"]
mod server;

use spider::Scheduler as _;
use sqlx::Row;

#[tokio::test]
async fn installs_the_scheduler_schema_in_an_isolated_database() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("schema").await;

    let mut tables = sqlx::query("SHOW TABLES")
        .fetch_all(database.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            String::from_utf8(row.try_get::<Vec<u8>, _>(0).unwrap())
                .expect("MySQL table names must be UTF-8")
        })
        .collect::<Vec<_>>();
    tables.sort_unstable();
    assert_eq!(
        tables,
        [
            "completions",
            "failed_workers",
            "queues",
            "requests",
            "trace_stats",
            "traces",
            "workers",
        ]
    );

    let time_columns = sqlx::query(
        "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE \
         FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = DATABASE() \
           AND COLUMN_NAME IN ('created_time', 'updated_time')",
    )
    .fetch_all(database.pool())
    .await
    .unwrap();
    assert_eq!(time_columns.len(), tables.len() * 2);
    assert!(
        time_columns
            .iter()
            .all(|row| row.try_get::<Vec<u8>, _>("COLUMN_TYPE").unwrap() == b"datetime(3)")
    );

    assert!(database.url().contains("crawler_test_schema_"));
    database.remove().await;
}

#[tokio::test]
async fn scheduler_open_never_creates_operator_owned_tables() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.empty_database("no-ddl").await;
    let scheduler = configured(database.url());
    let error = scheduler.open(4).await.unwrap_err();
    assert!(error.to_string().contains("missing required tables"));
    assert!(
        sqlx::query("SHOW TABLES")
            .fetch_all(database.pool())
            .await
            .unwrap()
            .is_empty()
    );
    database.remove().await;
}

#[tokio::test]
async fn scheduler_open_rejects_an_incomplete_schema() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("invalid-schema").await;
    sqlx::query("ALTER TABLE requests DROP COLUMN snapshot_hash")
        .execute(database.pool())
        .await
        .unwrap();

    let scheduler = configured(database.url());
    let error = scheduler.open(4).await.unwrap_err();
    assert!(error.to_string().contains("requests.snapshot_hash"));
    database.remove().await;
}

#[tokio::test]
async fn scheduler_open_rejects_missing_atomicity_keys() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("invalid-key").await;
    sqlx::query("ALTER TABLE queues DROP INDEX uq_queues_request")
        .execute(database.pool())
        .await
        .unwrap();

    let scheduler = configured(database.url());
    let error = scheduler.open(4).await.unwrap_err();
    assert!(
        error.to_string().contains("uq_queues_request"),
        "unexpected schema validation error: {error}"
    );
    database.remove().await;
}

#[tokio::test]
async fn scheduler_open_rejects_incompatible_queue_types() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("invalid-queue-type").await;
    sqlx::query("ALTER TABLE queues MODIFY priority VARCHAR(32) NOT NULL")
        .execute(database.pool())
        .await
        .unwrap();

    let scheduler = configured(database.url());
    let error = scheduler.open(4).await.unwrap_err();
    assert!(
        error.to_string().contains("queues.priority"),
        "unexpected schema validation error: {error}"
    );
    database.remove().await;
}

#[tokio::test]
async fn scheduler_open_rejects_narrow_stat_counters() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("invalid-stat-type").await;
    sqlx::query("ALTER TABLE trace_stats MODIFY total INT NOT NULL DEFAULT 0")
        .execute(database.pool())
        .await
        .unwrap();

    let scheduler = configured(database.url());
    let error = scheduler.open(4).await.unwrap_err();
    assert!(
        error.to_string().contains("trace_stats.total"),
        "unexpected schema validation error: {error}"
    );
    database.remove().await;
}

#[tokio::test]
async fn scheduler_open_rejects_incompatible_nullability() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("invalid-nullability").await;
    sqlx::query("ALTER TABLE workers MODIFY ip VARCHAR(45) NOT NULL")
        .execute(database.pool())
        .await
        .unwrap();

    let scheduler = configured(database.url());
    let error = scheduler.open(4).await.unwrap_err();
    assert!(
        error.to_string().contains("workers.ip"),
        "unexpected schema validation error: {error}"
    );
    database.remove().await;
}

#[tokio::test]
async fn scheduler_open_rejects_prefix_unique_keys() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("invalid-prefix-key").await;
    sqlx::query(
        "ALTER TABLE queues DROP INDEX uq_queues_request, \
         ADD UNIQUE KEY uq_queues_request (request_id(1))",
    )
    .execute(database.pool())
    .await
    .unwrap();

    let scheduler = configured(database.url());
    let error = scheduler.open(4).await.unwrap_err();
    assert!(
        error.to_string().contains("uq_queues_request"),
        "unexpected schema validation error: {error}"
    );
    database.remove().await;
}

#[tokio::test]
async fn scheduler_open_rejects_padding_identity_collations() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("invalid-collation").await;
    sqlx::query(
        "ALTER TABLE requests MODIFY id \
         VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL",
    )
    .execute(database.pool())
    .await
    .unwrap();

    let scheduler = configured(database.url());
    let error = scheduler.open(4).await.unwrap_err();
    assert!(
        error.to_string().contains("requests.id") && error.to_string().contains("utf8mb4_0900_bin"),
        "unexpected schema validation error: {error}"
    );
    database.remove().await;
}

fn configured(url: &str) -> contrib::scheduler::mysql::MySql {
    contrib::scheduler::mysql::MySql::new(url)
        .unwrap()
        .with_worker_id("schema-worker")
        .unwrap()
        .with_worker_host("schema-host")
        .unwrap()
        .with_worker_version("test")
        .unwrap()
}
