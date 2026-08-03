use std::collections::HashMap;

use spider::scheduler;
use sqlx::{MySqlPool, Row as _};

use super::decode;
use super::error::sqlx as sql_error;

const TABLES: &[&str] = &[
    "traces",
    "requests",
    "queues",
    "failed_workers",
    "completions",
    "trace_stats",
    "workers",
];

const IDENTITY_COLLATION: &str = "utf8mb4_0900_bin";

const STRICT_TEXT_COLUMNS: &[(&str, &[&str])] = &[
    ("traces", &["id", "task_id"]),
    (
        "requests",
        &[
            "id",
            "task_id",
            "trace_id",
            "node",
            "mode",
            "state",
            "leased_by",
        ],
    ),
    ("queues", &["request_id", "mode"]),
    ("failed_workers", &["request_id", "worker_id"]),
    (
        "completions",
        &[
            "request_id",
            "task_id",
            "trace_id",
            "node",
            "worker_id",
            "state",
        ],
    ),
    ("trace_stats", &["trace_id", "name"]),
    ("workers", &["worker_id", "host", "ip", "version", "token"]),
];

const COLUMNS: &[Column] = &[
    Column::required("traces", "id", "varchar(191)"),
    Column::required("traces", "task_id", "varchar(191)"),
    Column::required("traces", "snapshot", "json"),
    Column::required("traces", "created_time", "datetime(3)"),
    Column::required("traces", "updated_time", "datetime(3)"),
    Column::required("requests", "id", "varchar(191)"),
    Column::required("requests", "task_id", "varchar(191)"),
    Column::required("requests", "trace_id", "varchar(191)"),
    Column::required("requests", "node", "varchar(191)"),
    Column::required("requests", "mode", "varchar(16)"),
    Column::required("requests", "priority", "int"),
    Column::required("requests", "snapshot", "json"),
    Column::required("requests", "snapshot_hash", "binary(32)"),
    Column::required("requests", "state", "varchar(16)"),
    Column::required("requests", "version", "bigint"),
    Column::required("requests", "next_time", "bigint"),
    Column::required("requests", "leased_by", "varchar(191)"),
    Column::required("requests", "lease_time", "bigint"),
    Column::required("requests", "retry_count", "int"),
    Column::required("requests", "max_retry_count", "int"),
    Column::optional("requests", "ack_version", "bigint"),
    Column::required("requests", "created_time", "datetime(3)"),
    Column::required("requests", "updated_time", "datetime(3)"),
    Column::auto_increment("queues", "sequence", "bigint unsigned"),
    Column::required("queues", "request_id", "varchar(191)"),
    Column::required("queues", "mode", "varchar(16)"),
    Column::required("queues", "priority", "int"),
    Column::required("queues", "next_time", "bigint"),
    Column::required("queues", "created_time", "datetime(3)"),
    Column::required("queues", "updated_time", "datetime(3)"),
    Column::required("failed_workers", "request_id", "varchar(191)"),
    Column::required("failed_workers", "worker_id", "varchar(191)"),
    Column::required("failed_workers", "position", "int"),
    Column::required("failed_workers", "created_time", "datetime(3)"),
    Column::required("failed_workers", "updated_time", "datetime(3)"),
    Column::required("completions", "request_id", "varchar(191)"),
    Column::required("completions", "version", "bigint"),
    Column::required("completions", "task_id", "varchar(191)"),
    Column::required("completions", "trace_id", "varchar(191)"),
    Column::required("completions", "node", "varchar(191)"),
    Column::required("completions", "worker_id", "varchar(191)"),
    Column::required("completions", "state", "varchar(16)"),
    Column::optional("completions", "error", "longtext"),
    Column::optional("completions", "start_time", "bigint"),
    Column::optional("completions", "end_time", "bigint"),
    Column::required("completions", "created_time", "datetime(3)"),
    Column::required("completions", "updated_time", "datetime(3)"),
    Column::required("trace_stats", "trace_id", "varchar(191)"),
    Column::required("trace_stats", "name", "varchar(191)"),
    Column::required("trace_stats", "total", "bigint"),
    Column::required("trace_stats", "done", "bigint"),
    Column::required("trace_stats", "filter", "bigint"),
    Column::required("trace_stats", "dedup", "bigint"),
    Column::required("trace_stats", "validate", "bigint"),
    Column::required("trace_stats", "download", "bigint"),
    Column::required("trace_stats", "created_time", "datetime(3)"),
    Column::required("trace_stats", "updated_time", "datetime(3)"),
    Column::required("workers", "worker_id", "varchar(191)"),
    Column::required("workers", "host", "varchar(191)"),
    Column::optional("workers", "ip", "varchar(45)"),
    Column::required("workers", "version", "varchar(191)"),
    Column::required("workers", "modes", "json"),
    Column::required("workers", "concurrency", "int unsigned"),
    Column::required("workers", "heartbeat_timeout", "bigint"),
    Column::required("workers", "last_heartbeat", "bigint"),
    Column::required("workers", "token", "varchar(191)"),
    Column::optional("workers", "offline_time", "bigint"),
    Column::required("workers", "created_time", "datetime(3)"),
    Column::required("workers", "updated_time", "datetime(3)"),
];

const UNIQUE_KEYS: &[(&str, &str, &[&str])] = &[
    ("traces", "PRIMARY", &["id"]),
    ("requests", "PRIMARY", &["id"]),
    ("queues", "PRIMARY", &["sequence"]),
    ("queues", "uq_queues_request", &["request_id"]),
    ("failed_workers", "PRIMARY", &["request_id", "worker_id"]),
    (
        "failed_workers",
        "uq_failed_workers_position",
        &["request_id", "position"],
    ),
    ("completions", "PRIMARY", &["request_id", "version"]),
    ("trace_stats", "PRIMARY", &["trace_id", "name"]),
    ("workers", "PRIMARY", &["worker_id"]),
];

struct TableDefinition {
    engine: String,
    collation: String,
}

struct Column {
    table: &'static str,
    name: &'static str,
    kind: &'static str,
    nullable: &'static str,
    extra: &'static str,
}

impl Column {
    const fn required(table: &'static str, name: &'static str, kind: &'static str) -> Self {
        Self {
            table,
            name,
            kind,
            nullable: "NO",
            extra: "",
        }
    }

    const fn optional(table: &'static str, name: &'static str, kind: &'static str) -> Self {
        Self {
            table,
            name,
            kind,
            nullable: "YES",
            extra: "",
        }
    }

    const fn auto_increment(table: &'static str, name: &'static str, kind: &'static str) -> Self {
        Self {
            table,
            name,
            kind,
            nullable: "NO",
            extra: "auto_increment",
        }
    }
}

struct ColumnDefinition {
    kind: String,
    nullable: String,
    extra: String,
    collation: Option<String>,
}

type ColumnDefinitions = HashMap<(String, String), ColumnDefinition>;

pub(super) async fn validate(pool: &MySqlPool, database: &str) -> Result<(), scheduler::Error> {
    let tables = sqlx::query(
        "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = DATABASE()",
    )
    .fetch_all(pool)
    .await
    .map_err(sql_error)?;
    let mut definitions = HashMap::with_capacity(tables.len());
    for row in tables {
        definitions.insert(
            decode::string(&row, "TABLE_NAME")?,
            TableDefinition {
                engine: decode::string(&row, "ENGINE")?,
                collation: decode::string(&row, "TABLE_COLLATION")?,
            },
        );
    }
    let missing = TABLES
        .iter()
        .copied()
        .filter(|table| !definitions.contains_key(*table))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(scheduler::Error::Message(format!(
            "MySQL Scheduler database {database} is missing required tables: {}",
            missing.join(", ")
        )));
    }
    if let Some(table) = TABLES
        .iter()
        .copied()
        .find(|table| !definitions[*table].engine.eq_ignore_ascii_case("InnoDB"))
    {
        return Err(scheduler::Error::Message(format!(
            "MySQL Scheduler table {table} must use InnoDB"
        )));
    }
    if let Some(table) = TABLES.iter().copied().find(|table| {
        !definitions[*table]
            .collation
            .eq_ignore_ascii_case(IDENTITY_COLLATION)
    }) {
        return Err(scheduler::Error::Message(format!(
            "MySQL Scheduler table {table} must use {IDENTITY_COLLATION}"
        )));
    }

    let rows = sqlx::query(
        "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, EXTRA, COLLATION_NAME \
         FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE()",
    )
    .fetch_all(pool)
    .await
    .map_err(sql_error)?;
    let mut definitions = HashMap::with_capacity(rows.len());
    for row in rows {
        let table = decode::string(&row, "TABLE_NAME")?;
        let column = decode::string(&row, "COLUMN_NAME")?;
        let column_type = decode::string(&row, "COLUMN_TYPE")?;
        let nullable = decode::string(&row, "IS_NULLABLE")?;
        let extra = decode::string(&row, "EXTRA")?;
        let collation = row
            .try_get::<Option<Vec<u8>>, _>("COLLATION_NAME")
            .map_err(sql_error)?
            .map(String::from_utf8)
            .transpose()
            .map_err(super::error::message)?;
        definitions.insert(
            (table, column),
            ColumnDefinition {
                kind: column_type,
                nullable,
                extra,
                collation,
            },
        );
    }
    let missing = COLUMNS
        .iter()
        .filter(|column| {
            !definitions.contains_key(&(column.table.to_string(), column.name.to_string()))
        })
        .map(|column| format!("{}.{}", column.table, column.name))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(scheduler::Error::Message(format!(
            "MySQL Scheduler database {database} is missing required columns: {}",
            missing.join(", ")
        )));
    }

    for column in COLUMNS {
        require_definition(&definitions, column)?;
    }
    for (table, columns) in STRICT_TEXT_COLUMNS {
        for column in *columns {
            require_collation(&definitions, table, column)?;
        }
    }
    validate_unique_keys(pool).await?;
    Ok(())
}

async fn validate_unique_keys(pool: &MySqlPool) -> Result<(), scheduler::Error> {
    let rows = sqlx::query(
        "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART \
         FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = DATABASE() \
         ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
    )
    .fetch_all(pool)
    .await
    .map_err(sql_error)?;
    let mut indexes = HashMap::<(String, String), (i32, Vec<(String, Option<i64>)>)>::new();
    for row in rows {
        let table = decode::string(&row, "TABLE_NAME")?;
        let index = decode::string(&row, "INDEX_NAME")?;
        let non_unique = row.try_get::<i32, _>("NON_UNIQUE").map_err(sql_error)?;
        let column = decode::string(&row, "COLUMN_NAME")?;
        let prefix = row
            .try_get::<Option<i64>, _>("SUB_PART")
            .map_err(sql_error)?;
        let entry = indexes
            .entry((table, index))
            .or_insert((non_unique, Vec::new()));
        if entry.0 != non_unique {
            return Err(scheduler::Error::Message(
                "MySQL Scheduler index uniqueness metadata is inconsistent".to_string(),
            ));
        }
        entry.1.push((column, prefix));
    }
    for (table, index, columns) in UNIQUE_KEYS {
        let Some((non_unique, actual)) = indexes.get(&(table.to_string(), index.to_string()))
        else {
            return Err(scheduler::Error::Message(format!(
                "MySQL Scheduler table {table} is missing required unique key {index}"
            )));
        };
        if *non_unique != 0
            || actual.iter().any(|(_, prefix)| prefix.is_some())
            || actual
                .iter()
                .map(|(column, _)| column.as_str())
                .ne(columns.iter().copied())
        {
            return Err(scheduler::Error::Message(format!(
                "MySQL Scheduler unique key {table}.{index} has incompatible definition"
            )));
        }
    }
    Ok(())
}

fn require_definition(
    definitions: &ColumnDefinitions,
    column: &Column,
) -> Result<(), scheduler::Error> {
    let actual = definitions
        .get(&(column.table.to_string(), column.name.to_string()))
        .expect("required MySQL column was checked above");
    if actual.kind.eq_ignore_ascii_case(column.kind)
        && actual.nullable.eq_ignore_ascii_case(column.nullable)
        && actual.extra.eq_ignore_ascii_case(column.extra)
    {
        Ok(())
    } else {
        Err(scheduler::Error::Message(format!(
            "MySQL Scheduler column {}.{} has incompatible definition",
            column.table, column.name
        )))
    }
}

fn require_collation(
    definitions: &ColumnDefinitions,
    table: &str,
    column: &str,
) -> Result<(), scheduler::Error> {
    let actual = definitions
        .get(&(table.to_string(), column.to_string()))
        .expect("required MySQL column was checked above")
        .collation
        .as_deref();
    if actual.is_some_and(|value| value.eq_ignore_ascii_case(IDENTITY_COLLATION)) {
        Ok(())
    } else {
        Err(scheduler::Error::Message(format!(
            "MySQL Scheduler column {table}.{column} must use {IDENTITY_COLLATION}"
        )))
    }
}
