use std::time::Duration;

use sqlx::MySqlPool;
use sqlx::mysql::MySqlPoolOptions;

const DEFAULT_URL: &str = "mysql://root:123456@127.0.0.1:3306/mysql";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const SCHEMA: &str = include_str!("../../sql/mysql/schema.sql");

pub(super) struct Server {
    admin_url: url::Url,
    admin: MySqlPool,
}

pub(super) struct Database {
    name: Option<String>,
    url: String,
    pool: Option<MySqlPool>,
    admin: MySqlPool,
}

impl Server {
    pub(super) async fn connect() -> Option<Self> {
        let (url, required) = match std::env::var("CRAWLER_MYSQL_URL") {
            Ok(url) => (url, true),
            Err(std::env::VarError::NotPresent) => (DEFAULT_URL.to_string(), false),
            Err(error) => panic!("invalid CRAWLER_MYSQL_URL: {error}"),
        };
        let admin_url = url::Url::parse(&url)
            .unwrap_or_else(|error| panic!("invalid CRAWLER_MYSQL_URL {url:?}: {error}"));
        if admin_url.scheme() != "mysql" {
            panic!("CRAWLER_MYSQL_URL must use the mysql scheme");
        }

        let connect = MySqlPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(CONNECT_TIMEOUT)
            .connect(admin_url.as_str());
        let admin = match tokio::time::timeout(CONNECT_TIMEOUT, connect).await {
            Ok(Ok(pool)) => pool,
            Ok(Err(error)) => return unavailable(&url, &error, required),
            Err(_) => return unavailable_timeout(&url, required),
        };
        let ping =
            tokio::time::timeout(CONNECT_TIMEOUT, sqlx::query("SELECT 1").execute(&admin)).await;
        match ping {
            Ok(Ok(_)) => Some(Self { admin_url, admin }),
            Ok(Err(error)) => unavailable(&url, &error, required),
            Err(_) => unavailable_timeout(&url, required),
        }
    }

    pub(super) async fn database(&self, label: &str) -> Database {
        self.create_database(label, true).await
    }

    #[allow(dead_code)]
    pub(super) async fn empty_database(&self, label: &str) -> Database {
        self.create_database(label, false).await
    }

    async fn create_database(&self, label: &str, install_schema: bool) -> Database {
        let name = database_name(label);
        sqlx::query(&format!(
            "CREATE DATABASE `{name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
        ))
        .execute(&self.admin)
        .await
        .unwrap_or_else(|error| panic!("failed to create MySQL test database {name}: {error}"));

        let mut database_url = self.admin_url.clone();
        database_url.set_path(&format!("/{name}"));
        let url = database_url.to_string();
        let pool = match MySqlPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(CONNECT_TIMEOUT)
            .connect(&url)
            .await
        {
            Ok(pool) => pool,
            Err(error) => {
                drop_database(&self.admin, &name).await;
                panic!("failed to connect to MySQL test database {name}: {error}");
            }
        };
        if install_schema && let Err(error) = sqlx::raw_sql(SCHEMA).execute(&pool).await {
            pool.close().await;
            drop_database(&self.admin, &name).await;
            panic!("failed to install MySQL Scheduler schema in {name}: {error}");
        }

        Database {
            name: Some(name),
            url,
            pool: Some(pool),
            admin: self.admin.clone(),
        }
    }
}

impl Database {
    pub(super) fn url(&self) -> &str {
        &self.url
    }

    #[allow(dead_code)]
    pub(super) fn pool(&self) -> &MySqlPool {
        self.pool
            .as_ref()
            .expect("removed MySQL test database has no connection pool")
    }

    pub(super) async fn remove(mut self) {
        let name = self
            .name
            .take()
            .expect("MySQL test database was already removed");
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }
        drop_database(&self.admin, &name).await;
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let Some(name) = self.name.take() else {
            return;
        };
        let pool = self.pool.take();
        let admin = self.admin.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(pool) = pool {
                    pool.close().await;
                }
                let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS `{name}`"))
                    .execute(&admin)
                    .await;
            });
        }
    }
}

fn database_name(label: &str) -> String {
    let mut label = label
        .chars()
        .filter_map(|value| {
            if value.is_ascii_alphanumeric() {
                Some(value.to_ascii_lowercase())
            } else if value == '-' || value == '_' {
                Some('_')
            } else {
                None
            }
        })
        .take(16)
        .collect::<String>();
    if label.is_empty() {
        label.push_str("schema");
    }
    format!("crawler_test_{label}_{}", uuid::Uuid::now_v7().simple())
}

async fn drop_database(admin: &MySqlPool, name: &str) {
    sqlx::query(&format!("DROP DATABASE IF EXISTS `{name}`"))
        .execute(admin)
        .await
        .unwrap_or_else(|error| panic!("failed to drop MySQL test database {name}: {error}"));
}

fn unavailable(url: &str, error: &sqlx::Error, required: bool) -> Option<Server> {
    if required {
        panic!("configured MySQL at {url} is unavailable: {error}");
    }
    eprintln!("skipping MySQL Scheduler integration test: MySQL at {url} is unavailable: {error}");
    None
}

fn unavailable_timeout(url: &str, required: bool) -> Option<Server> {
    if required {
        panic!("configured MySQL at {url} did not respond within {CONNECT_TIMEOUT:?}");
    }
    eprintln!(
        "skipping MySQL Scheduler integration test: MySQL at {url} did not respond within {CONNECT_TIMEOUT:?}"
    );
    None
}
