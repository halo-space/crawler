use spider::scheduler;
use sqlx::Row as _;
use sqlx::mysql::MySqlRow;

use super::error::sqlx as sql_error;

pub(super) fn string(row: &MySqlRow, column: &str) -> Result<String, scheduler::Error> {
    let bytes = row.try_get::<Vec<u8>, _>(column).map_err(sql_error)?;
    String::from_utf8(bytes).map_err(super::error::message)
}
