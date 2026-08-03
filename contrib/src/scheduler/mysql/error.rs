use spider::scheduler;
use sqlx::mysql::MySqlDatabaseError;

pub(super) fn sqlx(error: sqlx::Error) -> scheduler::Error {
    if transient(&error) {
        scheduler::Error::Unavailable(format!("MySQL Scheduler operation failed: {error}"))
    } else {
        scheduler::Error::Message(format!("MySQL Scheduler operation failed: {error}"))
    }
}

pub(super) fn message(error: impl std::fmt::Display) -> scheduler::Error {
    scheduler::Error::Message(error.to_string())
}

fn transient(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(_) => database_number(error).is_some_and(transient_number),
        _ => false,
    }
}

fn transient_number(number: u16) -> bool {
    matches!(
        number,
        // Too many connections, lock wait timeout, deadlock, server shutdown,
        // connection loss, or server unavailable.
        1040 | 1053 | 1205 | 1213 | 2002 | 2003 | 2006 | 2013
    )
}

pub(super) fn database_number(error: &sqlx::Error) -> Option<u16> {
    match error {
        sqlx::Error::Database(error) => error
            .try_downcast_ref::<MySqlDatabaseError>()
            .map(MySqlDatabaseError::number),
        _ => None,
    }
}
