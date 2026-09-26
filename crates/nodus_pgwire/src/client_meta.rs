//! Per-connection metadata stored on the pgwire `ClientInfo`: session and
//! principal ids, transaction status tracking, statement-timeout bookkeeping,
//! and command-tag helpers.

use crate::{
    METADATA_NODUS_PRINCIPAL_ID, METADATA_NODUS_SESSION_ID, METADATA_SESSION_STARTED,
    METADATA_TX_STATUS,
};
use nodus_catalog::PrincipalId;
use pgwire::api::ClientInfo;
use pgwire::api::results::Tag;
use pgwire::messages::response::TransactionStatus;
use uuid::Uuid;

pub(crate) fn session_id_from_client<C: ClientInfo>(client: &C) -> String {
    client
        .metadata()
        .get(METADATA_NODUS_SESSION_ID)
        .cloned()
        .unwrap_or_default()
}

pub(crate) fn principal_id_from_client<C: ClientInfo>(client: &C) -> PrincipalId {
    client
        .metadata()
        .get(METADATA_NODUS_PRINCIPAL_ID)
        .and_then(|s| Uuid::parse_str(s).ok())
        .map(PrincipalId)
        .unwrap_or_default()
}

pub(crate) fn tx_status_from_client<C: ClientInfo>(client: &C) -> TransactionStatus {
    // pgwire also updates this state on protocol/parse errors, so use its
    // authoritative value rather than a metadata copy that could become stale.
    client.transaction_status()
}

pub(crate) fn set_tx_status<C: ClientInfo>(client: &mut C, status: TransactionStatus) {
    client.set_transaction_status(status);
    let encoded = match status {
        TransactionStatus::Idle => "I",
        TransactionStatus::Transaction => "T",
        TransactionStatus::Error => "E",
    };
    client
        .metadata_mut()
        .insert(METADATA_TX_STATUS.to_owned(), encoded.to_owned());
}

pub(crate) fn mark_error_status<C: ClientInfo>(client: &mut C) {
    if tx_status_from_client(client) == TransactionStatus::Transaction {
        set_tx_status(client, TransactionStatus::Error);
    }
}

/// Records a statement that failed during execution. A failed COMMIT or ROLLBACK
/// still ends the transaction (the executor has already discarded it), so the
/// session returns to idle as in PostgreSQL; any other failure inside a
/// transaction block aborts the block.
pub(crate) fn mark_execution_failed<C: ClientInfo>(
    client: &mut C,
    plan: &nodus_executor::LogicalPlan,
) {
    use nodus_executor::LogicalPlan;
    if matches!(plan, LogicalPlan::Commit | LogicalPlan::Rollback) {
        set_tx_status(client, TransactionStatus::Idle);
    } else {
        mark_error_status(client);
    }
}

/// Hands the executor the settings the client asked for when it connected
/// (`application_name`, `TimeZone`), once per session.
pub(crate) fn ensure_session_started<C: ClientInfo>(
    client: &mut C,
    executor: &dyn nodus_executor::Executor,
) {
    if client.metadata().contains_key(METADATA_SESSION_STARTED) {
        return;
    }
    let settings: Vec<(String, String)> = client
        .metadata()
        .iter()
        .filter(|(name, _)| {
            !name.starts_with("nodus_")
                && !matches!(
                    name.as_str(),
                    "user" | "database" | "options" | "replication"
                )
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    executor.start_session(&session_id_from_client(client), &settings);
    client
        .metadata_mut()
        .insert(METADATA_SESSION_STARTED.to_owned(), "1".to_owned());
}

pub(crate) fn described_statement_key(statement: &str) -> String {
    format!("nodus_described_statement:{statement}")
}

pub(crate) fn described_portal_key(portal_name: &str) -> String {
    format!("nodus_described_portal:{portal_name}")
}

pub(crate) fn command_tag_from_output_tag(output_tag: &str) -> Tag {
    if let Some(rest) = output_tag.strip_prefix("INSERT 0 ") {
        let rows = rest.parse::<usize>().unwrap_or(0);
        Tag::new("INSERT 0").with_rows(rows)
    } else if let Some(rest) = output_tag.strip_prefix("UPDATE ") {
        let rows = rest.parse::<usize>().unwrap_or(0);
        Tag::new("UPDATE").with_rows(rows)
    } else if let Some(rest) = output_tag.strip_prefix("DELETE ") {
        let rows = rest.parse::<usize>().unwrap_or(0);
        Tag::new("DELETE").with_rows(rows)
    } else if let Some(rest) = output_tag.strip_prefix("MERGE ") {
        let rows = rest.parse::<usize>().unwrap_or(0);
        Tag::new("MERGE").with_rows(rows)
    } else {
        Tag::new(output_tag)
    }
}

/// Reject statements in a failed transaction, and make COMMIT abort its writes.
pub(crate) fn transaction_plan<C: ClientInfo>(
    client: &C,
    plan: nodus_executor::LogicalPlan,
) -> pgwire::error::PgWireResult<nodus_executor::LogicalPlan> {
    use nodus_executor::LogicalPlan;
    if tx_status_from_client(client) != TransactionStatus::Error {
        return Ok(plan);
    }
    match plan {
        LogicalPlan::Commit => Ok(LogicalPlan::Rollback),
        LogicalPlan::Rollback | LogicalPlan::RollbackToSavepoint { .. } => Ok(plan),
        _ => Err(crate::wire_format::user_error(
            "ERROR",
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        )),
    }
}

pub(crate) fn apply_plan_tx_status<C: ClientInfo>(
    client: &mut C,
    plan: &nodus_executor::LogicalPlan,
) {
    use nodus_executor::LogicalPlan;
    match plan {
        LogicalPlan::Begin { .. } | LogicalPlan::RollbackToSavepoint { .. } => {
            set_tx_status(client, TransactionStatus::Transaction)
        }
        LogicalPlan::Commit | LogicalPlan::Rollback => {
            set_tx_status(client, TransactionStatus::Idle)
        }
        _ => {}
    }
}

/// COPY input must not enter its subprotocol while the transaction is failed.
pub(crate) fn ensure_transaction_usable<C: ClientInfo>(
    client: &C,
) -> pgwire::error::PgWireResult<()> {
    if tx_status_from_client(client) == TransactionStatus::Error {
        return Err(crate::wire_format::user_error(
            "ERROR",
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        ));
    }
    Ok(())
}
