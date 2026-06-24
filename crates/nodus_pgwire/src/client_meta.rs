//! Per-connection metadata stored on the pgwire `ClientInfo`: session and
//! principal ids, transaction status tracking, statement-timeout bookkeeping,
//! and command-tag helpers.

use crate::{
    METADATA_NODUS_PRINCIPAL_ID, METADATA_NODUS_SESSION_ID, METADATA_STATEMENT_TIMEOUT_MS,
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
    match client
        .metadata()
        .get(METADATA_TX_STATUS)
        .map(String::as_str)
    {
        Some("T") => TransactionStatus::Transaction,
        Some("E") => TransactionStatus::Error,
        _ => TransactionStatus::Idle,
    }
}

pub(crate) fn set_tx_status<C: ClientInfo>(client: &mut C, status: TransactionStatus) {
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

pub(crate) fn apply_command_tag_to_tx_status<C: ClientInfo>(client: &mut C, tag: &str) {
    let command = tag.split_whitespace().next().unwrap_or(tag);
    if command.eq_ignore_ascii_case("BEGIN") {
        set_tx_status(client, TransactionStatus::Transaction);
    } else if command.eq_ignore_ascii_case("COMMIT") || tag.trim().eq_ignore_ascii_case("ROLLBACK")
    {
        set_tx_status(client, TransactionStatus::Idle);
    }
}

pub(crate) fn parse_statement_timeout_ms(query: &str) -> Option<u64> {
    let normalized = query
        .trim()
        .trim_end_matches(';')
        .replace('=', " = ")
        .replace(',', " ");
    let parts = normalized.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 3
        || !parts[0].eq_ignore_ascii_case("SET")
        || !parts[1].eq_ignore_ascii_case("statement_timeout")
    {
        return None;
    }
    parts
        .iter()
        .skip(2)
        .find_map(|part| part.trim_matches('\'').parse::<u64>().ok())
}

pub(crate) fn remember_statement_timeout<C: ClientInfo>(client: &mut C, query: &str) {
    if let Some(timeout_ms) = parse_statement_timeout_ms(query) {
        client.metadata_mut().insert(
            METADATA_STATEMENT_TIMEOUT_MS.to_owned(),
            timeout_ms.to_string(),
        );
    }
}

pub(crate) fn statement_timeout_ms<C: ClientInfo>(client: &C) -> Option<u64> {
    client
        .metadata()
        .get(METADATA_STATEMENT_TIMEOUT_MS)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

pub(crate) fn pg_sleep_ms(query: &str) -> Option<u64> {
    let lower = query.to_ascii_lowercase();
    let start = lower.find("pg_sleep(")? + "pg_sleep(".len();
    let rest = &lower[start..];
    let end = rest.find(')')?;
    let seconds = rest[..end].trim().parse::<f64>().ok()?;
    Some((seconds * 1000.0).ceil() as u64)
}

pub(crate) fn statement_would_timeout<C: ClientInfo>(client: &C, query: &str) -> bool {
    match (statement_timeout_ms(client), pg_sleep_ms(query)) {
        (Some(timeout_ms), Some(sleep_ms)) => sleep_ms >= timeout_ms,
        _ => false,
    }
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
    } else {
        Tag::new(output_tag)
    }
}
