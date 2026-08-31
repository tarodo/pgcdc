use crate::error::PgcdcError;
use crate::lsn::Lsn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotInfo {
    pub restart_lsn: Option<Lsn>,
    pub confirmed_flush_lsn: Option<Lsn>,
}

/// The slot guard: called on EVERY replication session (`stream_once` in
/// `replication.rs`) — both on a cold start and on every reconnect, not
/// just once at first launch. Only the slot's existence is checked here;
/// on a cold start there's nothing to compare `confirmed_flush_lsn` against —
/// we have no persistent durable position and never will (DECISIONS
/// Q4) — while on reconnect the positions returned from here are checked by the
/// caller via `check_reconnect`. If the slot is missing — we fail, we do NOT create it:
/// auto-creation masks data loss, and that is measured in
/// docs/spike-findings.md §2.4.
pub async fn preflight_slot(conn_str: &str, slot: &str) -> Result<SlotInfo, PgcdcError> {
    let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls)
        .await
        .map_err(|e| PgcdcError::Connection(format!("preflight connect: {e}")))?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    let rows = client
        .query(
            "SELECT restart_lsn::text, confirmed_flush_lsn::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| PgcdcError::Connection(format!("preflight query: {e}")))?;

    handle.abort();

    let row = rows.first().ok_or_else(|| PgcdcError::SlotMissing {
        slot: slot.to_owned(),
    })?;
    Ok(SlotInfo {
        restart_lsn: row
            .get::<_, Option<String>>(0)
            .as_deref()
            .and_then(parse_lsn),
        confirmed_flush_lsn: row
            .get::<_, Option<String>>(1)
            .as_deref()
            .and_then(parse_lsn),
    })
}

/// Reconnect within a running process, where the durable position is in memory.
/// Returns `Ok(Some(text))` if the discrepancy is worth logging as a WARN,
/// and `Err` only if the slot has moved AHEAD of our durable point.
pub fn check_reconnect(
    slot: &str,
    info: &SlotInfo,
    durable: Lsn,
) -> Result<Option<String>, PgcdcError> {
    let confirmed = info.confirmed_flush_lsn.unwrap_or(Lsn(0));
    if confirmed > durable {
        return Err(PgcdcError::SlotAhead {
            slot: slot.to_owned(),
            slot_lsn: confirmed.to_string(),
            durable: durable.to_string(),
        });
    }
    if confirmed < durable {
        return Ok(Some(format!(
            "slot {slot} is behind our durable position: slot={confirmed}, durable={durable}; \
             the gap will be replayed as duplicates"
        )));
    }
    Ok(None)
}

/// PostgreSQL prints an LSN as `X/Y` in hexadecimal.
fn parse_lsn(text: &str) -> Option<Lsn> {
    let (hi, lo) = text.split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some(Lsn((hi << 32) | lo))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(confirmed: u64) -> SlotInfo {
        SlotInfo {
            restart_lsn: Some(Lsn(confirmed - 0x100)),
            confirmed_flush_lsn: Some(Lsn(confirmed)),
        }
    }

    #[test]
    fn slot_ahead_of_our_durable_position_is_fatal() {
        // Someone acknowledged WAL that we never committed to the sink.
        let err = check_reconnect("s", &info(0x2000), Lsn(0x1000)).unwrap_err();
        assert!(matches!(err, PgcdcError::SlotAhead { .. }));
    }

    #[test]
    fn slot_behind_is_a_warning_not_a_failure() {
        // The expected outcome of a drop: the last send_feedback might not have arrived.
        // START_REPLICATION with 0/0 will replay the gap as duplicates,
        // which the "duplicates are allowed" invariant explicitly permits.
        // Failing here would mean failing on every network hiccup.
        let warn = check_reconnect("s", &info(0x1000), Lsn(0x2000)).unwrap();
        assert!(warn.is_some(), "the discrepancy must be noticed");
        let text = warn.unwrap();
        assert!(
            text.contains("1000") && text.contains("2000"),
            "both positions are in the message"
        );
    }

    #[test]
    fn exact_match_is_silent() {
        assert!(check_reconnect("s", &info(0x1000), Lsn(0x1000))
            .unwrap()
            .is_none());
    }

    #[test]
    fn missing_confirmed_flush_is_treated_as_zero() {
        let empty = SlotInfo {
            restart_lsn: None,
            confirmed_flush_lsn: None,
        };
        // The slot exists but has never been acknowledged — it is behind any position of ours.
        assert!(check_reconnect("s", &empty, Lsn(0x1000)).unwrap().is_some());
    }

    #[test]
    fn parses_postgres_lsn_text() {
        assert_eq!(parse_lsn("0/19300D0"), Some(Lsn(0x0193_00D0)));
        assert_eq!(parse_lsn("1/FF"), Some(Lsn(0x0000_0001_0000_00FF)));
        assert_eq!(parse_lsn("garbage"), None);
    }
}
