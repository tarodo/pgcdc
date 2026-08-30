use crate::error::PgcdcError;
use crate::lsn::Lsn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotInfo {
    pub restart_lsn: Option<Lsn>,
    pub confirmed_flush_lsn: Option<Lsn>,
}

/// Холодный старт. Проверка только существования: сравнивать
/// `confirmed_flush_lsn` не с чем, персистентной durable-позиции у нас нет и
/// не будет (DECISIONS Q4). Слот отсутствует — падаем, НЕ создаём: автосоздание
/// маскирует потерю данных, и это измерено в docs/spike-findings.md §2.4.
pub async fn preflight_cold_start(conn_str: &str, slot: &str) -> Result<SlotInfo, PgcdcError> {
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

/// Реконнект внутри работающего процесса, где durable-позиция есть в памяти.
/// Возвращает `Ok(Some(text))`, если расхождение стоит записать в WARN,
/// и `Err`, только если слот ушёл ВПЕРЁД нашей durable-точки.
///
/// На эту функцию пока нет вызывающего кода: цикл репликации, построенный в
/// Task 6, не обрабатывает реконнект вообще, это появится двумя этапами
/// позже. Не удалять как мёртвый код — реконнект-обработчик станет её
/// единственным потребителем.
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

/// PostgreSQL печатает LSN как `X/Y` в шестнадцатеричном виде.
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
        // Кто-то подтвердил WAL, который мы не довели до sink.
        let err = check_reconnect("s", &info(0x2000), Lsn(0x1000)).unwrap_err();
        assert!(matches!(err, PgcdcError::SlotAhead { .. }));
    }

    #[test]
    fn slot_behind_is_a_warning_not_a_failure() {
        // Ожидаемый исход обрыва: последний send_feedback мог не дойти.
        // START_REPLICATION с 0/0 перечитает промежуток дубликатами,
        // что инвариант «дубликаты допустимы» прямо разрешает.
        // Падать здесь означало бы падать при каждом сетевом сбое.
        let warn = check_reconnect("s", &info(0x1000), Lsn(0x2000)).unwrap();
        assert!(warn.is_some(), "расхождение должно быть замечено");
        let text = warn.unwrap();
        assert!(
            text.contains("1000") && text.contains("2000"),
            "обе позиции в сообщении"
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
        // Слот есть, но ни разу не подтверждался — он позади любой нашей позиции.
        assert!(check_reconnect("s", &empty, Lsn(0x1000)).unwrap().is_some());
    }

    #[test]
    fn parses_postgres_lsn_text() {
        assert_eq!(parse_lsn("0/19300D0"), Some(Lsn(0x0193_00D0)));
        assert_eq!(parse_lsn("1/FF"), Some(Lsn(0x0000_0001_0000_00FF)));
        assert_eq!(parse_lsn("garbage"), None);
    }
}
