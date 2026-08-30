use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;

use crate::lsn::Lsn;

/// Значения колонок всегда строки либо JSON null (DECISIONS Q16).
/// `serde_json::Map` с включённым `preserve_order` держит порядок колонок таблицы.
pub type Row = serde_json::Map<String, serde_json::Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

/// Что именно сервер прислал в старом кортеже. Потребитель обязан различать
/// «полная старая строка» и «только ключ», иначе примет заглушку за NULL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BeforeKind {
    Key,
    Full,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ChangeEvent {
    pub schema: String,
    pub table: String,
    pub operation: Operation,
    pub before: Option<Row>,
    pub before_kind: Option<BeforeKind>,
    pub after: Option<Row>,
    pub unchanged_columns: Vec<String>,
    pub transaction_id: u32,
    pub lsn: Lsn,
    pub commit_lsn: Lsn,
    #[serde(serialize_with = "serialize_ts")]
    pub commit_timestamp: DateTime<Utc>,
}

fn serialize_ts<S: serde::Serializer>(ts: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&ts.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
}

/// Микросекунды от эпохи PostgreSQL (2000-01-01T00:00:00Z), а не от Unix-эпохи.
/// Смещение — 946684800 секунд.
pub fn pg_micros_to_utc(micros: i64) -> DateTime<Utc> {
    const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;
    let secs = micros.div_euclid(1_000_000) + PG_EPOCH_UNIX_SECS;
    let nanos = (micros.rem_euclid(1_000_000) * 1_000) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .expect("valid pg timestamp")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_insert() -> ChangeEvent {
        let mut after = Row::new();
        after.insert("id".into(), "1".into());
        after.insert("name".into(), "Alice".into());
        after.insert("email".into(), "alice@example.com".into());
        after.insert("bio".into(), serde_json::Value::Null);
        ChangeEvent {
            schema: "public".into(),
            table: "users".into(),
            operation: Operation::Insert,
            before: None,
            before_kind: None,
            after: Some(after),
            unchanged_columns: Vec::new(),
            transaction_id: 737,
            lsn: Lsn(0x0192_FFC0),
            commit_lsn: Lsn(0x0193_00D0),
            commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
        }
    }

    #[test]
    fn insert_event_serializes_to_the_contract() {
        let json = serde_json::to_string(&sample_insert()).unwrap();
        let expected = concat!(
            r#"{"schema":"public","table":"users","operation":"insert","#,
            r#""before":null,"before_kind":null,"#,
            r#""after":{"id":"1","name":"Alice","email":"alice@example.com","bio":null},"#,
            r#""unchanged_columns":[],"transaction_id":737,"#,
            r#""lsn":"0/192FFC0","commit_lsn":"0/19300D0","#,
            r#""commit_timestamp":"2026-08-30T16:42:31.314489Z"}"#
        );
        assert_eq!(json, expected);
    }

    #[test]
    fn optional_fields_are_present_not_omitted() {
        // Стабильная форма важнее компактности (DECISIONS Q20): потребитель
        // не должен писать `if "unchanged_columns" in event`.
        let json = serde_json::to_string(&sample_insert()).unwrap();
        assert!(json.contains(r#""before":null"#));
        assert!(json.contains(r#""before_kind":null"#));
        assert!(json.contains(r#""unchanged_columns":[]"#));
    }

    #[test]
    fn column_order_follows_the_table_not_the_alphabet() {
        let json = serde_json::to_string(&sample_insert()).unwrap();
        let id_at = json.find(r#""id""#).unwrap();
        let bio_at = json.find(r#""bio""#).unwrap();
        assert!(id_at < bio_at, "порядок колонок должен быть как в таблице");
    }

    #[test]
    fn timestamp_uses_the_2000_epoch_not_1970() {
        // Ровно та ловушка, что описана в docs/pgoutput-notes.md §5: от эпохи 1970
        // это же число даёт 1996-08-30 с тем же днём месяца и тем же временем суток,
        // то есть выглядит правдоподобной датой. Поэтому сверяем точное значение.
        let ts = pg_micros_to_utc(841_423_351_314_489);
        assert_eq!(
            ts.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            "2026-08-30T16:42:31.314489Z"
        );
    }
}
