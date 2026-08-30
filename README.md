# pgcdc

Минимальный движок Change Data Capture для PostgreSQL на Rust. Читает события логической
репликации напрямую по протоколу `pgoutput` и печатает нормализованные JSON-события.

## Что уже работает

Этап 1 — сквозной срез: `BEGIN`, `RELATION`, `INSERT`, `COMMIT` → JSON на stdout,
подтверждение LSN только после успешной записи в sink. `UPDATE` и `DELETE` пока
возвращают явную ошибку, а не игнорируются молча.

## Демо

```bash
docker compose up -d --wait

cargo run -- \
  --database-url postgres://postgres:postgres@localhost:5432/app \
  --publication pgcdc_pub \
  --slot pgcdc_slot \
  --output stdout
```

В другом терминале:

```sql
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', NULL);
```

Ожидаемый вывод:

```json
{"schema":"public","table":"users","operation":"insert","before":null,"before_kind":null,"after":{"id":"1","name":"Alice","email":"alice@example.com","bio":null},"unchanged_columns":[],"transaction_id":737,"lsn":"0/192FFC0","commit_lsn":"0/19300D0","commit_timestamp":"2026-08-30T16:42:31.314489Z"}
```

Логи идут в stderr, полезная нагрузка — в stdout, поэтому вывод можно безопасно
направлять в конвейер.

## Гарантии

Дубликаты после сбоя допустимы; тихая потеря — нет. Позиция WAL не подтверждается
PostgreSQL раньше, чем sink отчитался об успешной записи.

## Документация

- [DECISIONS.md](DECISIONS.md) — принятые решения по MVP
- [docs/pgoutput-notes.md](docs/pgoutput-notes.md) — побайтовый разбор протокола
- [docs/spike-findings.md](docs/spike-findings.md) — выводы по транспорту
