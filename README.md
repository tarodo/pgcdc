# pgcdc

Минимальный движок Change Data Capture для PostgreSQL на Rust. Читает события логической
репликации напрямую по протоколу `pgoutput` и печатает нормализованные JSON-события.

## Что уже работает

Этап 2 — полный декодер: декодируются `BEGIN`, `COMMIT`, `RELATION`, `INSERT`, `UPDATE`,
`DELETE` → JSON на stdout, подтверждение LSN только после успешной записи в sink. В
событии есть `before` с различением «полная строка» (`before_kind: "full"`, REPLICA
IDENTITY FULL) и «только ключ» (`before_kind: "key"`, изменился первичный ключ при
DEFAULT-идентичности); несланные TOAST-значения называются в `unchanged_columns` и в
`after` не появляются.

Этап 3 — корректность подтверждения: файловый sink с fsync (`--output file`), групповое
подтверждение по таймеру (`--ack-interval-ms`), продвижение слота на простаивающей
публикации по keepalive — без этого запись в таблицы вне публикации держала бы слот на
месте и растила WAL бесконечно.

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

Значения `transaction_id`, `lsn`, `commit_lsn` и `commit_timestamp` здесь — из
захваченной фикстуры; у вас на реальном прогоне они будут другими. Демо использует
фиксированный первичный ключ (`id = 1`), поэтому повторный запуск нужно предварять
`docker compose down -v` — иначе `INSERT` упадёт на дубликате ключа.

Логи идут в stderr, полезная нагрузка — в stdout, поэтому вывод можно безопасно
направлять в конвейер.

## Гарантии

Дубликаты после сбоя допустимы; тихая потеря — нет. Позиция WAL не подтверждается
PostgreSQL раньше, чем sink отчитался об успешной записи. Единственное исключение —
простаивающая публикация: слот продвигается до позиции сервера из keepalive, потому что
этот диапазон доказуемо не содержит ни одной строки нашей публикации; каждый диапазон,
где данные были, по-прежнему ждёт барьера.

Позиция подтверждается только после успешного барьера durability (`Sink::flush`), а не
после одного лишь приёма записи (`Sink::write_transaction`) — между ними существует окно,
и подтверждение внутри него означало бы подтверждение того, что ещё может быть потеряно
при крахе.

Не судите о прогрессе процесса по `pg_stat_replication.write_lsn`: библиотека
транспорта пересылает в нём позицию «получено по сети», которая обгоняет то, что
реально доведено до диска — ориентируйтесь на `confirmed_flush_lsn` слота. По той
же причине не добавляйте этот слот в `synchronous_standby_names`: утёкший вперёд
`write_lsn` начал бы освобождать ожидающие `synchronous_commit` раньше времени.

## Документация

- [DECISIONS.md](DECISIONS.md) — принятые решения по MVP
- [docs/pgoutput-notes.md](docs/pgoutput-notes.md) — побайтовый разбор протокола
- [docs/spike-findings.md](docs/spike-findings.md) — выводы по транспорту
