# pgoutput byte fixtures — manifest

Снято Task 4 (2026-08-30) на чистом слоте после `docker compose down -v && docker compose up
-d --wait`: `pgcdc_slot` пересоздан `docker/init.sql` на позиции сразу после `CREATE
PUBLICATION`, затем один запуск `./target/debug/spike` прогнал `scripts/gen-fixtures.sql`
целиком. Нумерация файлов `NNNN_kind.bin` — это порядковый номер сообщения pgoutput в этой
сессии (счётчик `seq` в `dump()`), не связан с `wal_start`.

Каждый `.bin` содержит **ровно один payload pgoutput**, без обёртки `XLogData` и без
финального перевода строки — так их читает `include_bytes!` в юнит-тестах декодера этапа 2.
Позиции WAL из обёртки `XLogData` (не входят в payload, поэтому вынесены сюда, DECISIONS
Q17) приведены в формате `hi/lo`, как их печатает `psql`/`pg_lsn`.

Формат таблицы: `wal_start` / `wal_end` — значения из лога spike'а (`raw.wal_start`,
`raw.wal_end` для этого сообщения); для всех сообщений в этом прогоне `wal_start == wal_end`
(row-сообщения не имеют собственного LSN, см. DECISIONS Q17 и примечание про RELATION ниже).

## Транзакция 1 — одиночный INSERT (users, REPLICA IDENTITY FULL)

SQL: `INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', NULL);`

| Файл | wal_start | wal_end | Проверяет |
|------|-----------|---------|-----------|
| `0001_begin.bin` | `0/192FFC0` | `0/192FFC0` | Разбор BEGIN: xid, commit-timestamp, final LSN |
| `0002_relation.bin` | `0/0` | `0/0` | Разбор RELATION для `public.users`: 4 колонки (id, name, email, bio), их OID типов и `key`-флаги. См. примечание про нулевой LSN ниже |
| `0003_insert.bin` | `0/192FFC0` | `0/192FFC0` | Разбор INSERT, 4 колонки, одна NULL (`bio`), три текстовых (`t`) |
| `0004_commit.bin` | `0/1930100` | `0/1930100` | Разбор COMMIT: flags, `commit_lsn`, `end_lsn`, timestamp |

## Транзакция 2 — UPDATE, REPLICA IDENTITY FULL → старый кортеж целиком (`O`)

SQL: `UPDATE users SET name = 'Bob' WHERE id = 1;`

| Файл | wal_start | wal_end | Проверяет |
|------|-----------|---------|-----------|
| `0005_begin.bin` | `0/1930100` | `0/1930100` | BEGIN второй транзакции |
| `0006_update.bin` | `0/1930100` | `0/1930100` | UPDATE с `before_kind = full` (тег `'O'`): старый кортеж — все 4 колонки текстом, новый кортеж — `name` изменился на `Bob`, `bio` остаётся NULL. Парсер должен различать тег `O`/`K`/отсутствие тега перед новым кортежем |
| `0007_commit.bin` | `0/19301B8` | `0/19301B8` | COMMIT |

## Транзакция 3 — DELETE, REPLICA IDENTITY FULL

SQL: `DELETE FROM users WHERE id = 1;`

| Файл | wal_start | wal_end | Проверяет |
|------|-----------|---------|-----------|
| `0008_begin.bin` | `0/19301B8` | `0/19301B8` | BEGIN |
| `0009_delete.bin` | `0/19301B8` | `0/19301B8` | DELETE с `before_kind = full` (тег `'O'`): полный старый кортеж (`id=1, name='Bob', email='alice@example.com', bio=NULL`) |
| `0010_commit.bin` | `0/1930248` | `0/1930248` | COMMIT |

## Транзакция 4a — INSERT, REPLICA IDENTITY DEFAULT (items)

SQL: `INSERT INTO items VALUES (10, 'Widget', 5);`

| Файл | wal_start | wal_end | Проверяет |
|------|-----------|---------|-----------|
| `0011_begin.bin` | `0/1930248` | `0/1930248` | BEGIN |
| `0012_relation.bin` | `0/0` | `0/0` | Разбор RELATION для `public.items`: 3 колонки (id, title, qty). Второе (и последнее) сообщение RELATION за весь прогон — PostgreSQL шлёт его один раз на таблицу за сессию, не на каждый DML |
| `0013_insert.bin` | `0/1930248` | `0/1930248` | INSERT в таблицу с другим OID/схемой колонок, чтобы декодер держал relation-cache по нескольким таблицам одновременно |
| `0014_commit.bin` | `0/1930368` | `0/1930368` | COMMIT |

## Транзакция 4b — UPDATE, REPLICA IDENTITY DEFAULT → старого кортежа нет

SQL: `UPDATE items SET qty = 7 WHERE id = 10;`

| Файл | wal_start | wal_end | Проверяет |
|------|-----------|---------|-----------|
| `0015_begin.bin` | `0/1930368` | `0/1930368` | BEGIN |
| `0016_update.bin` | `0/1930368` | `0/1930368` | UPDATE с `before_kind` **отсутствует** (нет тега `O`/`K`, сразу `'N'` и новый кортеж): ключевые колонки не менялись, REPLICA IDENTITY DEFAULT не шлёт старую версию строки вовсе |
| `0017_commit.bin` | `0/19303F0` | `0/19303F0` | COMMIT |

## Транзакция 4c — DELETE, REPLICA IDENTITY DEFAULT → только ключ (`K`)

SQL: `DELETE FROM items WHERE id = 10;`

| Файл | wal_start | wal_end | Проверяет |
|------|-----------|---------|-----------|
| `0018_begin.bin` | `0/19303F0` | `0/19303F0` | BEGIN |
| `0019_delete.bin` | `0/19303F0` | `0/19303F0` | DELETE с `before_kind = key` (тег `'K'`): в старом кортеже только `id='10'` текстом, `title`/`qty` — NULL-заглушки (не значения!) |
| `0020_commit.bin` | `0/1930468` | `0/1930468` | COMMIT |

## Транзакция 5a — INSERT с TOAST-значением (bio, STORAGE EXTERNAL)

SQL:
```sql
INSERT INTO users
SELECT 2, 'Carol', 'carol@example.com',
       (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 300));
```

| Файл | wal_start | wal_end | Проверяет |
|------|-----------|---------|-----------|
| `0021_begin.bin` | `0/1932D18` | `0/1932D18` | BEGIN |
| `0022_insert.bin` | `0/1932D18` | `0/1932D18` | INSERT с большим TOAST-значением (`bio`, 9600 байт текста) — на INSERT значение всегда приходит полностью текстом (тег `'t'`), маркера `'u'` тут быть не может: TOAST-оптимизация применима только к UPDATE. Файл 9651 байт — самый большой INSERT в наборе, годится для проверки, что декодер не режет длинные `int32`-длины колонок |
| `0023_commit.bin` | `0/1932DF8` | `0/1932DF8` | COMMIT |

## Транзакция 5b — UPDATE, не трогающий TOAST-колонку → маркер `'u'`

SQL: `UPDATE users SET name = 'Caroline' WHERE id = 2;`

**Самая хрупкая и самая важная фикстура набора.**

| Файл | wal_start | wal_end | Проверяет |
|------|-----------|---------|-----------|
| `0024_begin.bin` | `0/1932E30` | `0/1932E30` | BEGIN |
| `0025_update.bin` | `0/1932E30` | `0/1932E30` | **UPDATE с TOAST-маркером `'u'`.** `before_kind = full` (тег `'O'`, т.к. REPLICA IDENTITY FULL): старый кортеж несёт `bio` целиком текстом (9600 байт, тег `'t'`, это единственное место в наборе, где старый кортеж тоже TOAST-размера). В новом кортеже `bio` — однобайтовый тег `'u'` (unchanged-toast), без длины и без данных: колонка не менялась, Postgres не стал перетаскивать TOAST-значение из старой WAL-записи. Декодер обязан отличать `'u'` от `'n'` (NULL) и от `'t'` (данные есть) |
| `0026_commit.bin` | `0/19354A0` | `0/19354A0` | COMMIT |

**Проверка TOAST эмпирически (см. также раздел «TOAST evidence» ниже):**
`pg_column_size(bio) FROM users WHERE id = 2` → `9600` (порог TOAST на STORAGE EXTERNAL —
внутристрочный лимит ~2 КБ; 9600 существенно больше, значит `bio` гарантированно хранится
не в основной строке). Маркер `0x75` (`'u'`) байт-парсером найден в `0025_update.bin`,
в позиции формата 4-й колонки нового кортежа (`bio`), после трёх текстовых колонок —
структурный парсинг (см. отчёт) потребил ровно все 9696 байт файла без остатка.

## Транзакция 6 — многострочная транзакция (INSERT + UPDATE + DELETE в одном BEGIN/COMMIT)

SQL:
```sql
BEGIN;
INSERT INTO users VALUES (3, 'Dave', 'dave@example.com', NULL);
UPDATE users SET email = 'dave2@example.com' WHERE id = 3;
DELETE FROM users WHERE id = 3;
COMMIT;
```

| Файл | wal_start | wal_end | Проверяет |
|------|-----------|---------|-----------|
| `0027_begin.bin` | `0/19354A0` | `0/19354A0` | BEGIN одной транзакции на три DML подряд |
| `0028_insert.bin` | `0/19354A0` | `0/19354A0` | INSERT внутри многострочной транзакции — RELATION для `users` не повторяется (уже было в `0002_relation.bin`), декодер должен переиспользовать relation-cache между транзакциями сессии |
| `0029_update.bin` | `0/1935538` | `0/1935538` | UPDATE внутри той же транзакции, `before_kind = full` (меняется только `email`, но REPLICA IDENTITY FULL всё равно шлёт полный старый кортеж) |
| `0030_delete.bin` | `0/19355C0` | `0/19355C0` | DELETE внутри той же транзакции, `before_kind = full` |
| `0031_commit.bin` | `0/1935650` | `0/1935650` | Один COMMIT на три изменения — проверяет, что декодер группирует несколько row-сообщений под одним BEGIN/COMMIT в одну логическую транзакцию |

## Транзакция 7 — ROLLBACK

SQL:
```sql
BEGIN;
INSERT INTO users VALUES (999, 'Ghost', 'ghost@example.com', NULL);
ROLLBACK;
```

**Файлов нет.** PostgreSQL не декодирует и не шлёт в pgoutput ничего для откаченных
транзакций — ни BEGIN, ни INSERT, ни какого-либо аналога COMMIT/ABORT-сообщения в
`proto_version=1`. После выполнения этого блока счётчик `seq` в spike остался на `31`
(последний файл — `0031_commit.bin` от транзакции 6), новых файлов не появилось. Это
подтверждает ожидание из брифа, а не проверяет наш код — сам факт отсутствия фикстуры
и есть требуемый результат для этапа 2 (тест «rollback → decoder видит 0 событий» пишется
без байтовых данных, просто как утверждение об отсутствии).

## Примечание: RELATION-сообщения и `wal_start`/`wal_end` = `0/0`

Оба сообщения RELATION (`0002_relation.bin`, `0012_relation.bin`) пришли с `wal_start` и
`wal_end`, равными `0/0`, в отличие от всех остальных сообщений сессии. Это не артефакт
`spike.rs` и не баг транспорта: `pg_walstream::stream::parse_xlogdata_header` читает эти
два поля напрямую из байтов 1..9 и 9..17 заголовка `XLogData`, которые пришли по проводу от
`walsender` уже нулевыми. То есть сам PostgreSQL не связывает сообщение RELATION с конкретной
позицией WAL (это синтетическое метаданные-сообщение output-плагина, а не результат декодирования
отдельной WAL-записи). Декодер этапа 2 не должен полагаться на `wal_start`/`wal_end` из обёртки
RELATION-сообщения как на значимую позицию для чего-либо (например, для чекпоинта прогресса).

## Итоговая раскладка по типам сообщений

| Тип | Кол-во файлов |
|-----|---------------|
| `begin` | 9 |
| `commit` | 9 |
| `relation` | 2 |
| `insert` | 4 |
| `update` | 4 |
| `delete` | 3 |
| **Итого** | **31** |

Все шесть обязательных типов сообщений присутствуют (BEGIN, COMMIT, RELATION, INSERT, UPDATE,
DELETE). RELATION встречается ровно дважды — по одному разу на таблицу (`users`, `items`) за
сессию, не на каждую DML-операцию, как и ожидалось. TRUNCATE/TYPE/ORIGIN сообщений в этом
наборе SQL не возникает — они не входили в объём Task 4.
