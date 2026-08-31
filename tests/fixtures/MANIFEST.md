# pgoutput byte fixtures — manifest

Captured in Task 4 (2026-08-30) on a clean slot after `docker compose down -v && docker
compose up -d --wait`: `pgcdc_slot` recreated by `docker/init.sql` at the position right
after `CREATE PUBLICATION`, then a single run of `./target/debug/spike` drove
`scripts/gen-fixtures.sql` through in full. File numbering `NNNN_kind.bin` is the ordinal
position of the pgoutput message within this session (the `seq` counter in `dump()`), not
tied to `wal_start`.

Each `.bin` holds **exactly one pgoutput payload**, without the `XLogData` wrapper and
without a trailing newline — that's how `include_bytes!` reads them in the stage-2 decoder
unit tests. WAL positions from the `XLogData` wrapper (not part of the payload, hence
carried here, DECISIONS Q17) are given in `hi/lo` format, the way `psql`/`pg_lsn` prints
them.

Table format: `wal_start` / `wal_end` — values from the spike's log (`raw.wal_start`,
`raw.wal_end` for that message); for every message in this run `wal_start == wal_end` (row
messages have no LSN of their own, see DECISIONS Q17 and the note about RELATION below).

## Transaction 1 — single INSERT (users, REPLICA IDENTITY FULL)

SQL: `INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', NULL);`

| File | wal_start | wal_end | Checks |
|------|-----------|---------|-----------|
| `0001_begin.bin` | `0/192FFC0` | `0/192FFC0` | BEGIN parsing: xid, commit timestamp, final LSN |
| `0002_relation.bin` | `0/0` | `0/0` | RELATION parsing for `public.users`: 4 columns (id, name, email, bio), their type OIDs and `key` flags. See the note about the zero LSN below |
| `0003_insert.bin` | `0/192FFC0` | `0/192FFC0` | INSERT parsing, 4 columns, one NULL (`bio`), three text (`t`) |
| `0004_commit.bin` | `0/1930100` | `0/1930100` | COMMIT parsing: flags, `commit_lsn`, `end_lsn`, timestamp |

## Transaction 2 — UPDATE, REPLICA IDENTITY FULL → full old tuple (`O`)

SQL: `UPDATE users SET name = 'Bob' WHERE id = 1;`

| File | wal_start | wal_end | Checks |
|------|-----------|---------|-----------|
| `0005_begin.bin` | `0/1930100` | `0/1930100` | BEGIN of the second transaction |
| `0006_update.bin` | `0/1930100` | `0/1930100` | UPDATE with `before_kind = full` (tag `'O'`): old tuple — three text columns (`id`, `name`, `email`) and `bio` with tag `'n'` (a real NULL, not a placeholder), new tuple — `name` changed to `Bob`, `bio` stays NULL. The parser must distinguish the `O`/`K`/no-tag cases before the new tuple |
| `0007_commit.bin` | `0/19301B8` | `0/19301B8` | COMMIT |

## Transaction 3 — DELETE, REPLICA IDENTITY FULL

SQL: `DELETE FROM users WHERE id = 1;`

| File | wal_start | wal_end | Checks |
|------|-----------|---------|-----------|
| `0008_begin.bin` | `0/19301B8` | `0/19301B8` | BEGIN |
| `0009_delete.bin` | `0/19301B8` | `0/19301B8` | DELETE with `before_kind = full` (tag `'O'`): full old tuple (`id=1, name='Bob', email='alice@example.com', bio=NULL`) |
| `0010_commit.bin` | `0/1930248` | `0/1930248` | COMMIT |

## Transaction 4a — INSERT, REPLICA IDENTITY DEFAULT (items)

SQL: `INSERT INTO items VALUES (10, 'Widget', 5);`

| File | wal_start | wal_end | Checks |
|------|-----------|---------|-----------|
| `0011_begin.bin` | `0/1930248` | `0/1930248` | BEGIN |
| `0012_relation.bin` | `0/0` | `0/0` | RELATION parsing for `public.items`: 3 columns (id, title, qty). The second (and last) RELATION message in the whole run — an observation about this particular run (no DDL, no replica identity change, no publication change happened), not a protocol-wide rule: a repeated RELATION for the same OID is legal and must replace the cache entry, see `docs/pgoutput-notes.md` §6 |
| `0013_insert.bin` | `0/1930248` | `0/1930248` | INSERT into a table with a different OID/column schema, so the decoder keeps a relation cache across several tables at once |
| `0014_commit.bin` | `0/1930368` | `0/1930368` | COMMIT |

## Transaction 4b — UPDATE, REPLICA IDENTITY DEFAULT → no old tuple

SQL: `UPDATE items SET qty = 7 WHERE id = 10;`

| File | wal_start | wal_end | Checks |
|------|-----------|---------|-----------|
| `0015_begin.bin` | `0/1930368` | `0/1930368` | BEGIN |
| `0016_update.bin` | `0/1930368` | `0/1930368` | UPDATE with `before_kind` **absent** (no `O`/`K` tag, straight to `'N'` and the new tuple): key columns didn't change, REPLICA IDENTITY DEFAULT doesn't send the old row version at all |
| `0017_commit.bin` | `0/19303F0` | `0/19303F0` | COMMIT |

## Transaction 4c — DELETE, REPLICA IDENTITY DEFAULT → key only (`K`)

SQL: `DELETE FROM items WHERE id = 10;`

| File | wal_start | wal_end | Checks |
|------|-----------|---------|-----------|
| `0018_begin.bin` | `0/19303F0` | `0/19303F0` | BEGIN |
| `0019_delete.bin` | `0/19303F0` | `0/19303F0` | DELETE with `before_kind = key` (tag `'K'`): the old tuple carries only `id='10'` as text, `title`/`qty` are NULL placeholders (not values!) |
| `0020_commit.bin` | `0/1930468` | `0/1930468` | COMMIT |

## Transaction 5a — INSERT with a TOAST value (bio, STORAGE EXTERNAL)

SQL:
```sql
INSERT INTO users
SELECT 2, 'Carol', 'carol@example.com',
       (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 300));
```

| File | wal_start | wal_end | Checks |
|------|-----------|---------|-----------|
| `0021_begin.bin` | `0/1932D18` | `0/1932D18` | BEGIN |
| `0022_insert.bin` | `0/1932D18` | `0/1932D18` | INSERT with a large TOAST value (`bio`, 9600 bytes of text) — on INSERT the value always arrives in full as text (tag `'t'`); the `'u'` marker can't appear here (inferred from the behavior of PostgreSQL's reorder buffer, not confirmed directly by this fixture's bytes — see `docs/pgoutput-notes.md` §9): the TOAST optimization applies only to UPDATE. The file is 9651 bytes — the largest INSERT in the set, good for checking that the decoder doesn't truncate long `int32` column lengths |
| `0023_commit.bin` | `0/1932DF8` | `0/1932DF8` | COMMIT |

## Transaction 5b — UPDATE that doesn't touch the TOAST column → `'u'` marker

SQL: `UPDATE users SET name = 'Caroline' WHERE id = 2;`

**The most fragile and most important fixture in the set.**

| File | wal_start | wal_end | Checks |
|------|-----------|---------|-----------|
| `0024_begin.bin` | `0/1932E30` | `0/1932E30` | BEGIN |
| `0025_update.bin` | `0/1932E30` | `0/1932E30` | **UPDATE with the TOAST marker `'u'`.** `before_kind = full` (tag `'O'`, since REPLICA IDENTITY FULL): the old tuple carries `bio` in full as text (9600 bytes, tag `'t'` — the only place in the set where the old tuple is also TOAST-sized). In the new tuple, `bio` is a single-byte tag `'u'` (unchanged-toast), with no length and no data: the column didn't change, and PostgreSQL didn't bother carrying the TOAST value over from the old WAL record. The decoder must distinguish `'u'` from `'n'` (NULL) and from `'t'` (data present) |
| `0026_commit.bin` | `0/19354A0` | `0/19354A0` | COMMIT |

**Empirical TOAST verification (see also the "TOAST evidence" section below):**
`pg_column_size(bio) FROM users WHERE id = 2` → `9600` (the TOAST threshold on STORAGE
EXTERNAL — the in-row limit is ~2 KB; 9600 is well above that, so `bio` is guaranteed to be
stored out of line). The `0x75` (`'u'`) marker was found by the byte parser in
`0025_update.bin`, at the format position of the 4th column of the new tuple (`bio`), after
three text columns — structural parsing (see the report) consumed exactly all 9696 bytes of
the file with nothing left over.

## Transaction 6 — multi-statement transaction (INSERT + UPDATE + DELETE in one BEGIN/COMMIT)

SQL:
```sql
BEGIN;
INSERT INTO users VALUES (3, 'Dave', 'dave@example.com', NULL);
UPDATE users SET email = 'dave2@example.com' WHERE id = 3;
DELETE FROM users WHERE id = 3;
COMMIT;
```

| File | wal_start | wal_end | Checks |
|------|-----------|---------|-----------|
| `0027_begin.bin` | `0/19354A0` | `0/19354A0` | BEGIN of one transaction covering three DML statements in a row |
| `0028_insert.bin` | `0/19354A0` | `0/19354A0` | INSERT inside a multi-statement transaction — RELATION for `users` is not repeated (already sent in `0002_relation.bin`), the decoder must reuse the relation cache across transactions within a session |
| `0029_update.bin` | `0/1935538` | `0/1935538` | UPDATE inside the same transaction, `before_kind = full` (only `email` changes, but REPLICA IDENTITY FULL still sends the full old tuple) |
| `0030_delete.bin` | `0/19355C0` | `0/19355C0` | DELETE inside the same transaction, `before_kind = full` |
| `0031_commit.bin` | `0/1935650` | `0/1935650` | A single COMMIT for three changes — checks that the decoder groups several row messages under one BEGIN/COMMIT into one logical transaction |

## Transaction 7 — ROLLBACK

SQL:
```sql
BEGIN;
INSERT INTO users VALUES (999, 'Ghost', 'ghost@example.com', NULL);
ROLLBACK;
```

**No files.** PostgreSQL neither decodes nor sends anything into pgoutput for rolled-back
transactions — no BEGIN, no INSERT, no equivalent of a COMMIT/ABORT message in
`proto_version=1`. After running this block, the `seq` counter in the spike stayed at `31`
(the last file — `0031_commit.bin` from transaction 6), no new files appeared. This
confirms the expectation from the brief rather than testing our own code — the mere absence
of a fixture is itself the required result for stage 2 (the test "rollback → decoder sees 0
events" is written without byte data, simply as an assertion of absence).

## Note: RELATION messages and `wal_start`/`wal_end` = `0/0` (cause unconfirmed)

Both RELATION messages (`0002_relation.bin`, `0012_relation.bin`) arrived with `wal_start`
and `wal_end` equal to `0/0` — unlike every neighboring message in the same logical
transaction: `0002_relation` (`0/0`) comes right before `0003_insert` (`0/192FFC0`) within
the same row change, and the same pattern repeats in the `0012`/`0013` pair.

This is not a bug in `spike.rs`: `pg_walstream::stream::parse_xlogdata_header` reads
`wal_start`/`wal_end` directly from bytes 1..9 and 9..17 of the `XLogData` header, with no
branch on message type whatsoever — so client-side code could not have zeroed out precisely
these two messages on its own.

At the same time, a server-side cause is **not confirmed and, by all appearances, is not the
explanation**. Reading the PostgreSQL source, `change_cb_wrapper` (`logical.c`) sets
`ctx->write_location = change->lsn` once before calling `pgoutput_change`; `pgoutput_change`
(`pgoutput.c`) first calls `maybe_send_schema()` (which emits RELATION), then emits the row
message itself — and both go through `WalSndPrepareWrite(ctx, lsn, ...)` with the same
unchanged `ctx->write_location`. Nothing between these two calls zeroes `write_location`. In
other words, by the server's own logic RELATION and the row message that follows it should
carry the same, non-zero LSN — exactly what we do NOT observe in the captured data. The
cause of the zeroing is unknown: the `pg_walstream` 0.8.1 code that fills these fields also
contains no special handling by message type, so the mechanism isn't localized either on the
server (by reading its source) or in the client library (by reading its code) — i.e. this is
an open question, not an established fact. No further investigation was carried out: that
would be a separate task (capturing a packet dump on the wire, or re-reading the same stream
with a different client library, to find out whether the `0/0` already comes over the
network or gets zeroed somewhere else).

The operational conclusion doesn't depend on the cause: the stage-2 decoder must not rely on
`wal_start`/`wal_end` from the RELATION message's wrapper as a meaningful position for
anything (e.g. for a progress checkpoint).

## Final breakdown by message type

| Type | File count |
|-----|---------------|
| `begin` | 9 |
| `commit` | 9 |
| `relation` | 2 |
| `insert` | 4 |
| `update` | 4 |
| `delete` | 3 |
| **Total** | **31** |

All six required message types are present (BEGIN, COMMIT, RELATION, INSERT, UPDATE,
DELETE). RELATION occurs exactly twice — once per table (`users`, `items`) in this run;
that's an observation about this particular set, not a protocol rule — see
`docs/pgoutput-notes.md` §6. No TRUNCATE/TYPE/ORIGIN messages occur in this SQL set — they
were out of scope for Task 4.
