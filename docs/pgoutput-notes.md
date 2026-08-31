# pgoutput: a byte-by-byte reading of the fixtures

A manual reading of `tests/fixtures/*.bin` (Task 5 of stage 0) against
[PostgreSQL 16, Logical Replication Message Formats](https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html).

This document is the **specification** stage 2 used to write the decoder tests **before** the
implementation. Everything written here is confirmed by bytes from the fixtures, except what is
explicitly marked "from the documentation, not confirmed" and the "Not analysed" section at the end.

---

## 0. Capture conditions — the layout depends on them

The fixtures were captured by `src/bin/spike.rs` with these parameters (see `docs/spike-findings.md` §2):

| Parameter | Value | Consequence for the layout |
|----------|----------|--------------------------|
| `proto_version` | `1` | **No message carries an xid prefix.** In protocol v2+ an `Int32 xid` appears in `R`/`I`/`U`/`D` right after the type byte — every offset below would shift by 4 |
| `StreamingMode` | `Off` | No `S`/`E`/`c`/`A` messages (stream start/stop/commit/abort), no partial transactions |
| value format | text (the `binary` option was not requested) | Only the `'t'` tag occurs in `TupleData`; there is no `'b'` tag |
| publication | `pgcdc_pub` FOR TABLE `public.users`, `public.items` | Two tables; this run delivered exactly two `RELATION` messages (why that is not a rule — §6) |

Each `.bin` is **exactly one pgoutput payload**, without the `XLogData` envelope. The WAL positions
from the envelope live in `tests/fixtures/MANIFEST.md`; they are not in the fixture bytes (DECISIONS Q17).

## 1. General format conventions

- **Byte order is big-endian (network order)** for every integer field. Confirmed: the column
  count of `users` arrived as `00 04`, not `04 00`; the OID of type `text` is `00 00 00 19` (25).
- **`String` is a C string**: bytes up to the first `0x00`, and the zero byte itself is part of
  the field. There is no length. Confirmed: `70 75 62 6c 69 63 00` = `"public"`, 7 bytes for 6 characters.
- **`Byte1` is a single ASCII byte**, compare it as a character (`b'B'`, `b'C'`, ...).
- Mapping between the documentation's types and Rust:

| Doc | Rust | How to read |
|-----|------|-----------|
| `Int8` | `u8` | 1 byte |
| `Int16` | `u16` | `be_bytes`, 2 bytes |
| `Int32 (Oid)` | `u32` | `be_bytes`, 4 bytes |
| `Int32` (atttypmod, column length) | **`i32`, signed** | `be_bytes`, 4 bytes — see §14 item 6 |
| `Int64 (XLogRecPtr)` | `u64` | `be_bytes`, 8 bytes |
| `Int64 (TimestampTz)` | **`i64`, signed** | `be_bytes`, 8 bytes |

- **LSN format**: `XLogRecPtr` is a `u64`; the human-readable `pg_lsn` notation is
  `{high 32 bits in hex}/{low 32 bits in hex}`, without leading zeros.
  `0x00000000019300D0` → `0/19300D0`.

## 2. Catalogue of message types (first payload byte)

Present in the set:

| Byte | hex | Type | Fixtures |
|------|-----|-----|---------|
| `B` | `0x42` | Begin | 9 |
| `C` | `0x43` | Commit | 9 |
| `R` | `0x52` | Relation | 2 |
| `I` | `0x49` | Insert | 4 |
| `U` | `0x55` | Update | 4 |
| `D` | `0x44` | Delete | 3 |

Absent from the set (see "Not analysed" item 5): `T` Truncate, `Y` Type, `O` Origin,
`M` Message.

> **Trap.** The byte `'O'` is at once the Origin message type (at position 0) and the old-tuple
> tag inside `U`/`D` (at position 5). Only the position tells them apart. The decoder MUST read
> the message type strictly from byte 0 and MUST NOT "look for" tags by scanning the buffer.

---

## 3. BEGIN

Layout (docs): `Byte1('B')`, `Int64` final LSN, `Int64` commit timestamp, `Int32` xid.
Fixed size **21 bytes**. All 9 begin fixtures are exactly 21 bytes.

### `0001_begin.bin` (21 bytes)

```
00000000: 4200 0000 0001 9300 d000 02fd 4523 f5f8  B...........E#..
00000010: 3900 0002 e1                             9....
```

| off | len | bytes | field | value |
|----:|----:|-------|------|----------|
| 0 | 1 | `42` | message type | `'B'` |
| 1 | 8 | `00 00 00 00 01 93 00 d0` | `final_lsn` (XLogRecPtr) | `0x00000000019300D0` = `0/19300D0` |
| 9 | 8 | `00 02 fd 45 23 f5 f8 39` | `commit_timestamp` (TimestampTz) | `841423351314489` |
| 17 | 4 | `00 00 02 e1` | `xid` | `737` |

21 of 21 accounted for, no remainder.

**What `final_lsn` means.** It is the LSN of this transaction's *future* commit record, not the
position of the BEGIN itself. Confirmed across all 9 pairs: `BEGIN.final_lsn == COMMIT.commit_lsn`
(table in §8). That is, **BEGIN already knows where the transaction will end** — Postgres decodes
it after the commit.

**What `commit_timestamp` means in BEGIN.** The very same timestamp that will arrive in COMMIT.
Confirmed across all 9 pairs, bit for bit. Practical consequence: `commit_timestamp` for the JSON
can be taken from BEGIN already, waiting for COMMIT is not required.

---

## 4. COMMIT and the question "which LSN do we acknowledge"

Layout (docs): `Byte1('C')`, `Int8` flags, `Int64` commit LSN, `Int64` end LSN,
`Int64` commit timestamp. Fixed size **26 bytes**. All 9 commit fixtures are exactly 26 bytes.

### `0004_commit.bin` (26 bytes)

```
00000000: 4300 0000 0000 0193 00d0 0000 0000 0193  C...............
00000010: 0100 0002 fd45 23f5 f839                 .....E#..9
```

| off | len | bytes | field | value |
|----:|----:|-------|------|----------|
| 0 | 1 | `43` | message type | `'C'` |
| 1 | 1 | `00` | `flags` (Int8, doc: "currently unused") | `0` |
| 2 | 8 | `00 00 00 00 01 93 00 d0` | **`commit_lsn`** | `0x00000000019300D0` = **`0/19300D0`** |
| 10 | 8 | `00 00 00 00 01 93 01 00` | **`end_lsn`** | `0x0000000001930100` = **`0/1930100`** |
| 18 | 8 | `00 02 fd 45 23 f5 f8 39` | `commit_timestamp` | `841423351314489` |

26 of 26 accounted for, no remainder.

### Which of the two is which, and which one we acknowledge

Both fields are `Int64 XLogRecPtr`, they follow each other, and they differ by 48 bytes. There is
only one structural way to keep them apart: **`commit_lsn` comes first (offset 2),
`end_lsn` second (offset 10)**.

- **`commit_lsn` (offset 2)** — the LSN of the commit WAL record itself, that is, its **start**.
  Equal to `final_lsn` from the BEGIN of the same transaction.
- **`end_lsn` (offset 10)** — the LSN **immediately past the end** of the commit record, that is,
  the start of the next record.

For `0004_commit.bin`:

```
end_lsn - commit_lsn = 0x1930100 - 0x19300D0 = 0x30 = 48 bytes
```

Exactly 48 bytes — the size of a commit record in this build; the difference is the same across
all nine commit fixtures in the set (§8).

**We acknowledge `end_lsn`** (DECISIONS Q17). The reason is visible straight from the arithmetic:
acknowledge `commit_lsn = 0/19300D0` and `confirmed_flush_lsn` lands on the **start** of the commit
record, so after a restart the server will hand that record over again — the same transaction arrives
a second time. `end_lsn = 0/1930100` points **past** it, and a restart continues with
`0005_begin`.

For stage 0, the actual values from the fixtures (see the full table in §8):

| File | `commit_lsn` (do NOT acknowledge) | `end_lsn` (**acknowledge**) |
|------|-------------------------------|------------------------------|
| `0004_commit.bin` | `0/19300D0` | **`0/1930100`** |
| `0031_commit.bin` | `0/1935620` | **`0/1935650`** |

**Cross-check against the envelope.** In `MANIFEST.md`, for every commit fixture the `wal_end` from
`XLogData` matches the `end_lsn` from the payload — in all nine cases (`0004` → `0/1930100`,
`0007` → `0/19301B8`, ... `0031` → `0/1935650`). So `raw.wal_end` at COMMIT and the payload's
`end_lsn` are one and the same number, and either may be acknowledged. For
`commit_lsn` there is no such match, not once.

**`flags`.** `0x00` in all 9 fixtures. Docs: `Int8(0)`, "Flags; currently unused".
The field MUST be read (otherwise the offsets shift), but it carries no meaning — see
"Not analysed" item 1.

---

## 5. Checking the timestamp epoch — the arithmetic

The classic mistake: reading `commit_timestamp` as Unix time. Let us check it on a real value
from `0001_begin.bin` / `0004_commit.bin`.

```
raw value (Int64 BE, offset 9 in BEGIN / 18 in COMMIT):
    00 02 fd 45 23 f5 f8 39  =  841 423 351 314 489   (microseconds)

1) microseconds -> seconds:
    841423351314489 / 1e6  =  841423351 sec, remainder 314489 us

2) seconds -> days:
    841423351 / 86400  =  9738 days, remainder 60151 sec
    60151 sec = 16 h 42 min 31 sec

3) PostgreSQL epoch: 2000-01-01T00:00:00Z
    2000-01-01 + 9738 days  =  2026-08-30
    result: 2026-08-30T16:42:31.314489Z
```

**The fixtures were captured on 2026-08-30 (see the `MANIFEST.md` header). It checks out.**

Conversion to Unix time, should the code need it:

```
epoch difference 1970-01-01 -> 2000-01-01  =  946 684 800 sec
unix = 946684800 + 841423351 = 1 788 108 151  ->  2026-08-30T16:42:31Z
```

**A counterexample — why this had to be checked.** Read the same number as microseconds since
**1970**:

```
841423351314489 us since 1970-01-01  ->  1996-08-30T16:42:31Z
```

You get **1996-08-30T16:42:31Z** — the same day of the month and the same time of day, exactly
30 years earlier. The coincidence is not accidental: between 1970-01-01 and 1996-08-30 and between
2000-01-01 and 2026-08-30 there is the same number of days (9738) and the same number of leap days
(7), so one and the same offset lands on one and the same calendar date. An epoch error **does not
look like garbage**: the date is plausible, day and time match. A test that only checks "this looks
like a date" will not catch it. The stage 2 test MUST assert the **exact** value
`2026-08-30T16:42:31.314489Z` for the raw `841423351314489`.

---

## 6. RELATION

Layout (docs, proto v1 — no xid):

```
Byte1('R')
Int32   relation OID
String  namespace (C string; empty string for pg_catalog)
String  relation name (C string)
Int8    replica identity (equals relreplident from pg_class)
Int16   column count
  for each column:
    Int8    flags: 0 = no flags, 1 = column is part of the key
    String  column name (C string)
    Int32   type OID
    Int32   atttypmod (signed)
```

RELATION arrives before the first row message of its table, not before every DML.
**Observation on this set:** 11 row messages and only 2 RELATION messages; no table sent RELATION
twice; `0028_insert.bin` (transaction 6) comes with no repeated
RELATION for `users` at all, even though seven transactions and four row messages on the same table
(`0006`, `0009`, `0022`, `0025`) passed between it and `0002_relation.bin`. That is, the
relation cache survives transaction boundaries within a session.

**This MUST NOT be generalised into the rule "RELATION arrives exactly once per table per session".**
It came out that way here only because the whole run had no DDL, no REPLICA IDENTITY change, and no
publication change. pgoutput resends RELATION for the same OID every time the relation sync entry is
invalidated — that is, on DDL against the table, a replica identity change, or a publication change —
and not only after a reconnect. A repeated RELATION for an already known OID is a **normal message**,
and it MUST **replace** the cache entry
(see §9 of the base spec; "replacing the entry in the relation cache" is listed outright as stage 2 work —
`DECISIONS.md` §4, stage 2 "Full decoder"). A reconnect is a separate reason to drop the whole cache (DECISIONS Q19),
not the only one.

### `0002_relation.bin` — `public.users`, REPLICA IDENTITY FULL (75 bytes)

```
00000000: 5200 0040 0170 7562 6c69 6300 7573 6572  R..@.public.user
00000010: 7300 6600 0401 6964 0000 0000 14ff ffff  s.f...id........
00000020: ff01 6e61 6d65 0000 0000 19ff ffff ff01  ..name..........
00000030: 656d 6169 6c00 0000 0019 ffff ffff 0162  email..........b
00000040: 696f 0000 0000 19ff ffff ff              io.........
```

| off | len | bytes | field | value |
|----:|----:|-------|------|----------|
| 0 | 1 | `52` | message type | `'R'` |
| 1 | 4 | `00 00 40 01` | relation OID | `16385` |
| 5 | 7 | `70 75 62 6c 69 63 00` | namespace | `"public"` |
| 12 | 6 | `75 73 65 72 73 00` | relation name | `"users"` |
| 18 | 1 | `66` | **replica identity** | **`'f'` = FULL** |
| 19 | 2 | `00 04` | column count | `4` |
| 21 | 1 | `01` | col[0] flags | `1` — part of the key |
| 22 | 3 | `69 64 00` | col[0] name | `"id"` |
| 25 | 4 | `00 00 00 14` | col[0] type OID | `20` (`int8`) |
| 29 | 4 | `ff ff ff ff` | col[0] atttypmod | `-1` |
| 33 | 1 | `01` | col[1] flags | `1` |
| 34 | 5 | `6e 61 6d 65 00` | col[1] name | `"name"` |
| 39 | 4 | `00 00 00 19` | col[1] type OID | `25` (`text`) |
| 43 | 4 | `ff ff ff ff` | col[1] atttypmod | `-1` |
| 47 | 1 | `01` | col[2] flags | `1` |
| 48 | 6 | `65 6d 61 69 6c 00` | col[2] name | `"email"` |
| 54 | 4 | `00 00 00 19` | col[2] type OID | `25` (`text`) |
| 58 | 4 | `ff ff ff ff` | col[2] atttypmod | `-1` |
| 62 | 1 | `01` | col[3] flags | `1` |
| 63 | 4 | `62 69 6f 00` | col[3] name | `"bio"` |
| 67 | 4 | `00 00 00 19` | col[3] type OID | `25` (`text`) |
| 71 | 4 | `ff ff ff ff` | col[3] atttypmod | `-1` |

75 of 75 accounted for, no remainder.

**Mandatory cross-check from the brief:** the replica identity byte = `0x66` = `'f'`. ✔
The schema in `docker/init.sql` does indeed run `ALTER TABLE public.users REPLICA IDENTITY FULL`.
**All four columns carry flag `1`** — under REPLICA IDENTITY FULL the whole row counts as the key.

### `0012_relation.bin` — `public.items`, REPLICA IDENTITY DEFAULT (61 bytes)

```
00000000: 5200 0040 0870 7562 6c69 6300 6974 656d  R..@.public.item
00000010: 7300 6400 0301 6964 0000 0000 14ff ffff  s.d...id........
00000020: ff00 7469 746c 6500 0000 0019 ffff ffff  ..title.........
00000030: 0071 7479 0000 0000 17ff ffff ff         .qty.........
```

| off | len | bytes | field | value |
|----:|----:|-------|------|----------|
| 0 | 1 | `52` | message type | `'R'` |
| 1 | 4 | `00 00 40 08` | relation OID | `16392` |
| 5 | 7 | `70 75 62 6c 69 63 00` | namespace | `"public"` |
| 12 | 6 | `69 74 65 6d 73 00` | relation name | `"items"` |
| 18 | 1 | `64` | **replica identity** | **`'d'` = DEFAULT** |
| 19 | 2 | `00 03` | column count | `3` |
| 21 | 1 | `01` | col[0] flags | **`1` — part of the key** |
| 22 | 3 | `69 64 00` | col[0] name | `"id"` |
| 25 | 4 | `00 00 00 14` | col[0] type OID | `20` (`int8`) |
| 29 | 4 | `ff ff ff ff` | col[0] atttypmod | `-1` |
| 33 | 1 | `00` | col[1] flags | **`0` — not a key** |
| 34 | 6 | `74 69 74 6c 65 00` | col[1] name | `"title"` |
| 40 | 4 | `00 00 00 19` | col[1] type OID | `25` (`text`) |
| 44 | 4 | `ff ff ff ff` | col[1] atttypmod | `-1` |
| 48 | 1 | `00` | col[2] flags | **`0` — not a key** |
| 49 | 4 | `71 74 79 00` | col[2] name | `"qty"` |
| 53 | 4 | `00 00 00 17` | col[2] type OID | `23` (`int4`) |
| 57 | 4 | `ff ff ff ff` | col[2] atttypmod | `-1` |

61 of 61 accounted for, no remainder.

**Mandatory cross-check from the brief:** the replica identity byte = `0x64` = `'d'`. ✔
Flag `1` only on `id` (PRIMARY KEY), `0` on `title`/`qty`. Exactly what is expected
of DEFAULT: key = primary key.

Both cross-checks passed **from the bytes**, not by fitting: the reading went left to right by the
documentation, and the byte at position 18 turned out to be what it was supposed to be.

### Values of the replica identity byte

`relreplident` from `pg_class`: `'d'` DEFAULT (primary key), `'n'` NOTHING, `'f'` FULL,
`'i'` INDEX (set `USING INDEX`). Only `'d'` and `'f'` were observed in the set.

### Positional correspondence of columns

Neither `TupleData` nor the row messages carry column names. Names are taken **by index** from the
`RELATION` of the same OID: `RELATION.columns[i]` ↔ `TupleData.columns[i]`. In all 11
row fixtures the column count in `TupleData` matched the `ncols` from the corresponding RELATION
(4 for `users`, 3 for `items`), including in tuples with the `'n'` and `'u'` tags — that is,
**`TupleData` always holds an entry for every column**, there are no gaps.

---

## 7. TupleData — the general structure of a tuple

`TupleData` is not a message of its own but a substructure inside `I`/`U`/`D`. Layout:

```
Int16   column count N
  for each of the N columns — one tag byte, and then, depending on the tag:
    'n' (0x6E)  NULL           — nothing more, the next column follows immediately
    'u' (0x75)  unchanged TOAST — nothing more, the next column follows immediately
    't' (0x74)  text           — Int32 length, then exactly that many value bytes
    'b' (0x62)  binary         — Int32 length, then data (does NOT occur in the set)
```

The key point: `'n'` and `'u'` have **no length field**. A decoder that unconditionally reads 4
length bytes after the tag will slide off across the whole rest of the buffer.

Tag tally across all 11 row fixtures: `'t'` — 42 positions, `'n'` — 10 positions,
`'u'` — **exactly one** (`0025_update.bin`, offset 9695). The `'b'` tag never appears.

**The length is a signed `Int32`.** Observed values: from `1` to `9600`
(`00 00 25 80` in `0022`/`0025`). No negative lengths were observed; reading as `i32`
and rejecting a negative value is a safeguard, not a verified case.

**The value is always the type's text representation, not binary.** `id BIGINT` with value
`1` arrived as the single byte `0x31` = ASCII `'1'`, not as 8 bytes of `int8`. `qty INT` = `5`
arrived as `0x35`. This follows from the `binary` option not being requested. The decoder
hands values out as strings (see DECISIONS §3: `"id": "42"`); type coercion by
`type_oid` is not its job.

### Three semantics that must not be conflated

| Tag | Value in the JSON contract | Meaning |
|-----|---------------------------|-------|
| `'t'` | string | the value arrived |
| `'n'` | `null` **in a tuple tagged `'O'`/`'N'`** | the column really is NULL |
| `'n'` | **not "null", but "not sent"** in a tuple tagged `'K'` | a placeholder for a non-key column |
| `'u'` | the column goes into `unchanged_columns` and is **absent** from `after` | the TOAST value did not change, the server did not resend it |

The difference between the two `'n'`s is confirmed straight from the bytes: in `0019_delete.bin` the
`items` row had `title = 'Widget'`, `qty = 7` at the moment of the DELETE (the state left by
`0016_update.bin`), yet in the `'K'` tuple both arrived as `'n'`. So `'n'` under `'K'` is **not NULL**,
it is "the server did not send it". The decoder MUST tell them apart by the tuple tag, not the column tag.

---

## 8. Summary across all 31 fixtures: transactional invariants

| Tx | BEGIN | COMMIT | xid | `BEGIN.final_lsn` = `COMMIT.commit_lsn` | `COMMIT.end_lsn` (**ACK**) | Δ | `commit_timestamp` (identical in BEGIN and COMMIT) |
|----|-------|--------|-----|------------------------------------------|-----------------------------|---|--------------------------------|
| 1 | `0001` | `0004` | 737 | `0/19300D0` | `0/1930100` | 48 | `841423351314489` |
| 2 | `0005` | `0007` | 738 | `0/1930188` | `0/19301B8` | 48 | `841423351317800` |
| 3 | `0008` | `0010` | 739 | `0/1930218` | `0/1930248` | 48 | `841423351318363` |
| 4a | `0011` | `0014` | 740 | `0/1930338` | `0/1930368` | 48 | `841423351319177` |
| 4b | `0015` | `0017` | 741 | `0/19303C0` | `0/19303F0` | 48 | `841423351319762` |
| 4c | `0018` | `0020` | 742 | `0/1930438` | `0/1930468` | 48 | `841423351320257` |
| 5a | `0021` | `0023` | 743 | `0/1932DC8` | `0/1932DF8` | 48 | `841423351323979` |
| 5b | `0024` | `0026` | 744 | `0/1935470` | `0/19354A0` | 48 | `841423351324709` |
| 6 | `0027` | `0031` | 745 | `0/1935620` | `0/1935650` | 48 | `841423351326881` |

Confirmed invariants (checked on all nine pairs, not on a sample):

1. `BEGIN.final_lsn == COMMIT.commit_lsn` — always.
2. `BEGIN.commit_timestamp == COMMIT.commit_timestamp` — always, bit for bit.
3. `COMMIT.end_lsn > COMMIT.commit_lsn` — always, by exactly 48 bytes in this set.
4. `xid` grows monotonically 737 → 745, one per transaction. Transaction 6 (three DMLs)
   has **one** xid and **one** BEGIN/COMMIT pair — row messages are grouped
   by BEGIN/COMMIT boundaries, not by DML.
5. `COMMIT.flags == 0` — always.
6. `COMMIT.end_lsn` == `wal_end` from the `XLogData` envelope (per `MANIFEST.md`) — always.

**Transaction 7 (ROLLBACK) produced not a single byte.** In proto v1 a rolled-back transaction is
not decoded at all: there is no BEGIN and no equivalent of ABORT. The stage 2 test for a rollback is
written as an assertion about the absence of events; there are no byte data for it.

---

## 9. INSERT

Layout (docs, proto v1): `Byte1('I')`, `Int32` relation OID, `Byte1('N')`, `TupleData`.

The `'N'` tag is **always present** and always alone — an INSERT has no old tuple.

### `0003_insert.bin` — `users`, `bio` = NULL (47 bytes)

```
00000000: 4900 0040 014e 0004 7400 0000 0131 7400  I..@.N..t....1t.
00000010: 0000 0541 6c69 6365 7400 0000 1161 6c69  ...Alicet....ali
00000020: 6365 4065 7861 6d70 6c65 2e63 6f6d 6e    ce@example.comn
```

| off | len | bytes | field | value |
|----:|----:|-------|------|----------|
| 0 | 1 | `49` | message type | `'I'` |
| 1 | 4 | `00 00 40 01` | relation OID | `16385` (`public.users`) |
| 5 | 1 | `4e` | tuple tag | `'N'` — new tuple |
| 6 | 2 | `00 04` | column count | `4` |
| 8 | 1 | `74` | col[0] `id` tag | `'t'` |
| 9 | 4 | `00 00 00 01` | col[0] length | `1` |
| 13 | 1 | `31` | col[0] value | `"1"` |
| 14 | 1 | `74` | col[1] `name` tag | `'t'` |
| 15 | 4 | `00 00 00 05` | col[1] length | `5` |
| 19 | 5 | `41 6c 69 63 65` | col[1] value | `"Alice"` |
| 24 | 1 | `74` | col[2] `email` tag | `'t'` |
| 25 | 4 | `00 00 00 11` | col[2] length | `17` |
| 29 | 17 | `61 6c 69 63 65 40 ... 63 6f 6d` | col[2] value | `"alice@example.com"` |
| 46 | 1 | `6e` | col[3] `bio` tag | **`'n'` — NULL, no length and no data** |

47 of 47 accounted for. The last byte of the file is the `'n'` tag; nothing follows it. This is the
shortest possible check on "do not read a length after `'n'`".

### `0013_insert.bin` — `items`, a different table (32 bytes)

```
00000000: 4900 0040 084e 0003 7400 0000 0231 3074  I..@.N..t....10t
00000010: 0000 0006 5769 6467 6574 7400 0000 0135  ....Widgett....5
```

OID `16392`, `'N'`, 3 columns: `t(2)="10"`, `t(6)="Widget"`, `t(1)="5"`. No remainder.
A different OID and a different column count within the same run — a direct check that the
relation cache holds several tables at once and reads a tuple against the description of
**its own** OID.

### `0022_insert.bin` — a large TOAST value (9651 bytes)

Head of the file:

```
00000000: 4900 0040 014e 0004 7400 0000 0132 7400  I..@.N..t....2t.
00000010: 0000 0543 6172 6f6c 7400 0000 1163 6172  ...Carolt....car
00000020: 6f6c 4065 7861 6d70 6c65 2e63 6f6d 7400  ol@example.comt.
00000030: 0025 8038 6363 3336 3964 6361 3730 6437  .%.8cc369dca70d7
```

| off | len | field | value |
|----:|----:|------|----------|
| 0 | 1 | type | `'I'` |
| 1 | 4 | OID | `16385` |
| 5 | 1 | tag | `'N'` |
| 6 | 2 | ncols | `4` |
| 8..45 | | col[0..2] | `t(1)="2"`, `t(5)="Carol"`, `t(17)="carol@example.com"` |
| 46 | 1 | col[3] `bio` tag | `'t'` |
| 47 | 4 | col[3] length | `00 00 25 80` = **`9600`** |
| 51 | 9600 | col[3] value | 9600 bytes of hex text |

`51 + 9600 = 9651` — exactly the file size, no remainder.

**An inference, not confirmed by the bytes of this set:** a `'u'` marker cannot appear on an INSERT
in principle. That conclusion comes from the behaviour of the PostgreSQL reorder buffer (the value is
absent from the previous row version, so it has to be resent in full), not from anything the bytes of
the four INSERT fixtures prove: they only show that the `'u'` tag did not occur in them. The
"do not resend what did not change" TOAST optimisation applies only to UPDATE. This fixture
exists to check that the `Int32` length is read as 4 bytes and not as 2:
`0x2580` = 9600 fits in a `u16`, but reading two bytes of `00 00 25 80` would give length `0`.

---

## 10. UPDATE — three forms

Layout (docs, proto v1):

```
Byte1('U')
Int32       relation OID
[ Byte1('K') | Byte1('O') ]   — OPTIONAL, exactly one of the two, or nothing
[ TupleData ]                 — only if there was a 'K' or an 'O'
Byte1('N')
TupleData
```

The conditions from the PostgreSQL 16 documentation, verbatim:

- `'K'` — "This field is optional and is only present if the update changed data in any of
  the column(s) that are part of the REPLICA IDENTITY index."
- `'O'` — "This field is optional and is only present if table in which the update happened
  has REPLICA IDENTITY set to FULL."

**How to tell the presence of an old tuple from its absence.** Read the byte at position 5:
`'O'` (`0x4F`) or `'K'` (`0x4B`) — an old tuple follows, then the mandatory `'N'`.
`'N'` (`0x4E`) — there is no old tuple, this is already the new one. No other value should occur.
Telling them apart by message length or by counting tags is not allowed.

### Form 1: `'O'` — REPLICA IDENTITY FULL. `0006_update.bin` (87 bytes)

`UPDATE users SET name = 'Bob' WHERE id = 1;`

```
00000000: 5500 0040 014f 0004 7400 0000 0131 7400  U..@.O..t....1t.
00000010: 0000 0541 6c69 6365 7400 0000 1161 6c69  ...Alicet....ali
00000020: 6365 4065 7861 6d70 6c65 2e63 6f6d 6e4e  ce@example.comnN
00000030: 0004 7400 0000 0131 7400 0000 0342 6f62  ..t....1t....Bob
00000040: 7400 0000 1161 6c69 6365 4065 7861 6d70  t....alice@examp
00000050: 6c65 2e63 6f6d 6e                        le.comn
```

| off | len | bytes | field | value |
|----:|----:|-------|------|----------|
| 0 | 1 | `55` | message type | `'U'` |
| 1 | 4 | `00 00 40 01` | relation OID | `16385` (`users`) |
| 5 | 1 | `4f` | **old tuple tag** | **`'O'`** |
| 6 | 2 | `00 04` | ncols of the old tuple | `4` |
| 8 | 6 | `74 00000001 31` | O.id | `t(1) = "1"` |
| 14 | 10 | `74 00000005 "Alice"` | O.name | `t(5) = "Alice"` |
| 24 | 22 | `74 00000011 "alice@..."` | O.email | `t(17) = "alice@example.com"` |
| 46 | 1 | `6e` | O.bio | **`'n'` — a real NULL** |
| 47 | 1 | `4e` | **new tuple tag** | **`'N'`** |
| 48 | 2 | `00 04` | ncols of the new tuple | `4` |
| 50 | 6 | `74 00000001 31` | N.id | `t(1) = "1"` |
| 56 | 8 | `74 00000003 "Bob"` | N.name | `t(3) = "Bob"` |
| 64 | 22 | `74 00000011 "alice@..."` | N.email | `t(17) = "alice@example.com"` |
| 86 | 1 | `6e` | N.bio | `'n'` — NULL |

87 of 87 accounted for, no remainder.

Note: in the new tuple **all** columns are present, including the unchanged ones
(`id`, `email`). pgoutput sends the full new version of the row, not a delta. "What changed" is
computed by the decoder by comparing `before`/`after`; it does not follow from the protocol.

### Form 2: no tag — REPLICA IDENTITY DEFAULT, the key did not change. `0016_update.bin` (32 bytes)

`UPDATE items SET qty = 7 WHERE id = 10;`

```
00000000: 5500 0040 084e 0003 7400 0000 0231 3074  U..@.N..t....10t
00000010: 0000 0006 5769 6467 6574 7400 0000 0137  ....Widgett....7
```

| off | len | bytes | field | value |
|----:|----:|-------|------|----------|
| 0 | 1 | `55` | message type | `'U'` |
| 1 | 4 | `00 00 40 08` | relation OID | `16392` (`items`) |
| 5 | 1 | `4e` | tag | **`'N'` — the new tuple right away, there is NO old one** |
| 6 | 2 | `00 03` | ncols | `3` |
| 8 | 7 | `74 00000002 "10"` | N.id | `t(2) = "10"` |
| 15 | 11 | `74 00000006 "Widget"` | N.title | `t(6) = "Widget"` |
| 26 | 6 | `74 00000001 37` | N.qty | `t(1) = "7"` |

32 of 32 accounted for, no remainder.

**The byte at position 5 is `0x4E`, not `0x4F`/`0x4B`.** Compare with `0006_update.bin`, where the
same position holds `0x4F`. One byte is the entire difference between "there is a `before`" and
"there is no `before`". The file is exactly the same size as `0013_insert.bin` (32 bytes) and differs
from it by **two bytes only** (checked with `cmp -l`): offset 0 — message type `0x55` (`'U'`)
instead of `0x49` (`'I'`), and offset 31 — the `qty` value `0x37` (`"7"`) instead of `0x35` (`"5"`).
The layout of an INSERT and of an UPDATE-without-an-old-tuple matches byte for byte.

### Form 3: `'K'` — DEFAULT and the key changed

**Absent from the fixture set.** See "Not analysed" item 3.

### UPDATE with TOAST: `0025_update.bin` (9696 bytes)

`UPDATE users SET name = 'Caroline' WHERE id = 2;` — `bio` was not touched.

Head:

```
00000000: 5500 0040 014f 0004 7400 0000 0132 7400  U..@.O..t....2t.
00000010: 0000 0543 6172 6f6c 7400 0000 1163 6172  ...Carolt....car
00000020: 6f6c 4065 7861 6d70 6c65 2e63 6f6d 7400  ol@example.comt.
00000030: 0025 8038 6363 3336 3964 6361 3730 6437  .%.8cc369dca70d7
```

Tail (from offset `0x259E` = 9630):

```
0000259e: 6165 3931 6532 6233 6232 3534 3366 3339  ae91e2b3b2543f39
000025ae: 6566 6263 334e 0004 7400 0000 0132 7400  efbc3N..t....2t.
000025be: 0000 0843 6172 6f6c 696e 6574 0000 0011  ...Carolinet....
000025ce: 6361 726f 6c40 6578 616d 706c 652e 636f  carol@example.co
000025de: 6d75                                     mu
```

| off | len | field | value |
|----:|----:|------|----------|
| 0 | 1 | message type | `'U'` |
| 1 | 4 | relation OID | `16385` (`users`) |
| 5 | 1 | old tuple tag | **`'O'`** (REPLICA IDENTITY FULL) |
| 6 | 2 | ncols of the old tuple | `4` |
| 8 | 6 | O.id | `t(1) = "2"` |
| 14 | 10 | O.name | `t(5) = "Carol"` |
| 24 | 22 | O.email | `t(17) = "carol@example.com"` |
| 46 | 1 | O.bio tag | `'t'` |
| 47 | 4 | O.bio length | `00 00 25 80` = `9600` |
| 51 | 9600 | O.bio value | 9600 bytes — **the old tuple carries the TOAST in full** |
| 9651 | 1 | new tuple tag | `'N'` |
| 9652 | 2 | ncols of the new tuple | `4` |
| 9654 | 6 | N.id | `t(1) = "2"` |
| 9660 | 13 | N.name | `t(8) = "Caroline"` |
| 9673 | 22 | N.email | `t(17) = "carol@example.com"` |
| **9695** | **1** | **N.bio tag** | **`0x75` = `'u'` — unchanged TOAST** |

9696 of 9696 accounted for, no remainder. **`0x75` is the last byte of the file**; no length and no
data follow it. It is visible straight in the tail of the dump: `... 65 2e 63 6f 6d` (`"e.com"`) `75`.

An asymmetry that has to be understood: the **old** tuple carries `bio` in full (9600 bytes of
text), the **new** one carries a one-byte marker. Under REPLICA IDENTITY FULL Postgres MUST
resend the old row in full, TOAST included; for the new row it knows the TOAST
value did not change and does not pull it out of the TOAST table. That is exactly why the file is
9696 bytes and not 19,300 (9696 − 1 byte for the `'u'` tag + 5 bytes for a tag with a length + 9600 bytes of value).

The semantics for the JSON (DECISIONS §3): `unchanged_columns: ["bio"]`, and **there must be no
`bio` key in `after`**. Writing `"bio": null` is silent data corruption: the consumer will decide
the column was nulled out. `before.bio` meanwhile holds the full 9600 bytes.

### `0029_update.bin` — an UPDATE inside a multi-row transaction (86 bytes)

`UPDATE users SET email = 'dave2@example.com' WHERE id = 3;`

The same form as `0006`: `'U'`, OID `16385`, tag `'O'`, the old tuple
(`t"3"`, `t"Dave"`, `t"dave@example.com"`, `'n'`), tag `'N'`, the new tuple
(`t"3"`, `t"Dave"`, `t"dave2@example.com"`, `'n'`). No remainder.
Only `email` changes, yet `'O'` still carries all four columns: the tag depends
on the table's REPLICA IDENTITY, not on what exactly changed.

---

## 11. DELETE — two forms

Layout (docs, proto v1): `Byte1('D')`, `Int32` relation OID, `Byte1('K')` **or**
`Byte1('O')`, `TupleData`.

The difference from UPDATE: the tag is **mandatory**, "nothing" does not occur — the deleted row has
to be identified somehow. The conditions verbatim:

- `'K'` — "This field is present if the table in which the delete has happened uses an
  index as REPLICA IDENTITY."
- `'O'` — "This field is present if the table in which the delete happened has REPLICA
  IDENTITY set to FULL."

A DELETE has no new tuple: the message ends right after the old one.

### Form `'O'`: `0009_delete.bin` — `users`, FULL (45 bytes)

```
00000000: 4400 0040 014f 0004 7400 0000 0131 7400  D..@.O..t....1t.
00000010: 0000 0342 6f62 7400 0000 1161 6c69 6365  ...Bobt....alice
00000020: 4065 7861 6d70 6c65 2e63 6f6d 6e         @example.comn
```

| off | len | bytes | field | value |
|----:|----:|-------|------|----------|
| 0 | 1 | `44` | message type | `'D'` |
| 1 | 4 | `00 00 40 01` | relation OID | `16385` (`users`) |
| 5 | 1 | `4f` | tag | **`'O'`** — the full old tuple |
| 6 | 2 | `00 04` | ncols | `4` |
| 8 | 6 | `74 00000001 31` | id | `t(1) = "1"` |
| 14 | 8 | `74 00000003 "Bob"` | name | `t(3) = "Bob"` |
| 22 | 22 | `74 00000011 "alice@..."` | email | `t(17) = "alice@example.com"` |
| 44 | 1 | `6e` | bio | `'n'` — a real NULL |

45 of 45 accounted for, no remainder.

`name = "Bob"` is the state of the row after `0006_update.bin`, not the original `"Alice"`.
The old tuple is the version of the row **at the moment of deletion**.

### Form `'K'`: `0019_delete.bin` — `items`, DEFAULT (17 bytes)

```
00000000: 4400 0040 084b 0003 7400 0000 0231 306e  D..@.K..t....10n
00000010: 6e                                       n
```

| off | len | bytes | field | value |
|----:|----:|-------|------|----------|
| 0 | 1 | `44` | message type | `'D'` |
| 1 | 4 | `00 00 40 08` | relation OID | `16392` (`items`) |
| 5 | 1 | `4b` | tag | **`'K'`** — key only |
| 6 | 2 | `00 03` | ncols | **`3` — not 1! there are as many columns as in the table** |
| 8 | 1 | `74` | col[0] `id` tag | `'t'` |
| 9 | 4 | `00 00 00 02` | col[0] length | `2` |
| 13 | 2 | `31 30` | col[0] value | `"10"` |
| 15 | 1 | `6e` | col[1] `title` tag | **`'n'` — a placeholder, not NULL** |
| 16 | 1 | `6e` | col[2] `qty` tag | **`'n'` — a placeholder, not NULL** |

17 of 17 accounted for, no remainder. The shortest fixture in the set.

Two observations that have to go into the tests:

1. **`ncols = 3`, not 1.** A `'K'` tuple holds an entry for *every* column of the table,
   the non-key ones are simply filled with `'n'`. The decoder MUST NOT assume that a `'K'` tuple has
   as many elements as there are key columns.
2. **`'n'` here is not NULL.** At the moment of deletion the table held `title = 'Widget'`,
   `qty = 7`. The values existed, the server just did not send them. In the JSON `before`, under
   `before_kind = "key"`, only the columns with flag `1` from RELATION belong
   (`id`); serving `title`/`qty` as `null` is a lie about the data.

### `0030_delete.bin` — a DELETE inside a multi-row transaction (46 bytes)

`'D'`, OID `16385`, tag `'O'`, 4 columns: `t"3"`, `t"Dave"`, `t"dave2@example.com"`, `'n'`.
No remainder. `email` is already the one changed in `0029_update.bin`.

---

## 12. Four cases for the stage 2 tests

These four are exactly the ones that turn into tests on `before_kind` and `unchanged_columns`.

### Case 1 — UPDATE on `users` (REPLICA IDENTITY FULL): the `'O'` marker IS there

- **Fixture:** `0006_update.bin` (87 bytes). Duplicated by `0029_update.bin`.
- **Proving bytes:** offset 5 = `0x4F` = `'O'`. Then the old tuple over 4 columns
  (offsets 6..46), then offset 47 = `0x4E` = `'N'` and the new tuple.
- **Expectation:** `before_kind = "full"`, `before` = all 4 columns
  (`id="1"`, `name="Alice"`, `email="alice@example.com"`, `bio=null` — a real NULL),
  `after` = 4 columns with `name="Bob"`, `unchanged_columns = []`.
- **Negative check:** the message holds **two** `TupleData`. A decoder that stops
  after the first one leaves 40 bytes unread (87 − 47); the "buffer exhausted" assertion catches that.

### Case 2 — UPDATE on `items` (DEFAULT), the key did not change: there is NO old tuple

- **Fixture:** `0016_update.bin` (32 bytes).
- **Proving bytes:** offset 5 = `0x4E` = `'N'` (and not `0x4F`/`0x4B`). The first and
  only `TupleData` starts at offset 6. The message ends at offset 31,
  right after the last column of the new tuple.
- **Expectation:** `before_kind = null`, `before = null`,
  `after = {id:"10", title:"Widget", qty:"7"}`, `unchanged_columns = []`.
- **Why:** `UPDATE items SET qty = 7 WHERE id = 10` did not touch `id`. Under REPLICA
  IDENTITY DEFAULT, `'K'` appears only if key columns changed; here they did
  not — so there is no old version of the row at all.

### Case 3 — DELETE on `items` (DEFAULT): the `'K'` marker, key filled in, the rest `'n'`

- **Fixture:** `0019_delete.bin` (17 bytes).
- **Proving bytes:** offset 5 = `0x4B` = `'K'`; offsets 6..7 = `00 03` (three columns);
  offsets 8..14 = `74 00 00 00 02 31 30` (`id = "10"`); offset 15 = `0x6E`;
  offset 16 = `0x6E` — both without a length, and the file ends there.
- **Expectation:** `before_kind = "key"`, `before = {id: "10"}`, `after = null`,
  `unchanged_columns = []`.
- **Mandatory negative check:** `before` **MUST NOT** contain
  `title: null` / `qty: null`. The real values at the moment of deletion were `"Widget"` and
  `"7"` — the server simply did not send them.

### Case 4 — UPDATE with TOAST: the `'u'` marker in place of the `bio` value

- **Fixture:** `0025_update.bin` (9696 bytes).
- **Proving bytes:** offset **9695** = `0x75` = `'u'` — **the last byte of the file**,
  with neither an `Int32` length nor data after it. The position is the fourth column (index 3)
  of the **new** tuple, that is, `bio` per the `RELATION` for OID 16385. In the **old** tuple the same
  column at offset 46 has tag `'t'` with length `9600` (offsets 47..50 = `00 00 25 80`).
- **Expectation:** `before_kind = "full"`, `before.bio` = the full 9600-character string,
  `after` = `{id:"2", name:"Caroline", email:"carol@example.com"}` **with no `bio` key**,
  `unchanged_columns = ["bio"]`.
- **Mandatory negative check:** `after.bio` is not `null` and is not an empty
  string — the key is simply not there. Telling `'u'` (`0x75`) apart from `'n'` (`0x6E`) and from `'t'`
  (`0x74`) is three separate branches, not two.
- **The shape of `after` was settled by a decision, not derived from the bytes.** The bytes give only
  the fact "the server did not send the value"; what to write in the JSON is a matter of contract, and in
  `DECISIONS.md` it read differently in two places for a while: the output contract (§3) required the
  key to be absent from `after`, while the wording of Q15 read as a prohibition on omitting the column.
  The discrepancy was settled by a ruling: **the column is absent from `after` AND named in
  `unchanged_columns`**. The rejected option was *silent* omission — without the list;
  the list is what restores the distinction between "not sent" and "equal to null" without mixing
  metadata into the data. The wording of Q15 in `DECISIONS.md` has been brought into line.
  The stage 2 test is to lean on that pair (`after` without the key + the name in `unchanged_columns`),
  and not on one half of it.

---

## 13. Checklist for the stage 2 decoder

Extracted from the reading; every item corresponds to a real trap in the bytes above.

1. The message type is byte 0. Do not look for tags by scanning the buffer (`'O'` is ambiguous).
2. All integers are big-endian. `atttypmod` and the column value length are signed `i32`.
3. C strings: read up to `0x00`, consume the zero byte. An empty C string (the `pg_catalog`
   namespace) is one byte `0x00`, not zero bytes.
4. `'n'` and `'u'` have no length field. `'t'` and `'b'` do.
5. The tuple tag is optional in UPDATE (`'O'`/`'K'`/nothing) and mandatory in DELETE (`'O'`/`'K'`).
6. `TupleData` always holds an entry for every column of the table, even in a `'K'` tuple.
7. `'n'` under a `'K'` tag means "not sent"; under `'O'`/`'N'` it means "NULL". Different things.
8. `'u'` → the column goes into `unchanged_columns`, there is no key in `after`.
9. Column names come only from `RELATION`, by index. A repeated `RELATION` for an **already
   known** OID is legal (DDL, a replica identity change, a publication change) and MUST
   **overwrite** the cache entry — it is not an error, not a reason to fail, and not a reason to ignore it.
   Dropping the whole cache is a separate case, on reconnect (Q19).
10. Acknowledge `COMMIT.end_lsn` (offset 10), not `commit_lsn` (offset 2).
11. `commit_timestamp` is microseconds since **2000-01-01**, not since 1970.
12. After a message is read, the buffer MUST be exhausted exactly. A non-zero remainder is
    a parse error, not "extra bytes". Checked: all 31 fixtures leave a remainder of 0.
13. An unknown first byte (`T`/`Y`/`O`/`M` or any other) — do not panic: either skip it
    deliberately, or return a typed error. Silently ignoring it is not allowed.

**What is already closed and what is left.** The decoder (`src/postgres/pgoutput.rs`) landed in
stage 1 and closes items 1–4, 6, 9–13 of this checklist for `BEGIN` / `RELATION` /
`INSERT` / `COMMIT`. Items 5, 7, 8 were open for stage 2 — all of them about
what stage 1 did not have: `UPDATE`, `DELETE`, the before-image (`before_kind`), and turning the
unchanged-TOAST marker (`'u'`) into `unchanged_columns`. Stage 2 closed all three:

- **Item 5** (the tuple tag is optional in UPDATE and mandatory in DELETE) — closed by
  `src/postgres/pgoutput.rs::decodes_update_without_an_old_tuple` (optionality
  on UPDATE) and `src/postgres/pgoutput.rs::delete_without_a_tuple_tag_is_an_error`
  (mandatoriness on DELETE).
- **Item 7** (`'n'` under `'K'` means "not sent", under `'O'`/`'N'` it means NULL) —
  closed by `src/transaction.rs::key_tuple_omits_columns_the_server_did_not_send`.
- **Item 8** (`'u'` → the column goes into `unchanged_columns`, there is no key in `after`) —
  closed by `src/transaction.rs::full_tuple_keeps_real_nulls_and_reports_unchanged_toast`.

---

## 14. Not analysed

Eleven items. An honest list of what the bytes do not confirm. Every item is a
potential test that **MUST NOT** be written from this document as if it were fact.

1. **`flags` in COMMIT.** `0x00` in all 9 fixtures. The PostgreSQL 16 documentation declares
   the field as `Int8(0)` "currently unused" and describes no non-zero value.
   What to do when `flags != 0` is unknown. The field MUST be **read** (otherwise the
   offsets shift), but there is nothing to interpret it with. A "flags == 0" test would be a test on this
   particular data set, not on the protocol.

2. **The `'b'` (binary) tag was never observed.** The `binary` option was not requested in
   `START_REPLICATION`, so all values arrived as text. The `'b'` layout (`Int32` length +
   data) is taken **from the documentation** and is not confirmed by bytes. If stage 2 writes a
   `'b'` branch it will be unverified; returning a typed error
   "binary format not supported" is safer.

3. **An UPDATE with a `'K'` tag is absent from the set.** The only `'K'` is in a DELETE
   (`0019_delete.bin`). The case "REPLICA IDENTITY DEFAULT + the key changed" is not in
   `scripts/gen-fixtures.sql`: `UPDATE items SET qty = 7 WHERE id = 10` does not touch the key.
   So the branch "UPDATE → `'K'` → an old tuple with the key only → `'N'`" will be written
   **from the documentation, not from the bytes**. The layout per the docs is unambiguous and matches
   form `'O'` (tag, then `TupleData`, then `'N'`, then `TupleData`), but there is no fixture
   for it. If stage 2 wants a test, the fixture has to be captured
   (`UPDATE items SET id = 11 WHERE id = 10`), or the bytes assembled by hand and explicitly marked
   as synthetic.

   **Closed.** Task 4 ran exactly `UPDATE items SET id = 11 WHERE id = 10` against
   a live PostgreSQL (test
   `changing_a_key_column_produces_a_key_only_before_image` in `tests/integration.rs`).
   The server sent `before_kind: "key"`, a `before` with only the `id` column (`"10"`), and
   an `after` with all three columns (`id: "11", title: "Widget", qty: "5"`) — that is,
   exactly what the synthetic byte array above predicted: the `'K'` tag, an entry for
   every column in the old tuple, and non-key columns absent from `before` rather than
   `null`. The item stays on the list: it documents that the stage 0 capture itself
   still has no such fixture.

4. **The xid prefix for streamed transactions was not observed.** `proto_version = 1`,
   `StreamingMode::Off`, so `R`/`I`/`U`/`D` carry no `Int32 xid` after the type byte. Every
   offset in this document is valid **only for proto v1 without streaming**. On a
   move to v2 all of them shift by 4, and the whole fixture set becomes invalid.
   That is a limit on scope, not an error.

5. **The message types `T` (Truncate), `Y` (Type), `O` (Origin), `M` (Message) were not
   observed.** There is no `TRUNCATE` in `gen-fixtures.sql`; there are no user-defined types;
   `pg_logical_emit_message` was not called; replication is not cascading, so no Origin
   arrives. Their layout was not analysed in this document at all. The stage 2 decoder must
   manage not to break on them — but there is nothing to test those branches against.

6. **`atttypmod` equals `-1` (`ff ff ff ff`) in all seven columns of both tables.**
   Every type in the schema is `bigint`, `text`, `int`, and none of them has a length modifier. So
   **there is not a single positive `atttypmod` in the set**, and the "read it as `u32`" mistake
   (which would give `4294967295` instead of `-1`) is **not** caught by the fixtures — it is caught only
   by the test asserting exactly `-1`. The signedness is taken from the documentation
   (`Int32 - Type modifier of the column (atttypmod)`); telling `-1` and
   `4294967295` apart empirically on these data is impossible.

7. **An empty namespace was not observed.** Documentation: "Namespace (empty string for
   `pg_catalog`)". Both tables are in `public`, so the encoding of an empty C string
   (one byte `0x00`) is not confirmed by bytes.

8. **The documentation's caveat "for each column (except generated columns)" was not checked.**
   Neither `users` nor `items` has generated or dropped columns. In all
   11 row fixtures `TupleData.ncols` matched `RELATION.ncols`, so the mapping
   "the i-th tuple column = the i-th RELATION column" is confirmed **only for tables without
   generated and dropped columns**. It MUST NOT be relied on as a general invariant;
   the right thing is to compare `TupleData.ncols` against `RELATION.ncols` and fail on a mismatch.

9. **The value `Δ = 48` bytes between `commit_lsn` and `end_lsn` is not a protocol invariant.**
   It is the same in all nine fixtures, but that is the size of a commit WAL record in one particular
   PostgreSQL 16 build under one particular configuration. The decoder must not lean on it;
   the only thing that can be leaned on is that `end_lsn > commit_lsn`.

10. **A repeated `RELATION` for an already known OID was not observed.** The whole run had no
    DDL, no replica identity change, and no publication change, so every table
    sent RELATION exactly once. The rule "a repeated RELATION is legal and MUST
    replace the cache entry" (§6, §13 item 9) is taken from the base spec and from the behaviour of pgoutput,
    and is **not confirmed by the bytes of this set**: there is no fixture with two RELATION messages for one OID.
    The stage 2 test for replacing the relation cache entry will have to be built on synthetic bytes
    (two `R` messages with one OID and different column sets) and marked as synthetic.

    **Closed.** Task 4 ran `ALTER TABLE items ADD COLUMN note TEXT` in the middle of
    replication against a live PostgreSQL (test
    `schema_change_resends_relation_and_the_cache_takes_the_new_one` in
    `tests/integration.rs`). The first `INSERT` (before the DDL) gave an `after` with three columns
    (`id`, `title`, `qty`); the second `INSERT` (after the DDL, with the new column) gave an `after` with
    four columns, including `note: "hello"` — that is, the server really did
    resend `RELATION` for the same OID, and the cache replaced the entry rather than keeping the
    old one or failing. The item stays on the list: it documents that the stage 0 capture
    itself still has no repeated `RELATION`.

11. **Zeroed `wal_start`/`wal_end` on RELATION messages.** Both RELATION messages arrived with
    `0/0` in the `XLogData` envelope, which contradicts a reading of the PostgreSQL sources. Described
    in detail in `tests/fixtures/MANIFEST.md`, section "Note: RELATION messages and
    `wal_start`/`wal_end` = `0/0`", and marked there as **unconfirmed and
    not localised**. It was not investigated as part of Task 5: WAL positions are not part of the
    payload, and this document reads the payload. The operational conclusion does not change —
    do not use the LSN from a RELATION envelope for anything.

---

## 15. How this reading was verified

1. Every message was read left to right against the PostgreSQL 16 documentation, field by field,
   recording the offset and length. The offsets in the tables above are actual, not
   assumed: they were obtained by summing the lengths of the fields already read.
2. **The buffer-exhaustion check.** For all 31 fixtures the buffer remainder after the last field
   is read is zero. This is the strongest check available: any error in a
   field size, in byte order, or in the presence or absence of a length field yields a non-zero
   remainder or an out-of-bounds read. Not a single discrepancy.
3. **A check on semantics, not only on structure.** The parsed values were cross-checked against the
   SQL in `scripts/gen-fixtures.sql`: `"Alice"` → `"Bob"` → deleted; `qty` `5` → `7`;
   `"Carol"` → `"Caroline"`; `email` `dave@` → `dave2@`. All matched. A structurally
   correct but semantically shifted reading would have fallen apart here.
4. **The replica identity check as the brief's control point**: `0x66`/`'f'` for `users`,
   `0x64`/`'d'` for `items` — both bytes turned out to be at position 18, computed from the lengths of the
   preceding fields, and not found by searching the buffer.
5. **The timestamp epoch check** — §5, with a counterexample for the 1970 epoch.
6. The parsing script was one-off and is not committed (a condition of Task 5).
