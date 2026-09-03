# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Entries name the decision they came from; the full reasoning, including the alternatives
rejected, is in [DECISIONS.md](DECISIONS.md).

## [Unreleased]

### Documentation

- `DECISIONS.md` was restructured. The thirty-six decisions used to live in five markdown
  tables, one decision per cell, which put four multi-page arguments on single 4,000-plus
  character lines. Each decision is now its own linked section, preceded by an index. The
  text itself is unchanged, word for word.
- The evidence behind Q33, Q35 and Q36 — live reproductions, measurements, and
  walk-throughs of the mechanism each one closed — moved to
  [docs/decision-notes.md](docs/decision-notes.md), leaving the decision and its reasoning
  in the log. Moved verbatim, not summarized.
- This changelog was added.

## [0.1.1] — 2026-09-03

### Changed

- **`--max-transaction-events` now rejects any value above `u32::MAX` (4294967295) at
  startup, with exit code 2.** In 0.1.0 the flag had no upper bound. The per-transaction
  `event_index` is a `u32`, so a limit above `u32::MAX` allowed a buffer whose index could
  not be represented — the only thing standing in the way was the host running out of
  memory first, which is an accident of the machine rather than a property of the code. A
  wrapped index puts two distinct events on the same `(lsn, event_index)` deduplication
  key, and does it silently. Refusing the configuration is the loud version of the same
  problem.

  **If you pass a value above 4294967295, 0.1.1 will refuse to start where 0.1.0 started.**
  Values at or below `u32::MAX` behave exactly as before, the default (100000) included.
  See [Q36](DECISIONS.md#q36----max-transaction-events-is-bounded-at-u32max).

### Added

- `--version` and `-V` report the version. In 0.1.0 the binary answered
  `error: unexpected argument '--version' found` and exited 2, so an installed copy could
  not say which version it was. The value comes from the crate version, so the two cannot
  drift apart.
- An install-from-tag command in the README:
  `cargo install --git https://github.com/tarodo/pgcdc --tag v0.1.1`.

## [0.1.0] — 2026-09-03

First release. A minimal Change Data Capture engine for PostgreSQL: it reads logical
replication over the `pgoutput` protocol and emits normalized JSON Lines.

### Added

- Decoding of `BEGIN`, `COMMIT`, `RELATION`, `INSERT`, `UPDATE`, `DELETE` and `TRUNCATE`.
- Whole transactions only, emitted on commit — a rolled-back transaction never reaches the
  output.
- A WAL position is acknowledged only after the configured sink's barrier succeeds; for
  the file sink that barrier includes `fsync`. Stdout is best-effort and is excluded from
  the no-loss guarantee.
- Output to stdout, or to a file with `fsync`.
- Slot advancement on an idle publication, so unrelated write traffic cannot pin the WAL.
- Reconnect with exponential backoff, clean shutdown on `SIGTERM` / `SIGINT`, and a
  non-zero exit on anything a retry cannot fix.
- A pre-flight refusal of a slot PostgreSQL has already invalidated, before streaming
  rather than after.

### Output contract

The deduplication key is **`(lsn, event_index)`**. `lsn` alone is not enough: PostgreSQL
packs several rows into one WAL record for a bulk `COPY`, and a single `TRUNCATE` naming
several tables becomes several events — in both cases the events share a position. A
consumer merging several PostgreSQL clusters must add its own source identifier. See
[Q35](DECISIONS.md#q35--the-key-is-lsn-event_index-lsn-alone-never-sufficed).

[Unreleased]: https://github.com/tarodo/pgcdc/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/tarodo/pgcdc/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tarodo/pgcdc/releases/tag/v0.1.0
