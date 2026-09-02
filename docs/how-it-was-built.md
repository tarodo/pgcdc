# How pgcdc was built

The [README](../README.md) gives this one paragraph. This is the long version: what was
decided before any code existed, how each stage was checked, and the failures the checking
actually found.

---

## The specification

I wrote a short prompt describing the system I wanted — a minimal PostgreSQL CDC engine
reading `pgoutput` over logical replication — and had a language model draft a full
specification from it. I read [docs/spec.md](spec.md) end to end and accepted it as binding
before writing a line of implementation code: a contract to build against, not a draft to keep
rewriting to match whatever got built.

That constraint held. The spec was never edited to match the implementation. Every place I
needed to depart from it — because it was wrong, silent on a question, or contradicted itself —
became a numbered, justified entry in [DECISIONS.md](../DECISIONS.md) instead of a quiet
rewrite of the source document. By the end there were thirty of them (`Q1`–`Q30`), each
recording the decision, the reasoning, and the alternative I rejected.

Seven places in the spec needed correction outright: an assumption that a ready-made
replication library existed when none did; an `fsync`-per-commit design that caps throughput
at roughly a hundred transactions a second; treating stdout as if it could offer durability;
a logging plan that would print a thousand lines a second under normal load; a rollback test
that was mislabeled — it can't test our code, since logical decoding physically never delivers
a rolled-back transaction, so it needed renaming to say honestly what it actually tests: our
understanding of the protocol; TOAST handling deferred to a later phase it can't actually wait
for, because the protocol sends the marker in the MVP regardless; and a
persistent checkpoint file, which reintroduces exactly the second source of truth the rest of
the design was built to avoid. Each correction is recorded in DECISIONS.md's corrections
section, tied to the decision that fixed it.

---

## Testing the ground before writing a decoder

Before committing to any of this, I needed to know whether the transport crate I intended to
build on could actually honor the project's central rule — never acknowledge a WAL position
before the sink has confirmed it durable. I didn't take the crate's documentation on faith. I
ran four measured probes against a live PostgreSQL instance and read the crate's own source
for what the docs didn't answer, and wrote up the findings in
[docs/spike-findings.md](spike-findings.md) before stage 1 started.

The verdict was "fit, with reservations." Three of the four probes confirmed the invariant was
reachable: the crate never advances `confirmed_flush_lsn` on its own, it advances it to
exactly the position acknowledged, and a connection drop surfaces as a visible error rather
than a silent reconnect. The fourth probe found a real defect: pointed at a slot name that
didn't exist, the crate created one at the current WAL position and kept running — silently
discarding every row committed before that moment. That finding became a mandatory pre-flight
check (a slot must already exist, or the process refuses to start) rather than a request for
the transport crate to change.

The spike also produced a list of five crate methods that must never be called —
`next_event_with_retry`, `check_connection_health`, `into_stream`, `stream`, and
`for_each_event` — because each leads into an internal reconnect path that resumes from the
last *received* position rather than the last *durable* one, silently skipping WAL that was
never acknowledged. Only the raw, low-level read is allowed; the reconnect itself is written by
hand.

The spike raised, and a later source-level audit confirmed, a subtler trap: the crate picks
different internal code depending on whether the tokio runtime is single- or multi-threaded,
so a test built on the default single-threaded runtime silently exercises code the production
binary never runs. Every integration test now pins the multi-threaded runtime explicitly
because of that finding — and the write-up of the finding itself needed a correction after an
earlier version of it overstated what had actually been proven: a small, self-contained case
of the same failure mode described further down, where a note about a mechanism outlives its
accuracy.

---

## Thirty decisions, six stages

Development went as a vertical slice — spike, then byte-level fixtures frozen from real
protocol traffic, then test-driven implementation stage by stage (`Q8`) — rather than building
the decoder bottom-up for weeks with no proof the connection even worked. Six stages: the
spike itself; an end-to-end insert reaching JSON on stdout; the full decoder for update,
delete, and TOAST; correct acknowledgement behind a real `fsync` barrier; reconnect and
failure-mode resilience; and a wrap-up measured against the spec's own "Definition of Done"
checklist.

At every open question — and there were many the spec left unaddressed or got wrong — I made
the call myself and recorded why, along with what I rejected and why. A few examples: no local
checkpoint file at all, because the replication slot's own `confirmed_flush_lsn` is the only
position that has to survive a restart, and a second copy of it is a second thing that can
drift out of sync with it (`Q4`); TOAST columns the server didn't resend are named explicitly
in an `unchanged_columns` field, rather than left out of `after` silently — indistinguishable
from a column dropped from the schema — or written into `after` as `null` — indistinguishable
from a genuine null value; either shortcut lets a consumer overwrite a real value with nothing
(`Q15`); and durability is a property of the sink, not a global switch — the file sink fsyncs
before acknowledging, stdout acknowledges after a plain write and flush and logs a startup
warning that it cannot promise more, because refusing to acknowledge at all would just stall
the slot and grow the WAL forever (`Q6`).

---

## Review as an adversarial process

Every stage ended with a review whose standard was not "does the suite pass" but "does the
suite catch a deliberate lie." I broke the code on purpose — deleted a feature, swallowed an
error, sent the wrong value on the wire — and watched whether anything went red. Over the
course of the project that technique found a real gap five times.

The three sharpest examples landed together at the very end, each against a central claim of
the project, and each left the entire suite — 168 tests, at that point in the project's
history — green:

- deleting the periodic metrics-reporting block entirely;
- replacing the sink write call with one that silently discards a failure
  (`let _ = sink.write_transaction(&tx).await;` instead of `.await?;`) — a real, silent loss of
  data on the file sink;
- acknowledging the position the process had *received* instead of the position it had
  *durably written*, leaving the slot running tens of megabytes ahead of what was actually
  safe.

The last one is the most instructive, because the test that was supposed to catch it read the
process's own internal counter — its own record of what it intended to do — rather than what
it had actually sent to the server. A counter that records intent proves intent, but never
consequence.

The technique has one trap of its own: cargo decides what to rebuild by comparing file
modification times, so restoring a deliberately broken file without also touching its
timestamp leaves the previous, already-compiled binary in place. The test suite then passes
without the restored code ever having been rebuilt, and the mutation looks caught when nothing
ran at all. The Dockerfile has the same trap in a different shape, where a `COPY` that
preserves an old timestamp lets the build skip recompiling the change. A suspiciously instant
test run right after restoring a mutation is now treated as a symptom to investigate, not as
luck.

A smaller defect surfaced the same way. `clap` prints environment-variable values into
`--help` text, which meant the database password was leaking into help output. Fixing that
with `hide_env_values` wasn't the end of it: clap also echoes back *rejected* values in its
error text, which meant connection-string parsing had to be rewritten so that it never rejects
the string in the first place, in any way that would reprint it.

---

## The defect the spec caught that the tests didn't

The most serious defect the project produced was not found by mutation testing at all. It was
found by reading the spec's own acceptance checklist line by line, at the wrap-up stage,
rather than by trusting a suite that had been green throughout — 145 tests, at that point.

Item 14 of that checklist requires a non-zero exit code when the replication slot is unusable.
Walking it against the running code showed the process didn't do that: pointed at a slot the
server had invalidated (`SQLSTATE 55000` — its WAL already gone) or one carrying a foreign
output plugin (`SQLSTATE 22023`), it logged the failure, waited, and tried again — forever,
backoff capped at 30 seconds, looking perfectly healthy to anything watching it from outside.
The bug was a category error made silently: a server that stops answering and a server that
answers with an explicit refusal are different situations, and only the first is cured by
retrying. The fix made a server-issued refusal fatal while leaving genuine transport failures
on the existing retry path, and the split follows the transport crate's own error
classification, verified against its source — not a guess at matching error text, which a
localized `lc_messages` setting on the server would have silently broken.

---

## A recurring failure mode worth naming

One category of defect showed up more than any other over the course of the project: a
comment describing a mechanism that, by the time anyone read it again, no longer matched the
code — fourteen times. Every one was caught by someone reading the code afterward, never by
whoever wrote the comment, and twice in a row the inaccurate comment sat inside the very code
being changed at that moment — describing a mechanism wrong while touching it. The three most
recent instances landed in documents written specifically to be trusted: a specification
header with the wrong test count, a CI comment overstating how many tests actually need a
running container, and a README sentence explaining a throughput number with a figure quietly
pulled from a different metric. Whoever changes a mechanism is the last person able to notice
that a description of it has stopped matching — which is also why this document exists as a
separate, deliberately bounded account rather than an ever-expanding README section nobody
re-reads in full once it's grown long.

---

## Whose decisions these were

None of the above was generated and accepted wholesale. The specification was drafted by a
language model, on request, from a prompt I wrote; the thirty-two decisions that followed, the
six-stage plan, the pre-flight guard the spike demanded, what got deliberately broken at each
review, and the fix for every defect described here were mine. The model was a tool used at
each of those points — to draft the spec text, to help write and review code — the way a
compiler or a fuzzer is a tool: it can execute a request precisely and still be wrong about
everything nobody asked it to check. Posing the task, running the experiments that grounded
each decision, weighing the alternatives, and signing off on the result stayed with the person
running the project.
