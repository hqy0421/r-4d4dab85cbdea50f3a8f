# Durability and crash semantics

## What pg-cdc guarantees

**At-least-once delivery.** Every event committed in the source database
will appear in the sink at least once, even across pg-cdc process
crashes. Events are not deduplicated within a flush window — see
"Duplicates" below for the recovery footprint.

## How it works

The durability boundary is the `confirmed_flush_lsn` of the Postgres
replication slot. Postgres retains WAL at and beyond this position; it
re-sends everything past it on reconnect.

Two coordinated records define durability:

1. **The sink manifest** — what is *readable* downstream. A delta counts
   only once its epoch appears in `manifest.json`.
2. **The local SQLite batch journal** (`state.path`, default
   `.pgcdc/state.db`) — a crash-safe ledger mapping each epoch to its WAL
   window (`start_lsn`/`end_lsn`), its per-table uploaded files, and an
   `open`/`committed` status.

The streamer advances `confirmed_flush_lsn` as follows:

1. WAL events flow into pg-cdc's in-memory buffer.
2. A flush first **journals the batch** (epoch + start/end LSN, status
   `open`) in SQLite *before* touching the sink.
3. Each per-table delta Parquet is uploaded to a fixed, epoch-derived path;
   every successful upload is recorded in the journal. Re-uploading the
   same epoch overwrites the object in place, so replay is idempotent.
4. The manifest is written to the sink — the downstream visibility
   boundary.
5. **Only after the manifest write succeeds** is the epoch marked
   `committed` in SQLite and the durable high-water LSN advanced. The
   streamer reports that LSN through its `durableLSN` callback, and the
   next standby status update (every 10s by default) sends it to Postgres,
   which then advances `confirmed_flush_lsn`.

Crucially, the standby update reports the **flushed** position — never
the position Postgres has merely sent. The replication loop no longer
tracks or reports the server WAL end for feedback. Underreporting is safe
(Postgres keeps the WAL); overreporting causes Postgres to drop WAL
pg-cdc still needs after a crash.

## Startup reconciliation

On `start`, the journal is reconciled against the manifest before
streaming begins, and the actual replication start position is derived
from it:

| Journal state | Reconciliation action | Resume position |
|---|---|---|
| No journal rows | Fresh start | Manifest `init_lsn` |
| Latest epoch `committed` | Clean boundary | Journal `confirmed_lsn` |
| Latest epoch `open`, all files readable | Reuse the uploaded deltas, complete the manifest and close the journal without re-reading WAL | Batch `end_lsn` |
| Latest epoch `open`, some files missing/unreadable | Keep the same epoch number, replay from the batch start, overwrite partial objects, then commit | Batch `start_lsn` |
| Manifest already shows an epoch the journal still marks `open` (crash between manifest write and journal commit) | Skip the manifest rewrite, close the journal only | Batch `end_lsn` |

The reconciler refuses to start if a clean-boundary resume position is
ahead of the slot's `confirmed_flush_lsn` (that would skip data). During
open-batch replay the same condition is advisory: an unconfirmed flush can
legitimately sit ahead of the slot because the crashed process's standby
update may never have reached Postgres, and replaying the gap is the
recovery mechanism.

## Crash scenarios

### Mid-flush crash

Process is killed (`kill -9`, OOM) after some delta files have been
uploaded but before `WriteManifest` completes:

- The SQLite journal retains an `open` epoch holding the exact batch LSN
  window and every file that did land.
- `confirmed_flush_lsn` was last advanced after the **previous** committed
  batch — standby feedback never overshoots the durable position.
- On restart, reconciliation finds the open epoch. Readable files are
  reused; missing ones are rewritten under the **same epoch number** (S3
  PutObject / filesystem writes are atomic overwrites), so no orphan
  epoch is left and the batch is never skipped.
- Postgres re-sends any WAL still above the slot position; the
  redelivered window is bounded by the open batch's start LSN.

**No data loss.** Events committed in Postgres reach the sink eventually.

### Manifest write fails

Same outcome as a mid-flush crash:

- The epoch stays `open` in the journal; the durable LSN is not advanced.
- `confirmed_flush_lsn` stays at the previous batch's position.
- Reconnect replays everything since that point and reuses or overwrites
  the batch's files in place.

### Manifest written, journal commit fails

A narrow window where `WriteManifest` succeeded but the process died
before the journal's `CommitEpoch`:

- Reconciliation detects the epoch is already visible in the manifest and
  closes the journal without rewriting the manifest or allocating a new
  epoch. No duplicate delta is produced.

### Process restart with no crash (graceful shutdown)

On `SIGTERM`/`SIGINT`, the streamer cancels the replication loop and
drains the buffer through a detached 30s context (the parent context is
already cancelled), then writes the manifest, commits the epoch and
advances the durable LSN. The next start resumes at the committed
position with no replay.

### Slot dropped

`pgcdc_slot_active` going to `0` is the alert signal. See
[`runbooks/disaster-recovery.md`](runbooks/disaster-recovery.md) §1.

## Duplicates

Replay can produce duplicate writes within the **flush window of the
crash**. Consumers should treat the sink as at-least-once and key off
the primary key + a recency field (e.g. `__op_ts` or
`LatestDeltaEpoch`) when exactly-once semantics matter.

For Iceberg sinks, snapshot-level idempotency is preserved: each flush
is a single Iceberg commit, and a re-flush appends a new snapshot
rather than mutating the previous one. Time-travel reads to tags
created before the crash see the pre-crash data unchanged.

## Latency vs. durability trade-off

The flush interval (`flush.interval_sec`, default 10s) sets both the
flush cadence and the **maximum replayable window** on crash. Tightening
flush_interval reduces replay-on-crash but increases sink-write
overhead and may produce small Parquet files. Operators tune to match
their freshness SLO and replay tolerance.

The standby-status interval (10s, hardcoded) sets the maximum delay
between a successful flush and Postgres learning about it. After a
flush, up to 10s of WAL Postgres could otherwise release stays around.
This is intentional — Postgres can't release WAL until pg-cdc says it's
flushed, and we'd rather hold extra WAL briefly than risk overreporting.

## What's not durable

- **Run artifacts** (`events.ndjson`, `state.json`) are advisory. They
  live on local disk for the daemon's lifetime; a restart begins fresh
  artifacts in a new run-id directory. Audit retention is via the
  `audit=true` slog stream, not run artifacts.
- **Compaction state** (in `internal/state/state.go`) is durable
  per-table but optional — compaction is idempotent over delta files.
- **Refs in `refs.json`** are written after the manifest. A crash
  between manifest write and refs write means the new flush is in the
  manifest but not yet in any ref. The next flush re-derives all refs
  from the current manifest, so consumers see a one-flush gap before
  catching up.

### Manifest vs. journal ordering

The manifest is the sink-visible record; the SQLite journal is the local
crash ledger. A flush journals the batch and each upload *before* the
manifest write, and only marks the epoch committed *after* the manifest
write succeeds. Every ordering of process death within that sequence is
recoverable via startup reconciliation — see the table above. There is no
compare-and-swap (CAS) manifest protocol in this codebase; concurrent
writers against the same sink prefix are not supported and the single
daemon process owns the manifest.

## Verified scenarios

The recovery suite in `internal/stream_recovery_test.go` runs the real
filesystem sink and SQLite journal against a scripted fake source,
simulating process restarts across the durability boundary.

| Scenario | Outcome | Test |
|---|---|---|
| Clean flush | Manifest advances, epoch `committed`, durable LSN equals batch end; standby feedback never overshoots | `TestCleanRunCommitsAndAdvancesLSN` |
| Restart after clean shutdown | New `Stream` starts at the previous batch end, not `init_lsn` | `TestRestartResumesAtCommittedLSN` |
| Manifest write fails after deltas land | Flush is fatal; epoch stays `open` with the exact LSN window; restart reuses files and never skips the batch | `TestManifestCommitFailureReplaysBatch` |
| Slot already caught up when recovering | Reused epoch only, zero new uploads, no duplicate epoch | `TestManifestFailureWithCaughtUpSlotReusesOnlyEpoch` |
| Some delta uploads missing on restart | Same epoch number replayed from the batch start; missing object rewritten, then committed | `TestOpenBatchWithMissingFileIsReplayed` |
| Manifest written, journal commit lost | Reconciliation sees the visible epoch and closes the journal without a duplicate write | `TestManifestVisibleButJournalOpenClosesCleanly` |
| Resume position ahead of the slot | Fatal refusal on a clean boundary (would skip data); advisory during open-batch replay | `TestStartAheadOfSlotIsFatal` |
| Per-batch file ledger | Every upload is recorded with its epoch, table and remote path | `TestJournalEpochFilesTrackUploads` |

Out of scope for the unit suite (belongs in deployment smoke tests):

- Real Postgres replay after `pkill -9` — needs a live cluster; verify
  `confirmed_flush_lsn` matches the previous committed batch.
- Slot loss recovery — check the replication slot's `active`/`confirmed_flush_lsn`.

> **Note:** earlier revisions of this document described `If-Match` CAS
> manifest writes, a CAS conflict circuit breaker, `refs.json` and a
> `internal/chaos_test.go` suite. Those mechanisms are not present in this
> codebase; the SQLite batch journal and startup reconciliation above are
> the implemented durability protocol.

## Related Prometheus metrics

| Metric | What it tells you |
|---|---|
| `pgcdc_replication_lag_seconds` | Approx seconds between Postgres tip and pg-cdc's confirmed flush position |
| `pgcdc_slot_active` | 1 when slot exists and is being consumed; 0 when slot loss happened |
| `pgcdc_sink_errors_total` | Sink upload failures — flushes that didn't reach the sink |

A spike in `pgcdc_sink_errors_total` without a corresponding spike in
`pgcdc_replication_lag_seconds` means flushes are failing but the slot
still believes them durable — investigate immediately.
