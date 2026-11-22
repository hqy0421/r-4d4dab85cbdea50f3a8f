package internal

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/filesystem"
	"github.com/burnside-project/pg-cdc-core/internal/adapters/state/sqlite"
	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

const testTable = "public.widgets"

func testConfig() config.Config {
	return config.Config{
		Flush: config.FlushConfig{IntervalSec: 3600, MaxRows: 2},
	}
}

func testEvent(lsn string, i int) ports.CDCEvent {
	return ports.CDCEvent{
		Table:     testTable,
		Op:        "I",
		Row:       map[string]any{"id": string(rune('a' + i)), "name": "n"},
		LSN:       lsn,
		Timestamp: time.Now().UTC(),
	}
}

// newTestSink writes a manifest.json plus base layout into a temp dir and
// returns the filesystem sink ready for streaming.
func newTestSink(t *testing.T, initLSN string) (ports.Sink, string) {
	t.Helper()
	dir := t.TempDir()
	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("filesystem.New: %v", err)
	}
	m := &manifest.Manifest{
		Version: 1,
		Source:  manifest.Source{Name: "pg_cdc_slot", InitLSN: initLSN},
		Tables: map[string]manifest.Table{
			testTable: {
				Status:           "active",
				BaseEpoch:        0,
				LatestDeltaEpoch: 0,
				Path:             testTable,
			},
		},
		Schemas: map[string]manifest.Schema{
			testTable: {Version: 1, Columns: []manifest.Column{
				{Name: "id", PgType: "text", ParquetType: "UTF8", Nullable: false},
				{Name: "name", PgType: "text", ParquetType: "UTF8", Nullable: true},
			}},
		},
		UpdatedAt: time.Now().UTC(),
	}
	if err := sink.WriteManifest(context.Background(), m); err != nil {
		t.Fatalf("seed manifest: %v", err)
	}
	return sink, dir
}

func newTestStateStore(t *testing.T) ports.StateStore {
	t.Helper()
	store, err := sqlite.New(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	// Start owns the store lifecycle and closes it on return; the shared
	// wrapper keeps the underlying DB alive across simulated restarts.
	return &sharedStateStore{StateStore: store}
}

// sharedStateStore makes Close a no-op so one SQLite database can back
// several sequential Start invocations within a single test (the real
// daemon opens exactly one store for the process lifetime).
type sharedStateStore struct {
	ports.StateStore
}

func (s *sharedStateStore) Close() error { return nil }

// runStart invokes Start with a context that is cancelled once the streaming
// side goes idle, giving the flush-on-shutdown path a chance to run.
func runStart(t *testing.T, ctx context.Context, cfg config.Config, src ports.Source, sink ports.Sink, st ports.StateStore) error {
	t.Helper()
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- Start(runCtx, cfg, src, sink, st) }()

	// Give events time to flow and flush, then shut down gracefully.
	deadline := time.After(3 * time.Second)
	tick := time.NewTicker(15 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			cancel()
			return err
		case <-deadline:
			cancel()
			select {
			case err := <-done:
				return err
			case <-time.After(3 * time.Second):
				t.Fatal("Start did not return after shutdown")
			}
		case <-tick.C:
		}
	}
}

func manifestEpoch(t *testing.T, sink ports.Sink) int64 {
	t.Helper()
	m, err := sink.ReadManifest(context.Background())
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	return m.Tables[testTable].LatestDeltaEpoch
}

// writeSeedDelta renders a small epoch-1 delta Parquet and uploads it,
// standing in for a file that landed before a manifest write failed.
func writeSeedDelta(ctx context.Context, t *testing.T, sink ports.Sink, remotePath string) error {
	t.Helper()
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return err
	}
	rows := []writer.Row{
		{btypes.ColOp: "I", "id": "a", "name": "n"},
		{btypes.ColOp: "I", "id": "b", "name": "n"},
	}
	tmp := filepath.Join(t.TempDir(), "epoch.parquet")
	if err := writer.WriteDeltaParquet(tmp, rows, m.Schemas[testTable].Columns, 1); err != nil {
		return err
	}
	return uploadFile(ctx, sink, remotePath, tmp)
}

// TestCleanRunCommitsAndAdvancesLSN is the happy path: a flush commits the
// manifest, the journal records the epoch, and durable feedback never
// exceeds the committed batch end.
func TestCleanRunCommitsAndAdvancesLSN(t *testing.T) {
	ctx := context.Background()
	sink, _ := newTestSink(t, "0/1000")
	st := newTestStateStore(t)
	src := newFakeSource([]ports.CDCEvent{
		testEvent("0/1010", 0),
		testEvent("0/1020", 1),
	}, "0/1000")

	if err := runStart(t, ctx, testConfig(), src, sink, st); err != nil &&
		err != context.Canceled {
		t.Fatalf("Start: %v", err)
	}

	if got := manifestEpoch(t, sink); got != 1 {
		t.Fatalf("manifest epoch = %d, want 1", got)
	}
	rs, err := st.GetReplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rs.ConfirmedLSN != "0/1020" {
		t.Errorf("journal confirmed LSN = %q, want 0/1020", rs.ConfirmedLSN)
	}
	latest, err := st.GetLatestEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if latest.Status != "committed" || latest.EndLSN != "0/1020" {
		t.Errorf("latest epoch = %+v, want committed ending 0/1020", latest)
	}
	// Standby feedback must never overshoot the durable position.
	if max := src.maxFeedback(); max != "" {
		if cmp, _ := CompareLSN(max, "0/1020"); cmp > 0 {
			t.Errorf("standby feedback %s exceeds committed end 0/1020", max)
		}
	}
}

// TestRestartResumesAtCommittedLSN proves the manifest/journal position is
// wired to the actual replication start: the second Start must call Stream
// at the previous batch end, not at InitLSN.
func TestRestartResumesAtCommittedLSN(t *testing.T) {
	ctx := context.Background()
	sink, _ := newTestSink(t, "0/1000")
	st := newTestStateStore(t)
	// One full batch (MaxRows=2) so it commits via the row-count path rather
	// than relying on the shutdown flush.
	src := newFakeSource([]ports.CDCEvent{
		testEvent("0/1010", 0),
		testEvent("0/1020", 1),
	}, "0/1000")

	cfg := testConfig()
	if err := runStart(t, ctx, cfg, src, sink, st); err != nil && err != context.Canceled {
		t.Fatalf("first Start: %v", err)
	}

	// Second run: same sink + journal, same source. Reuse the source so we
	// can inspect the start LSN handed to Stream.
	if err := runStart(t, ctx, cfg, src, sink, st); err != nil && err != context.Canceled {
		t.Fatalf("second Start: %v", err)
	}

	starts := src.streamStartLSNs()
	if len(starts) < 2 {
		t.Fatalf("expected >=2 Stream calls, got %v", starts)
	}
	if starts[0] != "0/1000" {
		t.Errorf("first start LSN = %q, want 0/1000", starts[0])
	}
	if starts[1] != "0/1020" {
		t.Errorf("second start LSN = %q, want 0/1020 (last committed batch end)", starts[1])
	}
}

// TestManifestCommitFailureReplaysBatch is the central crash scenario:
// delta files have landed but WriteManifest fails. The process exits with an
// error (no silent skip), and the restart must replay from the open batch's
// start LSN rather than past it.
//
// Two recovery modes are exercised depending on how much WAL the slot still
// holds:
//
//   - The reusable files let reconciliation complete the manifest without
//     re-uploading anything.
//   - Postgres redelivers any WAL above the slot's confirmed_flush_lsn. When
//     the crashed process's standby feedback never reached Postgres, the
//     redelivered window is written as a new epoch (at-least-once); when it
//     did, no redelivery happens and only the reused epoch exists.
func TestManifestCommitFailureReplaysBatch(t *testing.T) {
	ctx := context.Background()
	rawSink, _ := newTestSink(t, "0/1000")
	failSink := newFailingSink(rawSink)
	failSink.manifestFailsLeft = 1
	st := newTestStateStore(t)
	src := newFakeSource([]ports.CDCEvent{
		testEvent("0/1010", 0),
		testEvent("0/1020", 1),
	}, "0/1000")

	err := runStart(t, ctx, testConfig(), src, failSink, st)
	if err == nil || err == context.Canceled {
		t.Fatalf("expected fatal flush error, got %v", err)
	}

	// Journal must retain an OPEN epoch at the batch boundary — nothing may
	// be treated as committed.
	open, err := st.GetLatestEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if open == nil || open.Status != "open" {
		t.Fatalf("expected open journal row, got %+v", open)
	}
	if open.StartLSN != "0/1010" || open.EndLSN != "0/1020" {
		t.Errorf("open batch window = [%s,%s], want [0/1010,0/1020]", open.StartLSN, open.EndLSN)
	}
	if got := manifestEpoch(t, failSink); got != 0 {
		t.Errorf("manifest must not advance on failed commit, epoch=%d", got)
	}

	// Restart with failures cleared. The open batch's files are readable, so
	// reconciliation reuses them and completes the manifest without
	// re-uploading. The same source instance stands in for the same slot.
	if err := runStart(t, ctx, testConfig(), src, failSink, st); err != nil && err != context.Canceled {
		t.Fatalf("restart Start: %v", err)
	}

	starts := src.streamStartLSNs()
	if len(starts) != 2 {
		t.Fatalf("expected 2 Stream calls, got %v", starts)
	}
	if starts[0] != "0/1000" {
		t.Errorf("initial start LSN = %q, want 0/1000", starts[0])
	}
	// Reconciliation resumes at the recovered batch end; the slot may still
	// redeliver WAL below it (at-least-once), but nothing past it is missed.
	if starts[1] != "0/1020" {
		t.Errorf("post-recovery start LSN = %q, want 0/1020", starts[1])
	}

	// The recovered epoch 1 is visible in the manifest and its file is the
	// reusable one; any redelivered WAL lands as epoch 2, never skipping the
	// open batch.
	m, err := failSink.ReadManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gotEpoch := m.Tables[testTable].LatestDeltaEpoch
	if gotEpoch < 1 {
		t.Fatalf("manifest epoch after recovery = %d, want >= 1", gotEpoch)
	}
	reusedPath := filepath.Join(testTable, "deltas", "epoch=000001.parquet")
	rc, err := failSink.ReadFile(ctx, reusedPath)
	if err != nil {
		t.Fatalf("reused epoch-1 delta not readable: %v", err)
	}
	_ = rc.Close()

	latest, _ := st.GetLatestEpoch(ctx)
	if latest.Status != "committed" {
		t.Errorf("journal status = %q, want committed", latest.Status)
	}
	rs, _ := st.GetReplicationState(ctx)
	if rs.ConfirmedLSN != "0/1020" {
		t.Errorf("confirmed LSN = %q, want 0/1020", rs.ConfirmedLSN)
	}
	// Standby feedback must never overshoot the durable batch end.
	if max := src.maxFeedback(); max != "" {
		if cmp, _ := CompareLSN(max, "0/1020"); cmp > 0 {
			t.Errorf("standby feedback %s exceeds committed end 0/1020", max)
		}
	}
}

// TestManifestFailureWithCaughtUpSlotReusesOnlyEpoch covers the other crash
// timing: standby feedback reached Postgres before the manifest write failed,
// so the slot already sits at the batch end when reconciliation starts.
// Streaming then has no WAL below the start to redeliver — only the reused
// epoch exists and no new delta is written.
func TestManifestFailureWithCaughtUpSlotReusesOnlyEpoch(t *testing.T) {
	ctx := context.Background()
	rawSink, _ := newTestSink(t, "0/1000")
	failSink := newFailingSink(rawSink)
	st := newTestStateStore(t)
	// Seed a slot that is already at the batch end: nothing should be
	// redelivered when the recovered process connects at that position.
	src := newFakeSource([]ports.CDCEvent{
		testEvent("0/1010", 0),
		testEvent("0/1020", 1),
	}, "0/1020")

	// Reproduce the crashed-process journal directly (as if the first run
	// had failed its manifest write) so the slot starts already caught up.
	if _, err := st.OpenEpoch(ctx, 1, "0/1010", "0/1020"); err != nil {
		t.Fatal(err)
	}
	deltaPath := filepath.Join(testTable, "deltas", "epoch=000001.parquet")
	if err := st.RecordEpochFile(ctx, 1, testTable, deltaPath); err != nil {
		t.Fatal(err)
	}
	// The reusable file must actually exist in the sink.
	if err := writeSeedDelta(ctx, t, failSink, deltaPath); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	_, uploadsBefore := failSink.stats()
	if err := runStart(t, ctx, cfg, src, failSink, st); err != nil && err != context.Canceled {
		t.Fatalf("Start: %v", err)
	}
	_, uploadsAfter := failSink.stats()
	if uploadsAfter != uploadsBefore {
		t.Errorf("delta uploads = %d new, want 0 (slot caught up, nothing redelivered)", uploadsAfter-uploadsBefore)
	}
	if got := manifestEpoch(t, failSink); got != 1 {
		t.Fatalf("manifest epoch = %d, want 1 (reused only)", got)
	}
	latest, _ := st.GetLatestEpoch(ctx)
	if latest.Status != "committed" {
		t.Errorf("journal status = %q, want committed", latest.Status)
	}
}

// TestOpenBatchWithMissingFileIsReplayed covers the case where some delta
// uploads never landed. Reconciliation must not mark the batch complete from
// the manifest alone; the missing table is rewritten under the same epoch
// and the batch is then committed.
func TestOpenBatchWithMissingFileIsReplayed(t *testing.T) {
	ctx := context.Background()
	rawSink, _ := newTestSink(t, "0/1000")
	failSink := newFailingSink(rawSink)
	st := newTestStateStore(t)

	// Simulate the crashed state directly: an open journal epoch whose only
	// registered file is unreadable, no manifest advance.
	if _, err := st.OpenEpoch(ctx, 1, "0/1010", "0/1020"); err != nil {
		t.Fatal(err)
	}
	missingPath := filepath.Join(testTable, "deltas", "epoch=000001.parquet")
	if err := st.RecordEpochFile(ctx, 1, testTable, missingPath); err != nil {
		t.Fatal(err)
	}
	failSink.hidePaths[missingPath] = true

	// Slot LSN starts at the init point; the fake source advances it as
	// durable LSN feedback arrives, mirroring Postgres.
	src := newFakeSource([]ports.CDCEvent{
		testEvent("0/1010", 0),
		testEvent("0/1020", 1),
	}, "0/1000")

	if err := runStart(t, ctx, testConfig(), src, failSink, st); err != nil && err != context.Canceled {
		t.Fatalf("Start: %v", err)
	}

	if got := manifestEpoch(t, failSink); got != 1 {
		t.Fatalf("manifest epoch = %d, want 1 (same epoch replayed, not skipped)", got)
	}
	// The missing object must now exist and be readable.
	rc, err := failSink.ReadFile(ctx, missingPath)
	if err != nil {
		t.Fatalf("replayed delta not readable: %v", err)
	}
	_ = rc.Close()
	latest, _ := st.GetLatestEpoch(ctx)
	if latest.Status != "committed" || latest.EpochNumber != 1 {
		t.Errorf("journal = %+v, want committed epoch 1", latest)
	}
	starts := src.streamStartLSNs()
	if len(starts) == 0 || starts[0] != "0/1010" {
		t.Errorf("start LSN = %v, want 0/1010", starts)
	}
}

// TestManifestVisibleButJournalOpenClosesCleanly covers the narrow crash
// window between a successful WriteManifest and the journal CommitEpoch:
// the sink already shows the epoch, but SQLite still records it as open.
// Reconciliation must notice the epoch is visible and only close the journal
// — it must not rewrite the manifest or allocate a duplicate epoch.
func TestManifestVisibleButJournalOpenClosesCleanly(t *testing.T) {
	ctx := context.Background()
	sink, _ := newTestSink(t, "0/1000")
	st := newTestStateStore(t)

	// Seed the crashed state: open journal row + uploaded file + a manifest
	// that already advanced to epoch 1.
	if _, err := st.OpenEpoch(ctx, 1, "0/1010", "0/1020"); err != nil {
		t.Fatal(err)
	}
	deltaPath := filepath.Join(testTable, "deltas", "epoch=000001.parquet")
	if err := st.RecordEpochFile(ctx, 1, testTable, deltaPath); err != nil {
		t.Fatal(err)
	}
	if err := writeSeedDelta(ctx, t, sink, deltaPath); err != nil {
		t.Fatal(err)
	}
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tbl := m.Tables[testTable]
	tbl.LatestDeltaEpoch = 1
	m.Tables[testTable] = tbl
	if err := sink.WriteManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	// Save a prior durable row so reconcile has a ConfirmedLSN to read.
	if err := st.SaveReplicationState(ctx, &ports.ReplicationState{
		SlotName: "pg_cdc_slot", ConfirmedLSN: "0/1000", Status: "streaming",
	}); err != nil {
		t.Fatal(err)
	}

	// Slot already caught up to the batch end, so nothing is redelivered.
	src := newFakeSource(nil, "0/1020")
	if err := runStart(t, ctx, testConfig(), src, sink, st); err != nil && err != context.Canceled {
		t.Fatalf("Start: %v", err)
	}

	if got := manifestEpoch(t, sink); got != 1 {
		t.Fatalf("manifest epoch = %d, want 1 (no duplicate epoch)", got)
	}
	latest, _ := st.GetLatestEpoch(ctx)
	if latest.Status != "committed" {
		t.Errorf("journal status = %q, want committed", latest.Status)
	}
	rs, _ := st.GetReplicationState(ctx)
	if rs.ConfirmedLSN != "0/1020" {
		t.Errorf("confirmed LSN = %q, want 0/1020", rs.ConfirmedLSN)
	}
}

// TestStartAheadOfSlotIsFatal ensures reconciliation refuses a position that
// would skip WAL the slot can no longer serve.
func TestStartAheadOfSlotIsFatal(t *testing.T) {
	ctx := context.Background()
	sink, _ := newTestSink(t, "0/1000")
	st := newTestStateStore(t)
	// Journal claims progress beyond the slot's confirmed_flush_lsn.
	if err := st.SaveReplicationState(ctx, &ports.ReplicationState{
		SlotName: "pg_cdc_slot", ConfirmedLSN: "0/9999", Status: "streaming",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.OpenEpoch(ctx, 1, "0/1010", "0/9999"); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitEpoch(ctx, 1, "0/9999", 1); err != nil {
		t.Fatal(err)
	}
	src := newFakeSource(nil, "0/2000")

	err := runStart(t, ctx, testConfig(), src, sink, st)
	if err == nil {
		t.Fatal("expected fatal error when start LSN is ahead of slot")
	}
}

// TestJournalEpochFilesTrackUploads verifies the per-batch file ledger used
// by the reuse/replay decision.
func TestJournalEpochFilesTrackUploads(t *testing.T) {
	ctx := context.Background()
	sink, _ := newTestSink(t, "0/1000")
	st := newTestStateStore(t)
	src := newFakeSource([]ports.CDCEvent{
		testEvent("0/1010", 0),
		testEvent("0/1020", 1),
	}, "0/1000")

	if err := runStart(t, ctx, testConfig(), src, sink, st); err != nil && err != context.Canceled {
		t.Fatalf("Start: %v", err)
	}
	files, err := st.GetEpochFiles(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("epoch files = %d, want 1", len(files))
	}
	want := filepath.Join(testTable, "deltas", "epoch=000001.parquet")
	if files[0].RemotePath != want {
		t.Errorf("remote path = %q, want %q", files[0].RemotePath, want)
	}
}
