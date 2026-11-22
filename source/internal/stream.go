package internal

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// Start begins WAL streaming and writes delta Parquet files.
//
// Recovery model (at-least-once, no batch skipped):
//
//   - The manifest remains the authoritative record of what is *readable in
//     the sink*: a delta counts only once its epoch is in the manifest.
//   - A local, crash-safe SQLite journal (ports.StateStore) records each
//     micro-batch: epoch number, start/end LSN, uploaded per-table files,
//     and open/committed status.
//   - On startup the journal is reconciled against the manifest. A batch
//     whose files landed but whose manifest commit failed is either reused
//     (files still readable) or replayed in full (files missing/unreadable).
//     Streaming resumes at the start LSN of the first not-yet-committed
//     batch — never past it.
//   - Standby feedback to Postgres is pinned to the end LSN of the last
//     fully committed batch, so the replication slot never releases WAL that
//     is not durable in the sink.
func Start(ctx context.Context, cfg config.Config, source ports.Source, sink ports.Sink, stateStore ports.StateStore) (err error) {
	defer source.Close()
	if stateStore != nil {
		defer func() { _ = stateStore.Close() }()
	}

	// Load manifest — the sink-side view of readable epochs.
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("read manifest (run 'pg-cdc init' first): %w", err)
	}

	slotName := cfg.Replication.Slot
	if slotName == "" {
		slotName = "pg_cdc_slot"
	}
	pubName := cfg.Replication.Publication
	if pubName == "" {
		pubName = "pg_cdc_pub"
	}

	flushInterval := time.Duration(cfg.Flush.IntervalSec) * time.Second
	if flushInterval == 0 {
		flushInterval = 10 * time.Second
	}
	flushMaxRows := cfg.Flush.MaxRows
	if flushMaxRows == 0 {
		flushMaxRows = 1000
	}

	coord, err := newCoordinator(ctx, source, sink, stateStore, m, slotName, pubName)
	if err != nil {
		return err
	}

	fmt.Printf("Streaming from slot=%s pub=%s (flush: %s / %d rows)\n", slotName, pubName, flushInterval, flushMaxRows)
	fmt.Printf("Init LSN: %s, resuming at: %s (last committed epoch: %d)\n",
		m.Source.InitLSN, coord.startLSN, coord.lastCommittedEpoch)

	// runCtx is owned by this loop: cancelling it unblocks the streaming
	// goroutine on a fatal flush so wg.Wait can never deadlock against a
	// Stream blocked on a full events channel.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	events := make(chan ports.CDCEvent, 1000)
	var streamErr error
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		streamErr = source.Stream(runCtx, slotName, pubName, coord.startLSN, events, coord.durableLSN)
	}()

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	// failFlush stops the streamer and returns a fatal error. The batch that
	// failed stays journaled as open, so restart replays it instead of
	// skipping past it.
	failFlush := func(ferr error) error {
		cancelRun()
		wg.Wait()
		if streamErr != nil {
			return fmt.Errorf("flush failed (batch retained for replay): %w; stream: %v", ferr, streamErr)
		}
		return fmt.Errorf("flush failed (batch retained for replay): %w", ferr)
	}

	for {
		select {
		case evt, ok := <-events:
			if !ok {
				// Stream closed — flush remaining. A flush failure here is
				// fatal: the events must be replayed on restart rather than
				// dropped on the floor.
				if ferr := coord.flush(runCtx); ferr != nil {
					return failFlush(ferr)
				}
				wg.Wait()
				return streamErr
			}
			coord.add(evt)
			if coord.count() >= flushMaxRows {
				if ferr := coord.flush(runCtx); ferr != nil {
					return failFlush(ferr)
				}
			}

		case <-flushTicker.C:
			if coord.count() > 0 {
				if ferr := coord.flush(runCtx); ferr != nil {
					return failFlush(ferr)
				}
			}

		case <-ctx.Done():
			// Graceful shutdown: drain whatever is buffered into a committed
			// batch so the next start sees no replay. The parent context is
			// already cancelled, so use a detached drain context with a
			// bounded window — SQLite/S3 operations would otherwise fail
			// instantly on the cancelled context.
			cancelRun()
			drainCtx, drainCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			ferr := coord.flush(drainCtx)
			drainCancel()
			if ferr != nil {
				fmt.Printf("shutdown flush error: %v (batch retained for replay on next start)\n", ferr)
			}
			wg.Wait()
			return ctx.Err()
		}
	}
}

// coordinator owns the crash-safe batch lifecycle. All mutable state is
// guarded by mu; the durable position read by the streaming goroutine is
// served from the same lock.
type coordinator struct {
	source   ports.Source
	sink     ports.Sink
	state    ports.StateStore // nil disables journaling (tests/legacy)
	manifest *manifest.Manifest
	tables   map[string]manifest.Table
	slotName string
	pubName  string

	mu sync.Mutex
	// Pending events not yet flushed.
	events []ports.CDCEvent
	// Epoch currently being assembled (0 = none).
	openEpoch int64
	openRowID int64
	// reusablePaths lists sink paths already verified readable for the
	// in-flight recovery epoch; their tables skip re-upload on replay.
	reusablePaths map[string]bool
	// Monotonic high-water marks.
	lastCommittedEpoch int64
	// replayingOpenBatch marks reconciliation that found an open journal
	// epoch; slot-position checks are advisory in that mode because an
	// unconfirmed flush can legitimately sit ahead of the slot's LSN.
	replayingOpenBatch bool
	// startLSN is where replication begins on this run.
	startLSN string
	// durableLSNValue is the end LSN of the last committed batch and the only
	// position ever reported back to Postgres.
	durableLSNValue string
}

func newCoordinator(ctx context.Context, source ports.Source, sink ports.Sink,
	stateStore ports.StateStore, m *manifest.Manifest, slotName, pubName string) (*coordinator, error) {

	c := &coordinator{
		source:   source,
		sink:     sink,
		state:    stateStore,
		manifest: m,
		tables:   m.ActiveTables(),
		slotName: slotName,
		pubName:  pubName,
	}

	// Epoch high-water from the manifest: readable deltas only.
	for _, t := range m.Tables {
		if t.LatestDeltaEpoch > c.lastCommittedEpoch {
			c.lastCommittedEpoch = t.LatestDeltaEpoch
		}
	}

	if stateStore == nil {
		// Journal-less mode keeps the historical behaviour: start at InitLSN.
		c.startLSN = m.Source.InitLSN
		return c, nil
	}

	if err := c.reconcile(ctx); err != nil {
		return nil, fmt.Errorf("startup reconciliation: %w", err)
	}
	return c, nil
}

// reconcile aligns the local batch journal with the sink manifest and
// decides the exact replication start position. It is the crash-recovery
// entry point and never advances past an uncommitted batch.
func (c *coordinator) reconcile(ctx context.Context) error {
	rs, err := c.state.GetReplicationState(ctx)
	if err != nil {
		return fmt.Errorf("read replication journal: %w", err)
	}

	latest, err := c.state.GetLatestEpoch(ctx)
	if err != nil {
		return fmt.Errorf("read epoch journal: %w", err)
	}

	switch {
	case rs == nil && latest == nil:
		// First run with a fresh state store.
		c.startLSN = c.manifest.Source.InitLSN
		c.durableLSNValue = c.manifest.Source.InitLSN
		c.replayingOpenBatch = false
		fmt.Println("recovery: no local journal, starting at manifest init LSN")
		if err := c.checkSlotPosition(ctx); err != nil {
			return err
		}

	case latest != nil && latest.Status != "committed":
		// An open batch means the previous process died mid-flush. The slot
		// still holds its WAL; resume at the batch start and drive it to
		// completion, reusing the files that already landed where possible.
		c.startLSN = latest.StartLSN
		// The durable position is the previous committed boundary, recorded
		// in the replication row. If that row is missing (older journal or a
		// crash before the first state save), pinning to the manifest init
		// point is the safe lower bound: Postgres retains everything above it.
		if rs != nil {
			c.durableLSNValue = rs.ConfirmedLSN
		} else {
			c.durableLSNValue = c.manifest.Source.InitLSN
		}
		c.replayingOpenBatch = true
		// Check against the batch START (the actual replay point), in
		// advisory replay mode.
		if err := c.checkSlotPosition(ctx); err != nil {
			return err
		}
		committed, err := c.recoverOpenBatch(ctx, latest)
		if err != nil {
			return err
		}
		fmt.Printf("recovery: open epoch %d (start=%s, end=%s): committed=%t\n",
			latest.EpochNumber, latest.StartLSN, latest.EndLSN, committed)
		if committed {
			// The manifest was advanced during recovery. Re-read it so this
			// process's in-memory copy matches the sink; otherwise the next
			// epoch would be allocated from a stale high-water mark and
			// overwrite/recreate an already-recovered epoch.
			fresh, err := c.sink.ReadManifest(ctx)
			if err != nil {
				return fmt.Errorf("re-read manifest after recovery: %w", err)
			}
			c.manifest = fresh
			c.tables = fresh.ActiveTables()
			c.lastCommittedEpoch = 0
			for _, t := range fresh.Tables {
				if t.LatestDeltaEpoch > c.lastCommittedEpoch {
					c.lastCommittedEpoch = t.LatestDeltaEpoch
				}
			}
			// Recovery consumed the whole open batch; stream from its end.
			// The slot may still lag because the standby feedback of the
			// crashed process never reached Postgres — that is expected and
			// the next feedback closes the gap.
			c.startLSN = latest.EndLSN
			c.replayingOpenBatch = false
		}

	default:
		// Journal and manifest agree on a clean boundary.
		if rs == nil {
			return fmt.Errorf("epoch journal present but replication_state row is missing; cannot determine resume position")
		}
		c.startLSN = rs.ConfirmedLSN
		c.durableLSNValue = rs.ConfirmedLSN
		c.replayingOpenBatch = false
		if cmp, err := CompareLSN(c.startLSN, c.manifest.Source.InitLSN); err == nil && cmp < 0 {
			return fmt.Errorf("journal confirmed LSN %s precedes manifest init LSN %s; refusing to start",
				c.startLSN, c.manifest.Source.InitLSN)
		}
		fmt.Printf("recovery: clean shutdown, resuming at confirmed LSN %s\n", c.startLSN)
		if err := c.checkSlotPosition(ctx); err != nil {
			return err
		}
	}

	// Seed the replication-state row so durableLSN is available immediately.
	return c.persistReplicationState(ctx, "streaming", "")
}

// recoverOpenBatch handles a batch whose journal row is still "open". Files
// already readable in the sink are registered for reuse; the rest will be
// rewritten when the batch is replayed.
//
// It returns committed=true when every recorded file is reusable: the
// manifest was the only lost step, so the batch is completed in place and no
// WAL replay is needed. When committed=false the open journal row is kept as
// the replay target; streaming resumes at the batch start and overwrites any
// partial object under the same epoch.
func (c *coordinator) recoverOpenBatch(ctx context.Context, open *ports.EpochState) (committed bool, err error) {
	files, err := c.state.GetEpochFiles(ctx, open.EpochNumber)
	if err != nil {
		return false, fmt.Errorf("read files for open epoch %d: %w", open.EpochNumber, err)
	}

	var missing int
	// Tables for which a readable file already exists at the exact target
	// path; replay skips rewriting these.
	reusablePaths := make(map[string]bool, len(files))
	for _, f := range files {
		if c.fileReadable(ctx, f.RemotePath) {
			reusablePaths[f.RemotePath] = true
			continue
		}
		missing++
		fmt.Printf("recovery: %s missing/unreadable — will replay table %s\n", f.RemotePath, f.TableName)
	}

	// Full reuse fast path: every file of the open batch is readable, so the
	// only lost step was the manifest commit. Re-point the manifest and
	// commit the journal without touching Postgres WAL.
	if len(files) > 0 && missing == 0 {
		if err := c.commitRecoveredBatch(ctx, open, files); err != nil {
			return false, err
		}
		return true, nil
	}

	// Partial or no files: keep the open journal row as the replay target.
	// Its epoch number is reused so any partially-uploaded object is
	// overwritten in place rather than orphaned behind a new epoch.
	c.openEpoch = open.EpochNumber
	c.openRowID = open.ID
	c.reusablePaths = reusablePaths
	return false, nil
}

// commitRecoveredBatch completes a fully-reusable open batch by advancing
// the manifest and journal — the same commit tail as a normal flush.
//
// It is idempotent against the "manifest written, journal commit lost"
// crash window: if the sink already shows the epoch, the manifest rewrite is
// a no-op and only the journal/durable boundary needs closing.
func (c *coordinator) commitRecoveredBatch(ctx context.Context, open *ports.EpochState, files []ports.EpochFile) error {
	c.mu.Lock()
	alreadyVisible := true
	for _, f := range files {
		t := c.manifest.Tables[f.TableName]
		if t.LatestDeltaEpoch < open.EpochNumber {
			t.LatestDeltaEpoch = open.EpochNumber
			c.manifest.Tables[f.TableName] = t
			alreadyVisible = false
		}
		c.tables[f.TableName] = t
	}
	if c.lastCommittedEpoch < open.EpochNumber {
		c.lastCommittedEpoch = open.EpochNumber
	}
	c.mu.Unlock()

	if !alreadyVisible {
		if err := c.writeManifest(ctx); err != nil {
			return fmt.Errorf("manifest commit for recovered epoch %d: %w", open.EpochNumber, err)
		}
	} else {
		fmt.Printf("recovery: epoch %d already visible in manifest; closing journal only\n", open.EpochNumber)
	}
	if err := c.state.CommitEpoch(ctx, open.ID, open.EndLSN, 0); err != nil {
		return fmt.Errorf("commit recovered epoch %d journal: %w", open.EpochNumber, err)
	}
	if err := c.advanceDurable(ctx, open.EndLSN, open.EpochNumber, ""); err != nil {
		return err
	}
	fmt.Printf("recovery: epoch %d completed from reusable files without WAL replay\n", open.EpochNumber)
	return nil
}

// checkSlotPosition best-effort compares the intended start LSN against the
// replication slot's confirmed_flush_lsn.
//
// On a clean boundary, starting above the slot would skip changes and is a
// hard error. During open-batch replay the start can legitimately be ahead
// of the slot: the previous process's standby feedback may not have reached
// Postgres before the crash, and replaying the gap is exactly the recovery
// mechanism. That case only warns.
func (c *coordinator) checkSlotPosition(ctx context.Context) error {
	row := c.source.QueryRow(ctx,
		`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, c.slotName)
	var slotLSN string
	if err := row.Scan(&slotLSN); err != nil {
		fmt.Printf("recovery: warning: cannot read slot %s position: %v\n", c.slotName, err)
		return nil
	}
	cmp, err := CompareLSN(c.startLSN, slotLSN)
	if err != nil {
		fmt.Printf("recovery: warning: cannot compare LSN %q vs slot %q: %v\n", c.startLSN, slotLSN, err)
		return nil
	}
	if cmp > 0 && !c.replayingOpenBatch {
		return fmt.Errorf("start LSN %s is ahead of slot confirmed_flush_lsn %s — refusing to skip data",
			c.startLSN, slotLSN)
	}
	if cmp > 0 {
		fmt.Printf("recovery: note: replay start %s is ahead of slot LSN %s; WAL since the slot position is replayed\n",
			c.startLSN, slotLSN)
	}
	return nil
}

// fileReadable verifies an uploaded delta still exists and can be opened.
// Read failures (including object-not-found) mean the file must be replayed.
func (c *coordinator) fileReadable(ctx context.Context, remotePath string) bool {
	rc, err := c.sink.ReadFile(ctx, remotePath)
	if err != nil {
		return false
	}
	_ = rc.Close()
	return true
}

// durableLSN is the callback handed to the source streamer. It returns the
// highest LSN whose batch is durable in the sink (files + manifest).
func (c *coordinator) durableLSN() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.durableLSNValue
}

func (c *coordinator) add(evt ports.CDCEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, evt)
}

func (c *coordinator) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events)
}

// flush drives one micro-batch through the crash-safe commit protocol:
//
//  1. journal the batch (epoch, start/end LSN) before any sink write,
//  2. upload each table's delta (idempotent overwrite at a fixed path),
//     recording every upload in the journal,
//  3. commit the manifest (sink-side visibility boundary),
//  4. mark the epoch committed and advance the durable LSN — only then may
//     Postgres release the underlying WAL.
//
// A failure at any step returns an error with the batch boundaries still
// journaled, so restart replays from the batch start instead of skipping it.
func (c *coordinator) flush(ctx context.Context) error {
	c.mu.Lock()
	if len(c.events) == 0 {
		c.mu.Unlock()
		return nil
	}
	events := c.events
	c.events = nil

	// Epoch allocation: reuse an in-flight recovery epoch, otherwise take the
	// next number above every epoch visible in the manifest.
	epochNum := c.openEpoch
	reusable := c.reusablePaths
	c.reusablePaths = nil
	if epochNum == 0 {
		epochNum = c.lastCommittedEpoch + 1
	}

	// Batch LSN window: first event's LSN is the (inclusive) low watermark;
	// the highest event LSN is the batch end.
	startLSN := events[0].LSN
	var endLSN string
	for _, evt := range events {
		m, err := maxLSN(endLSN, evt.LSN)
		if err != nil {
			c.mu.Unlock()
			return fmt.Errorf("invalid event LSN: %w", err)
		}
		endLSN = m
	}
	// The batch journaled on a previous run already carries the authoritative
	// window; preserve it rather than shrinking to whatever replay delivered.
	prevOpen := c.openRowID
	c.openEpoch = 0
	c.openRowID = 0
	c.mu.Unlock()

	if c.state == nil {
		return c.flushLegacy(ctx, events, epochNum)
	}

	// Step 1: journal the batch before touching the sink.
	var open *ports.EpochState
	var err error
	if prevOpen > 0 {
		open, err = c.state.GetEpoch(ctx, epochNum)
		if err != nil {
			return fmt.Errorf("reload open epoch %d: %w", epochNum, err)
		}
		if open == nil {
			return fmt.Errorf("journal row for recovery epoch %d vanished", epochNum)
		}
		// Replay may have buffered a wider window; widen the recorded end.
		if cmp, _ := CompareLSN(endLSN, open.EndLSN); cmp > 0 {
			if err := c.state.UpdateEpochEndLSN(ctx, epochNum, endLSN); err != nil {
				return fmt.Errorf("update end LSN for epoch %d: %w", epochNum, err)
			}
		} else {
			endLSN = open.EndLSN
		}
		startLSN = open.StartLSN
	} else {
		open, err = c.state.OpenEpoch(ctx, epochNum, startLSN, endLSN)
		if err != nil {
			return fmt.Errorf("open epoch %d journal: %w", epochNum, err)
		}
	}

	// Step 2: group, write and upload per-table deltas.
	byTable := make(map[string][]ports.CDCEvent)
	for _, evt := range events {
		byTable[evt.Table] = append(byTable[evt.Table], evt)
	}

	var totalRows int64
	uploaded := make([]ports.EpochFile, 0, len(byTable))
	for tableName, tableEvents := range byTable {
		table, ok := c.tables[tableName]
		if !ok {
			fmt.Printf("  skip: %s (not in manifest)\n", tableName)
			continue
		}

		remotePath := filepath.Join(table.Path, "deltas", deltaFilename(epochNum))
		// Reuse path: a recovered file that is already readable need not be
		// rewritten — but its events were still replayed into this buffer,
		// which is fine because the file content is identical by construction
		// (same epoch, same WAL window).
		if reusable[remotePath] && c.fileReadable(ctx, remotePath) {
			fmt.Printf("  epoch %d: reusing readable %s\n", epochNum, remotePath)
			uploaded = append(uploaded, ports.EpochFile{EpochNumber: epochNum, TableName: tableName, RemotePath: remotePath})
			totalRows += int64(len(tableEvents))
			continue
		}

		if err := c.writeTableDelta(ctx, tableName, tableEvents, epochNum, remotePath); err != nil {
			return err
		}
		// Journal the upload before moving on: a crash after this point finds
		// the file registered and can take the reuse fast path on restart.
		if err := c.state.RecordEpochFile(ctx, epochNum, tableName, remotePath); err != nil {
			return fmt.Errorf("record epoch file %s: %w", remotePath, err)
		}
		uploaded = append(uploaded, ports.EpochFile{EpochNumber: epochNum, TableName: tableName, RemotePath: remotePath})
		totalRows += int64(len(tableEvents))
		fmt.Printf("  epoch %d: %s (%d events)\n", epochNum, tableName, len(tableEvents))
	}

	// A flush with no manifest-known tables has nothing durable to commit;
	// drop the empty journal row so it does not block recovery.
	if len(uploaded) == 0 {
		if err := c.state.DeleteEpoch(ctx, epochNum); err != nil {
			return fmt.Errorf("cleanup empty epoch %d: %w", epochNum, err)
		}
		return nil
	}

	// Step 3: manifest commit — the sink visibility boundary.
	c.mu.Lock()
	for _, f := range uploaded {
		t := c.manifest.Tables[f.TableName]
		t.LatestDeltaEpoch = epochNum
		c.manifest.Tables[f.TableName] = t
		c.tables[f.TableName] = t
	}
	c.lastCommittedEpoch = epochNum
	c.mu.Unlock()

	if err := c.writeManifest(ctx); err != nil {
		return fmt.Errorf("write manifest after epoch %d: %w (files retained; restart replays this batch)", epochNum, err)
	}

	// Step 4: journal commit, then release the WAL window.
	if err := c.state.CommitEpoch(ctx, open.ID, endLSN, totalRows); err != nil {
		return fmt.Errorf("commit epoch %d journal: %w", epochNum, err)
	}
	if err := c.advanceDurable(ctx, endLSN, epochNum, ""); err != nil {
		return err
	}
	return nil
}

// writeTableDelta absorbs schema evolution, renders rows and uploads one
// delta Parquet file, overwriting any partial object at the same path.
func (c *coordinator) writeTableDelta(ctx context.Context, tableName string,
	tableEvents []ports.CDCEvent, epochNum int64, remotePath string) error {

	if _, err := c.evolveSchema(ctx, tableName, tableEvents); err != nil {
		fmt.Printf("  schema refresh failed for %s: %v\n", tableName, err)
	}

	rows := make([]writer.Row, len(tableEvents))
	for i, evt := range tableEvents {
		row := make(writer.Row, len(evt.Row)+1)
		row[btypes.ColOp] = evt.Op
		for k, v := range evt.Row {
			row[k] = v
		}
		rows[i] = row
	}

	schema, ok := c.manifest.Schemas[tableName]
	if !ok {
		return fmt.Errorf("no schema in manifest for %s", tableName)
	}

	tmpPath := filepath.Join("/tmp", "pgcdc-stream", remotePath)
	if err := writer.WriteDeltaParquet(tmpPath, rows, schema.Columns, epochNum); err != nil {
		return fmt.Errorf("write delta for %s: %w", tableName, err)
	}
	if err := uploadFile(ctx, c.sink, remotePath, tmpPath); err != nil {
		return fmt.Errorf("upload delta for %s: %w", tableName, err)
	}
	return nil
}

// writeManifest serialises the current manifest through the sink. Kept as a
// seam so failure injection in tests can target the exact commit boundary.
func (c *coordinator) writeManifest(ctx context.Context) error {
	c.manifest.UpdatedAt = time.Now().UTC()
	return c.sink.WriteManifest(ctx, c.manifest)
}

// advanceDurable moves the committed high-water mark and persists the
// replication-state row. The streamer's standby feedback reads this value,
// so it must change only after the manifest commit has succeeded.
func (c *coordinator) advanceDurable(ctx context.Context, endLSN string, epoch int64, errMsg string) error {
	c.mu.Lock()
	c.durableLSNValue = endLSN
	c.mu.Unlock()

	if c.state == nil {
		return nil
	}
	return c.persistReplicationState(ctx, "streaming", errMsg)
}

func (c *coordinator) persistReplicationState(ctx context.Context, status, errMsg string) error {
	c.mu.Lock()
	durable := c.durableLSNValue
	c.mu.Unlock()
	return c.state.SaveReplicationState(ctx, &ports.ReplicationState{
		SlotName:        c.slotName,
		PublicationName: c.pubName,
		ConfirmedLSN:    durable,
		LastReceivedLSN: durable,
		Status:          status,
		ErrorMessage:    errMsg,
	})
}

// flushLegacy preserves the pre-journal flush path for deployments without a
// state store. It keeps the old epoch-from-manifest allocation and does not
// report a durable LSN, so standby feedback stays pinned at the start point.
func (c *coordinator) flushLegacy(ctx context.Context, events []ports.CDCEvent, epochNum int64) error {
	byTable := make(map[string][]ports.CDCEvent)
	for _, evt := range events {
		byTable[evt.Table] = append(byTable[evt.Table], evt)
	}
	for tableName, tableEvents := range byTable {
		table, ok := c.tables[tableName]
		if !ok {
			fmt.Printf("  skip: %s (not in manifest)\n", tableName)
			continue
		}
		remotePath := filepath.Join(table.Path, "deltas", deltaFilename(epochNum))
		if err := c.writeTableDelta(ctx, tableName, tableEvents, epochNum, remotePath); err != nil {
			return err
		}
		t := c.manifest.Tables[tableName]
		t.LatestDeltaEpoch = epochNum
		c.manifest.Tables[tableName] = t
		c.tables[tableName] = t
		fmt.Printf("  epoch %d: %s (%d events)\n", epochNum, tableName, len(tableEvents))
	}
	return c.writeManifest(ctx)
}

func deltaFilename(epochNum int64) string {
	return fmt.Sprintf("epoch=%06d.parquet", epochNum)
}

// evolveSchema checks events for columns not present in the manifest schema
// and refreshes the schema from the source if any are found. New columns get
// appended to preserve existing column ordering in downstream Parquet files.
// Returns true if the schema was updated.
func (c *coordinator) evolveSchema(ctx context.Context, tableName string, events []ports.CDCEvent) (bool, error) {
	schema, ok := c.manifest.Schemas[tableName]
	if !ok {
		return false, nil
	}

	known := make(map[string]struct{}, len(schema.Columns))
	for _, col := range schema.Columns {
		known[col.Name] = struct{}{}
	}

	unknown := make(map[string]struct{})
	for _, evt := range events {
		for k := range evt.Row {
			if k == btypes.ColOp || k == btypes.ColEpoch || k == btypes.ColDeletedAt {
				continue
			}
			if _, ok := known[k]; !ok {
				unknown[k] = struct{}{}
			}
		}
	}
	if len(unknown) == 0 {
		return false, nil
	}

	parts := strings.SplitN(tableName, ".", 2)
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid table name %q", tableName)
	}
	cols, err := c.source.GetTableSchema(ctx, parts[0], parts[1])
	if err != nil {
		return false, fmt.Errorf("refresh schema for %s: %w", tableName, err)
	}

	added := 0
	for _, col := range cols {
		if _, ok := known[col.Name]; ok {
			continue
		}
		if _, ok := unknown[col.Name]; !ok {
			continue
		}
		schema.Columns = append(schema.Columns, manifest.Column{
			Name:        col.Name,
			PgType:      col.Type,
			ParquetType: btypes.ParquetType(col.Type),
			Nullable:    col.Nullable,
		})
		added++
	}
	if added == 0 {
		return false, nil
	}

	schema.Version++
	c.manifest.Schemas[tableName] = schema
	fmt.Printf("  schema evolved: %s (+%d cols, v=%d)\n", tableName, added, schema.Version)
	return true, nil
}
