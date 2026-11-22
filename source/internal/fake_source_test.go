package internal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// fakeSource is a deterministic in-memory Source for recovery tests.
// Stream replays the scripted events whose LSN is at/after startLSN and
// records every durable LSN it is asked to feedback — that is what a real
// Postgres would turn into confirmed_flush_lsn.
type fakeSource struct {
	mu sync.Mutex

	events      []ports.CDCEvent
	slotLSN     string
	feedback    []string
	streamCalls []string
	closed      bool

	// failBeforeLSN, when non-empty, makes Stream return an error as soon as
	// an event at or past that LSN has been delivered, simulating a crash
	// mid-batch (the channel is closed by the same path as a real disconnect).
	failBeforeLSN string
}

func newFakeSource(events []ports.CDCEvent, slotLSN string) *fakeSource {
	return &fakeSource{events: events, slotLSN: slotLSN}
}

func (f *fakeSource) ListTables(ctx context.Context, schema string) ([]string, error) {
	return nil, nil
}

func (f *fakeSource) GetTableSchema(ctx context.Context, schema, table string) ([]ports.ColumnInfo, error) {
	return nil, nil
}

func (f *fakeSource) GetPrimaryKeys(ctx context.Context, schema, table string) ([]string, error) {
	return nil, nil
}

func (f *fakeSource) Version(ctx context.Context) (string, error) { return "16", nil }

func (f *fakeSource) CreateReplicationSlot(ctx context.Context, slotName string) (ports.Snapshot, error) {
	return ports.Snapshot{}, nil
}

func (f *fakeSource) ReleaseSnapshot(ctx context.Context) {}

func (f *fakeSource) CreatePublication(ctx context.Context, pubName string, tables []string) error {
	return nil
}

func (f *fakeSource) CopyTable(ctx context.Context, schema, table, snapshotName string) ([]map[string]any, []ports.ColumnInfo, error) {
	return nil, nil, nil
}

func (f *fakeSource) Stream(ctx context.Context, slotName, pubName, startLSN string,
	events chan<- ports.CDCEvent, durableLSN func() string) error {

	f.mu.Lock()
	f.streamCalls = append(f.streamCalls, startLSN)
	failAt := f.failBeforeLSN
	f.mu.Unlock()

	defer close(events)

	// Real Postgres redelivers from the slot's confirmed_flush_lsn, which
	// can lag the requested start when the previous process's standby
	// feedback never arrived. Mirror that: replay begins at min(slot, start).
	// recordFeedback advances slotLSN as durable positions are reported, so
	// once feedback lands, redelivery stops on subsequent reconnects.
	f.mu.Lock()
	slot := f.slotLSN
	f.mu.Unlock()
	replayFrom := startLSN
	if slot != "" {
		if cmp, _ := CompareLSN(slot, replayFrom); cmp < 0 {
			replayFrom = slot
		}
	}
	replayOffset, err := ParseLSN(replayFrom)
	if err != nil {
		return fmt.Errorf("bad replay LSN: %w", err)
	}

	// Remaining scripted events; feedback is polled between deliveries so the
	// fake slot's confirmed_flush_lsn advances as batches commit, the way a
	// real Postgres processes standby status updates mid-stream. pgoutput
	// delivers WAL strictly after the starting position, hence the > check.
	pending := make([]ports.CDCEvent, 0, len(f.events))
	for _, evt := range f.events {
		evtLSN, err := ParseLSN(evt.LSN)
		if err != nil {
			return err
		}
		if evtLSN > replayOffset {
			pending = append(pending, evt)
		}
	}

	for len(pending) > 0 {
		f.recordFeedback(durableLSN)
		next := pending[0]
		select {
		case events <- next:
			pending = pending[1:]
		case <-ctx.Done():
			f.recordFeedback(durableLSN)
			return ctx.Err()
		}
		if failAt != "" {
			if cmp, _ := CompareLSN(next.LSN, failAt); cmp >= 0 {
				return fmt.Errorf("simulated stream crash at %s", next.LSN)
			}
		}
	}

	// Events exhausted: keep observing durable feedback until the caller
	// tears the context down, so a final committed batch still advances the
	// fake slot's confirmed_flush_lsn. The sleep stands in for network/heartbeat
	// latency in a real replication connection, giving the flush loop time to
	// commit between feedback polls.
	for {
		f.recordFeedback(durableLSN)
		select {
		case <-ctx.Done():
			f.recordFeedback(durableLSN)
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (f *fakeSource) recordFeedback(durableLSN func() string) {
	if durableLSN == nil {
		return
	}
	lsn := durableLSN()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.feedback) == 0 || f.feedback[len(f.feedback)-1] != lsn {
		f.feedback = append(f.feedback, lsn)
	}
	// Mirror Postgres: a standby status advances confirmed_flush_lsn to the
	// reported durable position (monotonically), so reconciliation's slot
	// check sees a realistic moving target across restarts.
	if lsn != "" {
		if cmp, _ := CompareLSN(lsn, f.slotLSN); cmp > 0 {
			f.slotLSN = lsn
		}
	}
}

func (f *fakeSource) feedbackValues() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.feedback))
	copy(out, f.feedback)
	return out
}

func (f *fakeSource) maxFeedback() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var max string
	for _, l := range f.feedback {
		if cmp, _ := CompareLSN(l, max); cmp > 0 {
			max = l
		}
	}
	return max
}

func (f *fakeSource) streamStartLSNs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.streamCalls))
	copy(out, f.streamCalls)
	return out
}

func (f *fakeSource) BuildRoleProfiles(ctx context.Context, roles []string) (map[string]ports.RoleProfile, error) {
	return nil, nil
}

func (f *fakeSource) DropReplicationSlot(ctx context.Context, slotName string) error { return nil }
func (f *fakeSource) DropPublication(ctx context.Context, pubName string) error      { return nil }

func (f *fakeSource) QueryRow(ctx context.Context, sql string, args ...any) ports.Row {
	return &fakeRow{lsn: f.slotLSN}
}

func (f *fakeSource) Close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
}

type fakeRow struct{ lsn string }

func (r *fakeRow) Scan(dest ...any) error {
	if p, ok := dest[0].(*string); ok {
		*p = r.lsn
	}
	return nil
}
