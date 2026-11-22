// Package sqlite adapts internal/state as a ports.StateStore implementation.
package sqlite

import (
	"context"
	"sync"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/state"
)

// Adapter wraps the concrete state.Store behind ports.StateStore.
type Adapter struct {
	store  *state.Store
	closed bool
	mu     sync.Mutex
}

// Verify interface compliance at compile time.
var _ ports.StateStore = (*Adapter)(nil)

// New opens a SQLite state store at the given path.
func New(path string) (*Adapter, error) {
	s, err := state.Open(path)
	if err != nil {
		return nil, err
	}
	return &Adapter{store: s}, nil
}

func (a *Adapter) GetReplicationState(ctx context.Context) (*ports.ReplicationState, error) {
	rs, err := a.store.GetReplicationState(ctx)
	if err != nil || rs == nil {
		return nil, err
	}
	return &ports.ReplicationState{
		SlotName:        rs.SlotName,
		PublicationName: rs.PublicationName,
		ConfirmedLSN:    rs.ConfirmedLSN,
		LastReceivedLSN: rs.LastReceivedLSN,
		Status:          rs.Status,
		ErrorMessage:    rs.ErrorMessage,
		UpdatedAt:       rs.UpdatedAt,
	}, nil
}

func (a *Adapter) SaveReplicationState(ctx context.Context, rs *ports.ReplicationState) error {
	return a.store.SaveReplicationState(ctx, &state.ReplicationState{
		SlotName:        rs.SlotName,
		PublicationName: rs.PublicationName,
		ConfirmedLSN:    rs.ConfirmedLSN,
		LastReceivedLSN: rs.LastReceivedLSN,
		Status:          rs.Status,
		ErrorMessage:    rs.ErrorMessage,
	})
}

func (a *Adapter) OpenEpoch(ctx context.Context, epochNumber int64, startLSN, endLSN string) (*ports.EpochState, error) {
	es, err := a.store.OpenEpoch(ctx, epochNumber, startLSN, endLSN)
	if err != nil {
		return nil, err
	}
	return toPortEpoch(es), nil
}

func (a *Adapter) UpdateEpochEndLSN(ctx context.Context, epochNumber int64, endLSN string) error {
	return a.store.UpdateEpochEndLSN(ctx, epochNumber, endLSN)
}

func (a *Adapter) RecordEpochFile(ctx context.Context, epochNumber int64, tableName, remotePath string) error {
	return a.store.RecordEpochFile(ctx, epochNumber, tableName, remotePath)
}

func (a *Adapter) GetEpochFiles(ctx context.Context, epochNumber int64) ([]ports.EpochFile, error) {
	files, err := a.store.GetEpochFiles(ctx, epochNumber)
	if err != nil {
		return nil, err
	}
	out := make([]ports.EpochFile, len(files))
	for i, f := range files {
		out[i] = ports.EpochFile{
			EpochNumber: f.EpochNumber,
			TableName:   f.TableName,
			RemotePath:  f.RemotePath,
		}
	}
	return out, nil
}

func (a *Adapter) GetEpoch(ctx context.Context, epochNumber int64) (*ports.EpochState, error) {
	es, err := a.store.GetEpoch(ctx, epochNumber)
	if err != nil || es == nil {
		return nil, err
	}
	return toPortEpoch(es), nil
}

func (a *Adapter) DeleteEpoch(ctx context.Context, epochNumber int64) error {
	return a.store.DeleteEpoch(ctx, epochNumber)
}

func toPortEpoch(es *state.EpochState) *ports.EpochState {
	return &ports.EpochState{
		ID:          es.ID,
		EpochNumber: es.EpochNumber,
		StartedAt:   es.StartedAt,
		CommittedAt: es.CommittedAt,
		StartLSN:    es.StartLSN,
		EndLSN:      es.EndLSN,
		RowCount:    es.RowCount,
		Status:      es.Status,
	}
}

func (a *Adapter) CommitEpoch(ctx context.Context, id int64, endLSN string, rowCount int64) error {
	return a.store.CommitEpoch(ctx, id, endLSN, rowCount)
}

func (a *Adapter) GetLatestEpoch(ctx context.Context) (*ports.EpochState, error) {
	es, err := a.store.GetLatestEpoch(ctx)
	if err != nil || es == nil {
		return nil, err
	}
	return toPortEpoch(es), nil
}

func (a *Adapter) SaveTableState(ctx context.Context, ts *ports.TableState) error {
	return a.store.SaveTableState(ctx, &state.TableState{
		TableName:       ts.TableName,
		LastSnapshotLSN: ts.LastSnapshotLSN,
		RowCount:        ts.RowCount,
		SchemaVersion:   ts.SchemaVersion,
	})
}

func (a *Adapter) GetTableState(ctx context.Context, tableName string) (*ports.TableState, error) {
	ts, err := a.store.GetTableState(ctx, tableName)
	if err != nil || ts == nil {
		return nil, err
	}
	return &ports.TableState{
		TableName:       ts.TableName,
		LastSnapshotLSN: ts.LastSnapshotLSN,
		RowCount:        ts.RowCount,
		SchemaVersion:   ts.SchemaVersion,
		UpdatedAt:       ts.UpdatedAt,
	}, nil
}

func (a *Adapter) SaveCompactionState(ctx context.Context, cs *ports.CompactionStateRecord) error {
	return a.store.SaveCompactionState(ctx, &state.CompactionStateRecord{
		TableName:      cs.TableName,
		LastBaseEpoch:  cs.LastBaseEpoch,
		TombstoneCount: cs.TombstoneCount,
	})
}

func (a *Adapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	return a.store.Close()
}
