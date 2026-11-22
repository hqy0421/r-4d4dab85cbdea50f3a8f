package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// CDCEvent represents a single change captured from WAL.
type CDCEvent struct {
	Table     string         // qualified table name (schema.table)
	Op        string         // "I", "U", "D"
	Row       map[string]any // column values
	LSN       string
	Timestamp time.Time
}

// Stream starts logical replication and sends decoded events to the channel.
// The channel is closed when Stream returns. Blocks until ctx is cancelled.
//
// durableLSN (may be nil) returns the highest LSN known to be durably
// committed in the sink. Standby feedback only ever reports that position —
// the WAL Postgres has merely sent (serverWALEnd) is used for keepalive
// timeout tracking but is never confirmed back, so Postgres cannot release
// WAL pg-cdc would need to replay after a crash.
func (c *Conn) Stream(ctx context.Context, slotName, publicationName, startLSN string, events chan<- CDCEvent, durableLSN func() string) error {
	defer close(events)

	replConn, err := c.connectReplication(ctx)
	if err != nil {
		return fmt.Errorf("replication connect: %w", err)
	}
	defer func() { _ = replConn.Close(context.Background()) }()

	lsn, err := pglogrepl.ParseLSN(startLSN)
	if err != nil {
		return fmt.Errorf("parse start LSN: %w", err)
	}

	err = pglogrepl.StartReplication(ctx, replConn, slotName, lsn,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '2'",
				fmt.Sprintf("publication_names '%s'", publicationName),
				"messages 'true'",
			},
		})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	// feedbackPosition is the only LSN we ever confirm to Postgres. It
	// starts at the replication start point and moves forward strictly as
	// batches become durable in the sink.
	feedbackPosition := lsn
	resolveFeedback := func() pglogrepl.LSN {
		if durableLSN == nil {
			return feedbackPosition
		}
		raw := durableLSN()
		if raw == "" {
			return feedbackPosition
		}
		parsed, perr := pglogrepl.ParseLSN(raw)
		if perr != nil || parsed <= feedbackPosition {
			return feedbackPosition
		}
		feedbackPosition = parsed
		return feedbackPosition
	}

	// Immediate standby status to prevent wal_sender_timeout.
	_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: resolveFeedback()})

	standbyDeadline := time.Now().Add(10 * time.Second)
	decoder := newDecoder()

	for {
		if ctx.Err() != nil {
			_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: resolveFeedback()})
			return ctx.Err()
		}

		// Periodic standby status — always the durable position, never the
		// server's WAL end.
		if time.Now().After(standbyDeadline) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: resolveFeedback()}); err != nil {
				return fmt.Errorf("standby status: %w", err)
			}
			standbyDeadline = time.Now().Add(10 * time.Second)
		}

		recvCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		rawMsg, err := replConn.ReceiveMessage(recvCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("receive: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("replication error: %s", errMsg.Message)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				standbyDeadline = time.Time{}
				if err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
					pglogrepl.StandbyStatusUpdate{WALWritePosition: resolveFeedback()}); err != nil {
					return fmt.Errorf("standby status (reply requested): %w", err)
				}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog: %w", err)
			}

			evt, err := decoder.decode(xld.WALData, xld.WALStart.String(), xld.ServerTime)
			if err != nil {
				return fmt.Errorf("decode WAL: %w", err)
			}
			if evt != nil {
				select {
				case events <- *evt:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}
