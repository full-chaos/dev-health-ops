package providerunit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// finishDayReadConn is a driver.Conn whose stored-finish-day read (the SELECT
// the ci_pipeline_runs write runs before its INSERT) drops: at Query, or in
// the row stream (Rows.Err). It records every PrepareBatch, so a cell knows
// no INSERT started.
type finishDayReadConn struct {
	driver.Conn
	queryErr, streamErr error
	prepared            *int
}

func (c finishDayReadConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return finishDayStream{err: c.streamErr}, nil
}

func (c finishDayReadConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	*c.prepared++
	return nil, errors.New("no INSERT may start after a failed finish-day read")
}

type finishDayStream struct {
	driver.Rows
	err error
}

func (finishDayStream) Next() bool        { return false }
func (finishDayStream) Close() error      { return nil }
func (stream finishDayStream) Err() error { return stream.err }

// committedFinishDayReadDrop is the error the REAL effect committer returns
// when the REAL TestOps sink's ci_pipeline_runs write fails on its
// stored-finish-day read, and the count of INSERTs that started.
func committedFinishDayReadDrop(t *testing.T, conn finishDayReadConn) (error, int) {
	t.Helper()
	now := time.Date(2026, 11, 22, 17, 27, 12, 0, time.UTC)
	prepared := 0
	conn.prepared = &prepared
	row := fmt.Sprintf(`{"org_id":"org-acme","repo_id":"7b0d3a52-2f4e-4c55-9a43-0c6f1f6d2a11","run_id":"9001",`+
		`"provider":"github_actions","started_at":%q,"last_synced":%q}`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	effect, err := providersync.BuildEffectBatch("ci_pipeline_runs", providersync.EffectReadbackRequired,
		[]json.RawMessage{json.RawMessage(row)})
	if err != nil {
		t.Fatal(err)
	}
	committer := providersync.EffectCommitter{
		Ledger: &testEffectLedger{},
		Sink: providersync.TestOpsClickHouseEffects{
			Conn:  conn,
			Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		},
		Readback: testEffectReadback{},
		Now:      func() time.Time { return now },
	}
	_, err = committer.Commit(context.Background(), sinkDropClaim(now), []providersync.EffectBatch{effect}, now)
	return err, prepared
}

// A dropped READ inside a chunk write is no dropped INSERT: no INSERT was
// tried, so the failure keeps the unclassified class and its cap of 2 (only a
// drop at the INSERT boundary gets sink_connection_dropped and the registry
// ceiling).
func TestADroppedFinishDayReadInsideAChunkWriteIsUnclassified(t *testing.T) {
	for name, conn := range map[string]finishDayReadConn{
		"query":      {queryErr: realDroppedConnFirstBlockError(t)},
		"row stream": {streamErr: realDroppedConnFirstBlockError(t)},
	} {
		t.Run(name, func(t *testing.T) {
			err, inserts := committedFinishDayReadDrop(t, conn)
			if err == nil || !errors.Is(err, io.EOF) || inserts != 0 {
				t.Fatalf("premise: committer error %v (errors.Is io.EOF=%v) after %d INSERTs, want the dropped read and no INSERT",
					err, errors.Is(err, io.EOF), inserts)
			}
			var dropped *providersync.SinkConnectionDroppedError
			if class, limit := causeClass(err, false), retryMaxAttempts(err); errors.As(err, &dropped) ||
				class != providersync.CauseClassUnclassified || limit != unclassifiedMaxAttempts {
				t.Fatalf("dropped finish-day read: cause_class=%q cap=%d (sink drop error %v), want %q cap %d",
					class, limit, errors.As(err, &dropped), providersync.CauseClassUnclassified, unclassifiedMaxAttempts)
			}
		})
	}
}

// A drop AT the INSERT boundary keeps its own class and the registry ceiling:
// the REAL TestOps sink's PrepareBatch, and its batch's Send, each dropped.
func TestADroppedInsertCallInsideAChunkWriteIsASinkDrop(t *testing.T) {
	now := time.Date(2026, 11, 22, 17, 27, 12, 0, time.UTC)
	for name, onSend := range map[string]bool{"PrepareBatch": false, "Send": true} {
		t.Run(name, func(t *testing.T) {
			committer := providersync.EffectCommitter{
				Ledger:   &testEffectLedger{},
				Sink:     droppingTestOpsSinkAt(droppingConn{err: realDroppedConnFirstBlockError(t), onSend: onSend}),
				Readback: testEffectReadback{},
				Now:      func() time.Time { return now },
			}
			_, err := committer.Commit(context.Background(), sinkDropClaim(now),
				[]providersync.EffectBatch{testSuiteResultsEffect(t, now)}, now)
			if class, limit := causeClass(err, false), retryMaxAttempts(err); class != causeClassSinkConnectionDropped || limit != 0 {
				t.Fatalf("dropped %s: err %v cause_class=%q cap=%d, want %q cap 0 (registry ceiling)",
					name, err, class, limit, causeClassSinkConnectionDropped)
			}
		})
	}
}
