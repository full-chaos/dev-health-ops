package sync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

type streakRows struct {
	pgx.Rows
	scanErr, iterErr error
	rowsLeft         int
}

func (rows *streakRows) Next() bool {
	if rows.rowsLeft == 0 {
		return false
	}
	rows.rowsLeft--
	return true
}
func (rows *streakRows) Scan(...any) error { return rows.scanErr }
func (rows *streakRows) Err() error        { return rows.iterErr }
func (rows *streakRows) Close()            {}

type streakTx struct {
	pgx.Tx
	beginErr, commitErr error
	rows                *streakRows
}

func (tx *streakTx) Begin(context.Context) (pgx.Tx, error) {
	if tx.beginErr != nil {
		return nil, tx.beginErr
	}
	return tx, nil
}
func (tx *streakTx) Query(context.Context, string, ...any) (pgx.Rows, error) { return tx.rows, nil }
func (tx *streakTx) Commit(context.Context) error                            { return tx.commitErr }
func (tx *streakTx) Rollback(context.Context) error                          { return nil }
func (tx *streakTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

// A failure-history read that breaks anywhere must be logged: with no log, a
// broken read looks like "no failures" and the source keeps its hourly
// full-window retries (CHAOS-10074).
func TestLoadFailureStreaksLogsEveryReadFailure(t *testing.T) {
	boom := errors.New("boom secret detail")
	for name, test := range map[string]struct {
		tx   *streakTx
		want string
	}{
		"begin":  {&streakTx{beginErr: boom}, "savepoint_begin_failed"},
		"scan":   {&streakTx{rows: &streakRows{scanErr: boom, rowsLeft: 1}}, "row_scan_failed"},
		"iter":   {&streakTx{rows: &streakRows{iterErr: boom}}, "row_iteration_failed"},
		"commit": {&streakTx{rows: &streakRows{}, commitErr: boom}, "savepoint_commit_failed"},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			datasets := []PlanDataset{{Key: workitemcontract.FamilyDatasets()[0]}}
			if got := loadFailureStreaks(context.Background(), test.tx, "org", "integration", "github", datasets); got != nil {
				t.Fatalf("streaks = %v, want nil", got)
			}
			out := logs.String()
			if !strings.Contains(out, "sync.materializer.failure_streaks_unavailable") || !strings.Contains(out, "failure="+test.want) {
				t.Fatalf("log = %q, want failure_streaks_unavailable with failure=%s", out, test.want)
			}
			if strings.Contains(out, "secret detail") {
				t.Fatalf("log carries the error text: %q", out)
			}
		})
	}
}
