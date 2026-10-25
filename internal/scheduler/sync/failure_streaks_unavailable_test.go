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

	"github.com/full-chaos/dev-health-ops/internal/providersync"
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
	beginErr, rollbackErr error
	rows                  *streakRows
	execSQL               []string
	commits, rollbacks    int
	rollbackCtxErr        error
}

func (tx *streakTx) Begin(context.Context) (pgx.Tx, error) {
	if tx.beginErr != nil {
		return nil, tx.beginErr
	}
	return tx, nil
}
func (tx *streakTx) Query(context.Context, string, ...any) (pgx.Rows, error) { return tx.rows, nil }
func (tx *streakTx) Commit(context.Context) error {
	tx.commits++
	return nil
}
func (tx *streakTx) Rollback(ctx context.Context) error {
	tx.rollbacks++
	tx.rollbackCtxErr = ctx.Err()
	if tx.rollbackErr != nil {
		return tx.rollbackErr
	}
	return ctx.Err()
}
func (tx *streakTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	tx.execSQL = append(tx.execSQL, sql)
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
		"begin": {&streakTx{beginErr: boom}, "savepoint_begin_failed"},
		"scan":  {&streakTx{rows: &streakRows{scanErr: boom, rowsLeft: 1}}, "row_scan_failed"},
		"iter":  {&streakTx{rows: &streakRows{iterErr: boom}}, "row_iteration_failed"},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			datasets := []PlanDataset{{Key: workitemcontract.FamilyDatasets()[0]}}
			got, err := callLoadFailureStreaks(context.Background(), test.tx, "org", "integration", "github", datasets)
			if got != nil || err != nil {
				t.Fatalf("streaks = %v, err = %v, want nil and no error: an unreadable history is not fatal", got, err)
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

func familyDatasets() []PlanDataset {
	return []PlanDataset{{Key: workitemcontract.FamilyDatasets()[0]}}
}

// CHAOS-11391: the rollback of the read savepoint must run on a context that
// outlives the step budget. A rollback on the spent step context fails and
// leaves the whole materialize transaction aborted.
func TestLoadFailureStreaksRollsBackOnALiveContextAfterTheStepContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tx := &streakTx{rows: &streakRows{iterErr: context.Canceled}}
	got, err := callLoadFailureStreaks(ctx, tx, "org", "integration", "github", familyDatasets())
	if got != nil || err != nil {
		t.Fatalf("streaks = %v, err = %v, want nil and no error", got, err)
	}
	if tx.rollbacks != 1 || tx.rollbackCtxErr != nil {
		t.Fatalf("rollbacks = %d on a context with error %v, want one rollback on a live context", tx.rollbacks, tx.rollbackCtxErr)
	}
}

// CHAOS-11391: the read is read-only, so its savepoint is rolled back on
// success too. A RELEASE would keep the SET LOCAL statement_timeout for the
// rest of the materialize transaction.
func TestLoadFailureStreaksAlwaysRollsBackTheSavepointAndSetsAServerBound(t *testing.T) {
	tx := &streakTx{rows: &streakRows{}}
	got, err := callLoadFailureStreaks(context.Background(), tx, "org", "integration", "github", familyDatasets())
	if err != nil || len(got) != 0 {
		t.Fatalf("streaks = %v, err = %v, want none and no error", got, err)
	}
	if tx.commits != 0 || tx.rollbacks != 1 {
		t.Fatalf("commits = %d rollbacks = %d, want 0 and 1", tx.commits, tx.rollbacks)
	}
	if len(tx.execSQL) != 1 || !strings.Contains(tx.execSQL[0], "set_config('statement_timeout', $1, true)") {
		t.Fatalf("exec = %v, want one transaction-local statement_timeout", tx.execSQL)
	}
}

// A failed rollback leaves the transaction unusable: that is the one error the
// caller must see, and it is not logged as an unavailable read.
func TestLoadFailureStreaksReportsAFailedRollback(t *testing.T) {
	tx := &streakTx{rows: &streakRows{iterErr: errors.New("boom")}, rollbackErr: errors.New("conn closed")}
	got, err := callLoadFailureStreaks(context.Background(), tx, "org", "integration", "github", familyDatasets())
	if got != nil || err == nil {
		t.Fatalf("streaks = %v, err = %v, want nil and an error", got, err)
	}
}

// The streak SQL must stay a per-source top-N: a window over every terminal
// unit of the integration grows with the backfill (CHAOS-11391).
func TestRecentTerminalUnitsSQLReadsOnlyTheNewestRowsOfEachSource(t *testing.T) {
	sql := providersync.RecentTerminalUnitsSQL
	if !strings.Contains(sql, "LATERAL") || !strings.Contains(sql, "LIMIT $4") || strings.Contains(sql, "row_number") {
		t.Fatalf("RecentTerminalUnitsSQL is not a per-source LATERAL top-N:\n%s", sql)
	}
}
