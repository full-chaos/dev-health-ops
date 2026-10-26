package sync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

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
	beginErr           error
	begins             int
	rows               *streakRows
	execSQL            []string
	execArgs           [][]any
	commits, rollbacks int
	rollbackCtxErr     error
}

func (tx *streakTx) Begin(context.Context) (pgx.Tx, error) {
	tx.begins++
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
	return ctx.Err()
}
func (tx *streakTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.execSQL = append(tx.execSQL, sql)
	tx.execArgs = append(tx.execArgs, args)
	return pgconn.CommandTag{}, nil
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func familyDatasets() []PlanDataset {
	return []PlanDataset{{Key: workitemcontract.FamilyDatasets()[0]}}
}

// A failure-history read that breaks anywhere must be logged: with no log, a
// broken read looks like "no failures" and the source keeps its hourly
// full-window retries (CHAOS-10074). It is never fatal.
func TestLoadFailureStreaksLogsEveryReadFailure(t *testing.T) {
	boom := errors.New("boom secret detail")
	for name, test := range map[string]struct {
		tx   *streakTx
		want string
	}{
		"begin": {&streakTx{beginErr: boom}, "read_begin_failed"},
		"scan":  {&streakTx{rows: &streakRows{scanErr: boom, rowsLeft: 1}}, "row_scan_failed"},
		"iter":  {&streakTx{rows: &streakRows{iterErr: boom}}, "row_iteration_failed"},
	} {
		t.Run(name, func(t *testing.T) {
			logs := captureLogs(t)
			if got := loadFailureStreaks(context.Background(), test.tx, "org", "integration", "github", familyDatasets()); got != nil {
				t.Fatalf("streaks = %v, want nil: an unreadable history is not fatal", got)
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

// The read runs in its own transaction, which is rolled back on success too
// (RELEASE or COMMIT would keep nothing useful and the read writes nothing),
// with a transaction-local server bound.
func TestLoadFailureStreaksAlwaysRollsBackItsOwnTransactionAndSetsAServerBound(t *testing.T) {
	tx := &streakTx{rows: &streakRows{}}
	if got := loadFailureStreaks(context.Background(), tx, "org", "integration", "github", familyDatasets()); len(got) != 0 {
		t.Fatalf("streaks = %v, want none", got)
	}
	if tx.begins != 1 || tx.commits != 0 || tx.rollbacks != 1 {
		t.Fatalf("begins = %d commits = %d rollbacks = %d, want 1, 0 and 1", tx.begins, tx.commits, tx.rollbacks)
	}
	if len(tx.execSQL) != 1 || !strings.Contains(tx.execSQL[0], "set_config('statement_timeout', $1, true)") {
		t.Fatalf("exec = %v, want one transaction-local statement_timeout", tx.execSQL)
	}
	if got := tx.execArgs[0][0]; got != "500" {
		t.Fatalf("statement_timeout = %v ms with no step deadline, want the 500 ms cap", got)
	}
}

// CHAOS-11391: the rollback runs on a context that outlives the step budget.
func TestLoadFailureStreaksRollsBackOnALiveContextAfterTheStepContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tx := &streakTx{rows: &streakRows{iterErr: context.Canceled}}
	if got := loadFailureStreaks(ctx, tx, "org", "integration", "github", familyDatasets()); got != nil {
		t.Fatalf("streaks = %v, want nil", got)
	}
	if tx.rollbacks != 1 || tx.rollbackCtxErr != nil {
		t.Fatalf("rollbacks = %d on a context with error %v, want one rollback on a live context", tx.rollbacks, tx.rollbackCtxErr)
	}
}

// CHAOS-11427: the read takes at most 500 ms and never the last 5 s of the
// step; below 100 ms it is skipped.
func TestFailureStreakReadBoundKeepsTheStepReserve(t *testing.T) {
	if bound, _, ok := failureStreakReadBound(context.Background()); !ok || bound != failureStreakReadCap {
		t.Fatalf("no deadline: bound = %s ok = %t, want the cap", bound, ok)
	}
	for _, test := range []struct {
		left           time.Duration
		ok             bool
		minimum, limit time.Duration
	}{
		{15 * time.Second, true, failureStreakReadCap, failureStreakReadCap},
		{5500 * time.Millisecond, true, failureStreakReadCap - 50*time.Millisecond, failureStreakReadCap},
		{5300 * time.Millisecond, true, 250 * time.Millisecond, 300 * time.Millisecond},
		{5050 * time.Millisecond, false, -time.Hour, 50 * time.Millisecond},
		{2 * time.Second, false, -time.Hour, 0},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), test.left)
		bound, remaining, ok := failureStreakReadBound(ctx)
		cancel()
		if ok != test.ok || bound < test.minimum || bound > test.limit {
			t.Errorf("%s left: bound = %s ok = %t, want ok = %t and a bound in [%s, %s]", test.left, bound, ok, test.ok, test.minimum, test.limit)
		}
		if remaining > test.left || remaining < test.left-time.Second {
			t.Errorf("%s left: remaining = %s", test.left, remaining)
		}
	}
}

// CHAOS-11427: below the floor the read does not reach the database at all,
// and the skip is logged with the time left in the step.
func TestLoadFailureStreaksSkipsTheReadBelowTheFloor(t *testing.T) {
	logs := captureLogs(t)
	ctx, cancel := context.WithTimeout(context.Background(), failureStreakReadReserve+50*time.Millisecond)
	defer cancel()
	tx := &streakTx{rows: &streakRows{}}
	if got := loadFailureStreaks(ctx, tx, "org", "integration", "github", familyDatasets()); got != nil {
		t.Fatalf("streaks = %v, want nil", got)
	}
	if tx.begins != 0 {
		t.Fatalf("begins = %d, want 0: a skipped read must not open a transaction", tx.begins)
	}
	out := logs.String()
	if !strings.Contains(out, "failure=skipped_low_budget") || !strings.Contains(out, "remaining_ms=") {
		t.Fatalf("log = %q, want failure=skipped_low_budget with remaining_ms", out)
	}
}

// The server bound is the step-derived bound, not a fixed value.
func TestLoadFailureStreaksSetsTheStepDerivedServerBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), failureStreakReadReserve+300*time.Millisecond)
	defer cancel()
	tx := &streakTx{rows: &streakRows{}}
	loadFailureStreaks(ctx, tx, "org", "integration", "github", familyDatasets())
	if len(tx.execArgs) != 1 {
		t.Fatalf("exec = %v", tx.execSQL)
	}
	ms, err := strconv.Atoi(tx.execArgs[0][0].(string))
	if err != nil || ms < 250 || ms > 300 {
		t.Fatalf("statement_timeout = %v ms, want the step-derived bound in [250, 300]", tx.execArgs[0][0])
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
