package sync

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

type budgetBlockedRow struct {
	source  string
	blocked bool
}

// budgetBlockedRows returns the given rows through Scan, newest first per source, as TestOpsBudgetBlockedUnitsSQL does.
type budgetBlockedRows struct {
	pgx.Rows
	rows []budgetBlockedRow
	next int
}

func (rows *budgetBlockedRows) Next() bool {
	rows.next++
	return rows.next <= len(rows.rows)
}
func (rows *budgetBlockedRows) Scan(dest ...any) error {
	row := rows.rows[rows.next-1]
	*dest[0].(*string) = row.source
	*dest[1].(*bool) = row.blocked
	*dest[2].(*time.Time) = time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	return nil
}
func (rows *budgetBlockedRows) Err() error { return nil }
func (rows *budgetBlockedRows) Close()     {}

type budgetBlockedTx struct {
	streakTx
	result    *budgetBlockedRows
	querySQL  string
	queryArgs []any
}

func (tx *budgetBlockedTx) Begin(ctx context.Context) (pgx.Tx, error) {
	if _, err := tx.streakTx.Begin(ctx); err != nil {
		return nil, err
	}
	return tx, nil
}
func (tx *budgetBlockedTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx.querySQL, tx.queryArgs = sql, args
	return tx.result, nil
}

func TestLoadTestOpsBudgetBlockedCountsPerSourceInItsOwnBoundedTransaction(t *testing.T) {
	tx := &budgetBlockedTx{result: &budgetBlockedRows{rows: []budgetBlockedRow{
		{"a", true}, {"a", false}, {"a", true},
		{"b", false}, {"b", false},
	}}}
	got := loadTestOpsBudgetBlocked(context.Background(), tx, "org", "integration", "github", []PlanDataset{{Key: "tests"}})
	if len(got) != 1 || got["a"] != 2 {
		t.Fatalf("counts = %v, want a=2 and no entry for b", got)
	}
	if tx.querySQL != providersync.TestOpsBudgetBlockedUnitsSQL {
		t.Fatal("the read does not run TestOpsBudgetBlockedUnitsSQL")
	}
	if len(tx.queryArgs) != 4 || tx.queryArgs[2] != "cicd" || tx.queryArgs[3] != providersync.TestOpsNarrowingLookback {
		t.Fatalf("query args = %v, want the canonical cicd dataset and the lookback", tx.queryArgs)
	}
	if tx.begins != 1 || tx.commits != 0 || tx.rollbacks != 1 {
		t.Fatalf("begins = %d commits = %d rollbacks = %d, want 1, 0 and 1", tx.begins, tx.commits, tx.rollbacks)
	}
	if len(tx.execSQL) != 1 || !strings.Contains(tx.execSQL[0], "set_config('statement_timeout', $1, true)") {
		t.Fatalf("exec = %v, want one transaction-local statement_timeout", tx.execSQL)
	}
}

func TestLoadTestOpsBudgetBlockedReadsOnlyForAGitHubTestOpsPlan(t *testing.T) {
	for name, test := range map[string]struct {
		provider string
		datasets []PlanDataset
	}{
		"gitlab":         {"gitlab", []PlanDataset{{Key: "cicd"}}},
		"no tests, cicd": {"github", []PlanDataset{{Key: "commits"}}},
	} {
		t.Run(name, func(t *testing.T) {
			tx := &budgetBlockedTx{result: &budgetBlockedRows{}}
			if got := loadTestOpsBudgetBlocked(context.Background(), tx, "org", "integration", test.provider, test.datasets); got != nil {
				t.Fatalf("counts = %v, want nil", got)
			}
			if tx.begins != 0 {
				t.Fatalf("begins = %d, want no read", tx.begins)
			}
		})
	}
}

// A read that breaks is logged and never fatal: the plan proceeds unnarrowed, and the log carries no error text.
func TestLoadTestOpsBudgetBlockedLogsAReadFailure(t *testing.T) {
	logs := captureLogs(t)
	tx := &budgetBlockedTx{streakTx: streakTx{beginErr: errors.New("boom secret detail")}}
	if got := loadTestOpsBudgetBlocked(context.Background(), tx, "org", "integration", "github", []PlanDataset{{Key: "cicd"}}); got != nil {
		t.Fatalf("counts = %v, want nil", got)
	}
	out := logs.String()
	if !strings.Contains(out, "sync.materializer.testops_budget_blocked_unavailable") || !strings.Contains(out, "failure=read_begin_failed") {
		t.Fatalf("log = %q, want testops_budget_blocked_unavailable with failure=read_begin_failed", out)
	}
	if strings.Contains(out, "secret detail") {
		t.Fatalf("log carries the error text: %q", out)
	}
}

type replayEndRow struct {
	source string
	end    time.Time
}

type replayEndRows struct {
	pgx.Rows
	rows []replayEndRow
	next int
}

func (rows *replayEndRows) Next() bool {
	rows.next++
	return rows.next <= len(rows.rows)
}
func (rows *replayEndRows) Scan(dest ...any) error {
	row := rows.rows[rows.next-1]
	*dest[0].(*string) = row.source
	*dest[1].(*time.Time) = row.end
	return nil
}
func (rows *replayEndRows) Err() error { return nil }
func (rows *replayEndRows) Close()     {}

type replayEndReader struct {
	rows    *replayEndRows
	err     error
	queries int
	args    []any
}

func (reader *replayEndReader) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	reader.queries++
	reader.args = args
	if sql != testOpsReplayEndsSQL {
		return nil, errors.New("unexpected statement")
	}
	if reader.err != nil {
		return nil, reader.err
	}
	return reader.rows, nil
}

// The committed-window read runs for a GitHub tests/cicd plan only, keys the ends by source, finds nothing on a first
// attempt, and fails the attempt (never ErrInvalidPlan, so the occurrence is retried, not quarantined) when it breaks.
func TestLoadTestOpsReplayEndsReadsTheCommittedCicdUnits(t *testing.T) {
	end := time.Date(2026, 11, 17, 12, 0, 0, 0, time.FixedZone("x", 3600))
	reader := &replayEndReader{rows: &replayEndRows{rows: []replayEndRow{{"a", end}}}}
	got, err := loadTestOpsReplayEnds(context.Background(), reader, "run", "github", []PlanDataset{{Key: "tests"}})
	if err != nil || len(got) != 1 || !got["a"].Equal(end) || got["a"].Location() != time.UTC {
		t.Fatalf("ends = %v err %v, want a=%s in UTC", got, err, end.UTC())
	}
	if len(reader.args) != 2 || reader.args[0] != "run" || reader.args[1] != canonicalTestOpsDataset {
		t.Fatalf("args = %v, want the sync run and the canonical cicd dataset", reader.args)
	}
	first := &replayEndReader{rows: &replayEndRows{}}
	if got, err := loadTestOpsReplayEnds(context.Background(), first, "run", "github", []PlanDataset{{Key: "cicd"}}); err != nil || got != nil {
		t.Fatalf("first attempt ends = %v err %v, want none", got, err)
	}
	for name, test := range map[string]struct {
		provider string
		datasets []PlanDataset
	}{
		"gitlab":           {"gitlab", []PlanDataset{{Key: "cicd"}}},
		"no tests or cicd": {"github", []PlanDataset{{Key: "commits"}}},
	} {
		skipped := &replayEndReader{rows: &replayEndRows{}}
		if got, err := loadTestOpsReplayEnds(context.Background(), skipped, "run", test.provider, test.datasets); err != nil || got != nil || skipped.queries != 0 {
			t.Fatalf("%s: ends = %v err %v queries %d, want no read", name, got, err, skipped.queries)
		}
	}
	broken := &replayEndReader{err: errors.New("boom")}
	_, err = loadTestOpsReplayEnds(context.Background(), broken, "run", "github", []PlanDataset{{Key: "cicd"}})
	if err == nil || errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("broken read err = %v, want a retryable error (not ErrInvalidPlan)", err)
	}
}
