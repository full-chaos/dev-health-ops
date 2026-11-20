package syncreconciler

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// rearmWarningTx fails the step named by failAt with the error fail returns.
type rearmWarningTx struct {
	*shapeTx
	failAt string
	fail   func(context.Context) error
}

func (tx *rearmWarningTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if (tx.failAt == "unit_lock" && sql == lockOrphanedUnitSQL) ||
		(tx.failAt == "source_read" && sql == readOrphanedSourceRowSQL) {
		return shapeRow{err: tx.fail(ctx)}
	}
	return tx.shapeTx.QueryRow(ctx, sql, args...)
}

func (tx *rearmWarningTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if tx.failAt == "insert_reclaim" {
		return pgconn.CommandTag{}, tx.fail(ctx)
	}
	return tx.shapeTx.Exec(ctx, sql, args...)
}

func (tx *rearmWarningTx) Commit(ctx context.Context) error {
	if tx.failAt == "domain_commit" {
		tx.open = nil
		return tx.fail(ctx)
	}
	return tx.shapeTx.Commit(ctx)
}

// rearmWarningRepair re-arms 3 orphans, one domain transaction each; the
// transaction number failOn fails at failAt.
func rearmWarningRepair(store *shapeStore, failOn int, failAt string, fail func(context.Context) error) *OrphanedUnitRepair {
	budget := newShapeBudget(shapeOrphanBudget)
	begins := 0
	return &OrphanedUnitRepair{
		queryQueue: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &partialSurveyRows{remaining: orphanedPassCandidates(3)}, nil
		},
		beginDomain: func(ctx context.Context) (pgx.Tx, error) {
			begins++
			tx := &shapeTx{store: store, budget: budget, commitCost: 14}
			if begins != failOn {
				return tx, nil
			}
			if failAt == "domain_begin" {
				return nil, fail(ctx)
			}
			return &rearmWarningTx{shapeTx: tx, failAt: failAt, fail: fail}, nil
		},
		staleAge: time.Minute,
	}
}

func countSlogRecords(records []map[string]any, message string) (int, map[string]any) {
	count := 0
	var last map[string]any
	for _, record := range records {
		if record["msg"] == message {
			count++
			last = record
		}
	}
	return count, last
}

// CHAOS-14238: re-arms an orphan pass committed before it failed get the
// syncreconciler.orphaned_units_rearmed WARN exactly once with their count,
// also when the failure is the OUTER tick being canceled or past its deadline
// (MutationPipeline.Step returns early then); the error is still returned.
func TestOrphanedUnitsRearmedWarningCoversCommittedReArmsOfAFailedPass(t *testing.T) {
	for _, step := range []string{"domain_begin", "unit_lock", "source_read", "insert_reclaim", "domain_commit"} {
		for _, mode := range []string{"stage_error", "outer_canceled", "outer_deadline"} {
			t.Run(step+"/"+mode, func(t *testing.T) {
				outer := newShapeBudget(3600)
				var ctx context.Context = shapeContext{Context: context.Background(), budget: outer}
				cancel := func() {}
				if mode == "outer_canceled" {
					ctx, cancel = context.WithCancel(context.Background())
				}
				defer cancel()
				fail := func(stageCtx context.Context) error {
					switch mode {
					case "outer_canceled":
						cancel()
						<-stageCtx.Done()
						return stageCtx.Err()
					case "outer_deadline":
						_ = outer.spend(7200)
						<-stageCtx.Done()
						return stageCtx.Err()
					}
					return &pgconn.PgError{Code: "40001", Message: "could not serialize access due to concurrent update"}
				}
				pipeline, err := newRecoveryCountingPipeline(t, TerminalDeliveryRepairResult{})
				if err != nil {
					t.Fatal(err)
				}
				store := &shapeStore{}
				pipeline.orphanedUnit = rearmWarningRepair(store, 2, step, fail)
				captured, restore := captureSlogRecords(t)
				defer restore()
				_, stepErr := pipeline.Step(ctx, time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC), 10)
				if mode != "stage_error" && stepErr == nil {
					t.Fatal("Step = nil, want the outer context's error returned")
				}
				if len(store.durable) != 1 {
					t.Fatalf("committed %d re-arms, want 1 before the failure", len(store.durable))
				}
				warnings, line := countSlogRecords(*captured, "syncreconciler.orphaned_units_rearmed")
				if warnings != 1 || line["re_armed"] != int64(1) {
					t.Fatalf("orphaned_units_rearmed lines = %d (last %v), want exactly one with re_armed=1", warnings, line)
				}
			})
		}
	}
}

// A pass that fails before any re-arm commits logs no orphaned_units_rearmed.
func TestOrphanedUnitsRearmedWarningIsAbsentWithoutCommittedReArms(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pipeline, err := newRecoveryCountingPipeline(t, TerminalDeliveryRepairResult{})
	if err != nil {
		t.Fatal(err)
	}
	store := &shapeStore{}
	pipeline.orphanedUnit = rearmWarningRepair(store, 1, "unit_lock", func(stageCtx context.Context) error {
		cancel()
		<-stageCtx.Done()
		return stageCtx.Err()
	})
	captured, restore := captureSlogRecords(t)
	defer restore()
	_, _ = pipeline.Step(ctx, time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC), 10)
	if warnings, _ := countSlogRecords(*captured, "syncreconciler.orphaned_units_rearmed"); warnings != 0 || len(store.durable) != 0 {
		t.Fatalf("orphaned_units_rearmed lines = %d with %d committed, want none", warnings, len(store.durable))
	}
}
