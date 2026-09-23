//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// scriptedEstimator fails exactly one call, by 0-indexed call order, with the
// scripted error, and answers every other call with one estimate per unit.
type scriptedEstimator struct {
	calls  int
	failAt int
	err    error
}

func (f *scriptedEstimator) DispatchBudgetEstimate(_ context.Context, _, _ string, unitIDs []string) (map[string][]budgetEstimate, error) {
	index := f.calls
	f.calls++
	if index == f.failAt {
		return nil, f.err
	}
	result := make(map[string][]budgetEstimate, len(unitIDs))
	for _, id := range unitIDs {
		result[id] = estimateFor(1, "github", "rest_core", "core")
	}
	return result, nil
}

// estimateErrorClasses is the error axis of the gate's failure table: every
// error a budgetEstimator can return on main, plus the fatal class and its
// wrappings. fatal reports whether enforceRun must fail its pass on it.
var estimateErrorClasses = []struct {
	name          string
	err           error
	fatal         bool // enforceRun fails its pass (any estimate call)
	estimateFatal bool // the class is ErrEstimateFatal (also fails the baseline)
}{
	{"bare error", errors.New("transport reset"), false, false},
	{"bridge request", fmt.Errorf("%w: status=502", ErrBridgeRequest), false, false},
	{"contract rejected", fmt.Errorf("%w: status=422", ErrBridgeContractRejected), true, false},
	{"context canceled", context.Canceled, false, false},
	{"deadline exceeded", context.DeadlineExceeded, false, false},
	{"fatal", ErrEstimateFatal, true, true},
	{"fatal wrapped", fmt.Errorf("load estimate inputs: %w", ErrEstimateFatal), true, true},
	{"fatal joined", errors.Join(errors.New("other"), ErrEstimateFatal), true, true},
	{"text only", errors.New(ErrEstimateFatal.Error()), false, false},
	{"fatal and contract rejected", errors.Join(ErrEstimateFatal, ErrBridgeContractRejected), true, true},
}

// A candidate set of 600 units is two chunks (500 + 100), so failing call 0
// is the first chunk and call 1 is a later chunk with an earlier chunk ok.
const fatalChunkCandidates = 600

func unitsInSuccessfulChunks(failAt int) int {
	if failAt == 0 {
		return fatalChunkCandidates - dispatchBudgetEstimateMaxUnitIDs
	}
	return dispatchBudgetEstimateMaxUnitIDs
}

func unitRowsSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(ctx, `
SELECT coalesce(string_agg(row_to_json(u)::text, ',' ORDER BY id), '')
FROM public.sync_run_units u`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestEstimateErrorClassesAcrossEveryConsumerAndChunkPosition(t *testing.T) {
	t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedManyCandidateUnits(t, ctx, pool, fatalChunkCandidates)
		// 600 running units in one OTHER run: the population
		// activeBudgetConsumption re-estimates (two chunks of 500 + 100).
		const activeRunID = "00000000-0000-4000-8000-0000000001b0"
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units
 (id, sync_run_id, org_id, integration_id, source_id, provider, dataset_key, cost_class,
  status, updated_at, lease_expires_at, result)
SELECT ('00000000-0000-4000-9100-' || lpad(to_hex(n), 12, '0'))::uuid, $1::uuid, 'org-1',
  '00000000-0000-4000-8000-000000000010'::uuid, '00000000-0000-4000-8000-000000000011'::uuid,
  'github', 'commits', 'rest_core', 'running', now(), now() + interval '1 hour', '{}'::json
FROM generate_series(1, $2) AS n`, activeRunID, fatalChunkCandidates); err != nil {
			t.Fatal(err)
		}
		budgetKey := budgetKeyFor(estimateFor(1, "github", "rest_core", "core")[0].Bucket, "core")

		for _, class := range estimateErrorClasses {
			for failAt := 0; failAt < 4; failAt++ {
				name := fmt.Sprintf("%s/call %d", class.name, failAt)
				// Calls 0-1 estimate the candidates (500 + 100); calls 2-3 estimate the
				// active baseline. A contract rejection fails the pass only at a
				// candidate call, as on main; the fatal sentinel fails it at both.
				enforceFails := class.fatal && (failAt < 2 || class.estimateFatal)
				t.Run("enforceRun/"+name, func(t *testing.T) {
					before := unitRowsSnapshot(t, ctx, pool)
					tx, err := pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = tx.Rollback(ctx) }()
					estimator := &scriptedEstimator{failAt: failAt, err: class.err}
					result, err := enforceRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, nil, pgNow(), nil)
					if enforceFails {
						if !errors.Is(err, class.err) {
							t.Fatalf("error=%v, want the scripted %q error: the pass must fail closed", err, class.name)
						}
						if len(result.deferredUnitIDs) != 0 {
							t.Fatalf("deferred=%v, want none from a failed pass", result.deferredUnitIDs)
						}
						if err := tx.Commit(ctx); err != nil {
							t.Fatal(err)
						}
						if after := unitRowsSnapshot(t, ctx, pool); after != before {
							t.Fatalf("a failed pass wrote unit rows:\nbefore=%s\nafter=%s", before, after)
						}
						return
					}
					if err != nil {
						t.Fatalf("error=%v, want nil: this class fails the chunk open, as on main", err)
					}
					if len(result.deferredUnitIDs) != 0 {
						t.Fatalf("deferred=%v, want none: a failed-open chunk is admitted unchecked", result.deferredUnitIDs)
					}
				})
			}
		}

		for _, class := range estimateErrorClasses {
			for _, failAt := range []int{0, 1} {
				name := fmt.Sprintf("%s/chunk %d", class.name, failAt)
				t.Run("observeRun/"+name, func(t *testing.T) {
					tx, err := pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = tx.Rollback(ctx) }()
					estimator := &scriptedEstimator{failAt: failAt, err: class.err}
					observations, err := observeRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, pgNow())
					if err != nil {
						t.Fatalf("error=%v, want nil: observeRun degrades a failed chunk for every class", err)
					}
					if want := unitsInSuccessfulChunks(failAt); len(observations) != want {
						t.Fatalf("observations=%d, want %d (units of the chunks that succeeded)", len(observations), want)
					}
				})
				t.Run("activeBudgetConsumption/"+name, func(t *testing.T) {
					tx, err := pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = tx.Rollback(ctx) }()
					estimator := &scriptedEstimator{failAt: failAt, err: class.err}
					consumed, err := activeBudgetConsumption(ctx, tx, estimator, nil, pgNow(), map[string]bool{budgetKey: true})
					if class.estimateFatal {
						if !errors.Is(err, ErrEstimateFatal) {
							t.Fatalf("error=%v, want ErrEstimateFatal: the baseline must not degrade on a fatal class", err)
						}
						return
					}
					if err != nil {
						t.Fatalf("error=%v, want nil: activeBudgetConsumption degrades a failed chunk for every non-fatal class", err)
					}
					if want := unitsInSuccessfulChunks(failAt); consumed[budgetKey] != want {
						t.Fatalf("consumed=%d, want %d (units of the chunks that succeeded)", consumed[budgetKey], want)
					}
				})
			}
		}
	})
}

// TestEstimateErrorSentinelsAreAllClassified pins the sentinel errors the
// estimate path can return: a new Err* declared in the estimate-path files
// fails here until it is added to estimateErrorClasses and its row in the
// gate's failure table is decided.
func TestEstimateErrorSentinelsAreAllClassified(t *testing.T) {
	fset := token.NewFileSet()
	found := map[string]bool{}
	for _, file := range []string{"bridge.go", "budget_estimate.go", "budget_consumption.go", "in_process_budget_estimator.go"} {
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				for _, name := range spec.(*ast.ValueSpec).Names {
					if strings.HasPrefix(name.Name, "Err") {
						found[name.Name] = true
					}
				}
			}
		}
	}
	got := make([]string, 0, len(found))
	for name := range found {
		got = append(got, name)
	}
	sort.Strings(got)
	want := []string{"ErrBridgeContractRejected", "ErrBridgeRequest", "ErrEstimateFatal", "ErrInvalidBridge"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("estimate-path sentinels = %v, pinned %v -- classify the new one in estimateErrorClasses and the failure table", got, want)
	}
}

// TestEveryEstimateSentinelHasAClassifiedErrorCase closes the gap between the
// name pin above and the runtime table: each sentinel must appear, by
// errors.Is, in an error class the consumer matrix executes.
func TestEveryEstimateSentinelHasAClassifiedErrorCase(t *testing.T) {
	sentinels := map[string]error{
		"ErrBridgeRequest":          ErrBridgeRequest,
		"ErrBridgeContractRejected": ErrBridgeContractRejected,
		"ErrEstimateFatal":          ErrEstimateFatal,
	}
	for name, sentinel := range sentinels {
		covered := false
		for _, class := range estimateErrorClasses {
			if errors.Is(class.err, sentinel) {
				covered = true
			}
		}
		if !covered {
			t.Fatalf("%s has no error class in estimateErrorClasses, so no consumer cell executes it", name)
		}
	}
	// ErrInvalidBridge is returned by DispatchBudgetEstimate only for a nil
	// bridge or an empty unit list, both short-circuited by every caller
	// before the call; it never reaches a consumer's error branch.
}
