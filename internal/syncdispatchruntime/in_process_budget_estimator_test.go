package syncdispatchruntime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// scriptedRow answers one QueryRow by the first SQL keyword it matches.
type scriptedRow struct{ scan func(dest ...any) error }

func (row scriptedRow) Scan(dest ...any) error { return row.scan(dest...) }

type scriptedDB struct {
	runExists bool
	fenceErr  error
	fence     int
}

func (db scriptedDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	switch {
	case strings.Contains(sql, "SELECT EXISTS"):
		return scriptedRow{func(dest ...any) error { *dest[0].(*bool) = db.runExists; return nil }}
	case strings.Contains(sql, "SELECT count(*) FROM public.sync_run_units"):
		return scriptedRow{func(dest ...any) error {
			if db.fenceErr != nil {
				return db.fenceErr
			}
			*dest[0].(*int) = db.fence
			return nil
		}}
	}
	// The per-unit bootstrap: the unit row is gone.
	return scriptedRow{func(...any) error { return pgx.ErrNoRows }}
}

type unusedDecryptor struct{}

func (unusedDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return nil, errors.New("decrypt must not run in this test")
}

const (
	estimatorTestOrg  = "00000000-0000-4000-8000-000000000001"
	estimatorTestRun  = "00000000-0000-4000-8000-000000000002"
	estimatorTestUnit = "00000000-0000-4000-8000-000000000003"
)

// TestInProcessEstimatorErrorClasses pins how the estimator's outcomes map
// onto the classes enforceRun, observeRun and activeBudgetConsumption
// already branch on: a refused reference fails enforceRun's pass (contract
// rejected), a failed batch read fails the chunk open (bridge request), and
// a failed unit gets no estimates and a warning while the batch succeeds.
func TestInProcessEstimatorErrorClasses(t *testing.T) {
	getenv := func(string) string { return "" }
	estimate := func(t *testing.T, db scriptedDB, logs *bytes.Buffer) (map[string][]budgetEstimate, error) {
		t.Helper()
		estimator, err := newInProcessBudgetEstimator(db, BudgetEstimatorDependencies{
			Decryptor: unusedDecryptor{}, Getenv: getenv, Logger: slog.New(slog.NewTextHandler(logs, nil)),
		})
		if err != nil {
			t.Fatal(err)
		}
		return estimator.DispatchBudgetEstimate(context.Background(), estimatorTestOrg, estimatorTestRun, []string{estimatorTestUnit})
	}

	for name, db := range map[string]scriptedDB{
		"stale run":        {runExists: false},
		"unit outside run": {runExists: true, fence: 0},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := estimate(t, db, &bytes.Buffer{})
			if !errors.Is(err, ErrBridgeContractRejected) {
				t.Fatalf("err=%v, want ErrBridgeContractRejected", err)
			}
		})
	}

	t.Run("batch read fails", func(t *testing.T) {
		_, err := estimate(t, scriptedDB{runExists: true, fenceErr: errors.New("connection reset")}, &bytes.Buffer{})
		if !errors.Is(err, ErrBridgeRequest) || errors.Is(err, ErrBridgeContractRejected) {
			t.Fatalf("err=%v, want ErrBridgeRequest and not a contract rejection", err)
		}
	})

	t.Run("unit fails alone", func(t *testing.T) {
		var logs bytes.Buffer
		estimates, err := estimate(t, scriptedDB{runExists: true, fence: 1}, &logs)
		if err != nil {
			t.Fatalf("err=%v, want the batch to succeed", err)
		}
		got, present := estimates[estimatorTestUnit]
		if !present || len(got) != 0 {
			t.Fatalf("estimates=%v, want an empty entry for the failed unit", estimates)
		}
		if !strings.Contains(logs.String(), "dispatch_sync_run.budget_estimate_unit_failed") ||
			!strings.Contains(logs.String(), estimatorTestUnit) {
			t.Fatalf("the unit failure was not logged: %s", logs.String())
		}
	})

	t.Run("missing dependency", func(t *testing.T) {
		if _, err := newInProcessBudgetEstimator(nil, BudgetEstimatorDependencies{Decryptor: unusedDecryptor{}, Getenv: getenv}); !errors.Is(err, ErrInvalidBridge) {
			t.Fatalf("nil db: err=%v", err)
		}
		if _, err := NewInProcessBudgetEstimator(BudgetEstimatorDependencies{Decryptor: unusedDecryptor{}, Getenv: getenv}); !errors.Is(err, ErrInvalidBridge) {
			t.Fatalf("nil pool: err=%v", err)
		}
		var estimator *InProcessBudgetEstimator
		if _, err := estimator.DispatchBudgetEstimate(context.Background(), estimatorTestOrg, estimatorTestRun, []string{estimatorTestUnit}); !errors.Is(err, ErrInvalidBridge) {
			t.Fatalf("nil estimator: err=%v", err)
		}
	})
}
