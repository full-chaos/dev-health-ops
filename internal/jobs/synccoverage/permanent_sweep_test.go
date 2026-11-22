package synccoverage

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	coverage "github.com/full-chaos/dev-health-ops/internal/synccoverage"
)

// A config that fails the same way on every attempt (a LimitError) must not make the sweep retryable: River would run
// it MaxAttempts (3) times and discard it.
func TestHandlerDoesNotRetryAnAllDeterministicFailureSet(t *testing.T) {
	t.Parallel()
	projector := &recordingProjector{result: coverage.RefreshResult{
		Refreshed: 4, Failed: 1,
		Failures: []coverage.RefreshFailure{{OrgID: "org-a", ConfigID: uuid.New(),
			Err: &coverage.LimitError{Bound: "backfill_pair", Count: 20001, Max: 20000}}},
	}}
	handler, err := NewHandler(projector, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), coverageExecution(100))
	if err == nil {
		t.Fatal("Work() = nil: a partial sweep must not be success")
	}
	if !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("Work() = %v, want permanent: the same LimitError fails all 3 attempts", err)
	}
}
