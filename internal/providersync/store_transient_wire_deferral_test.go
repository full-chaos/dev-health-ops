package providersync

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type wireDeferralRun struct {
	err       error
	deferrals int
}

// runWireDeferral swaps the process default logger, so its callers must not run in parallel. It runs one deferral write ("rate_limit" or "budget") against a
// storeWire. priorDeferrals is the contention count the claim carries.
func runWireDeferral(t *testing.T, w *storeWire, write string, priorDeferrals int) wireDeferralRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	claim := nativeTestClaim("github", "commits")
	if priorDeferrals > 0 {
		claim.Result = map[string]any{"provider_budget_contention_deferrals": float64(priorDeferrals)}
	}
	w.sourceID = claim.SourceID
	repository, err := NewPostgresRepository(w.pool(t, ctx), providerfoundation.NewMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	repository.storeRetrySleep = func(context.Context, int) error { return nil }
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&lockedLogBuffer{}, nil)))
	defer slog.SetDefault(previous)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	var run wireDeferralRun
	switch write {
	case "rate_limit":
		run.err = repository.DeferForRateLimit(ctx, claim, now.Add(time.Minute), now, RateLimitObservation{}, RateLimitEpisodeStep{})
	case "budget":
		run.deferrals, run.err = repository.DeferForBudgetContention(ctx, claim, now.Add(5*time.Second), now)
	default:
		t.Fatalf("unknown write %q", write)
	}
	return run
}

// The budget-contention deferral commits and its reply is lost: the write is
// read back and reported as done, once, with the count the statement would have
// returned.
func TestBudgetContentionDeferralWithALostReplyAfterTheCommitIsReadBack(t *testing.T) {
	w := &storeWire{faults: []*wireFault{{step: "commit", code: "08006", times: 1}}}
	run := runWireDeferral(t, w, "budget", 0)
	if run.err != nil || run.deferrals != 1 || w.updates != 1 || w.commits != 1 || w.reads != 1 {
		t.Fatalf("err=%v deferrals=%d updates=%d commits=%d reads=%d, want nil, 1, 1, 1, 1 (read back once, not written twice)",
			run.err, run.deferrals, w.updates, w.commits, w.reads)
	}
}

// The read-back path takes the count from the claim: a unit that carried 2
// contention deferrals gets 3.
func TestBudgetContentionDeferralReadBackCountsFromTheClaim(t *testing.T) {
	w := &storeWire{faults: []*wireFault{{step: "commit", code: "08006", times: 1}}}
	run := runWireDeferral(t, w, "budget", 2)
	if run.err != nil || run.deferrals != 3 || w.commits != 1 || w.reads != 1 {
		t.Fatalf("err=%v deferrals=%d commits=%d reads=%d, want nil, 3, 1, 1", run.err, run.deferrals, w.commits, w.reads)
	}
}

// Control: an acknowledged write returns the statement's own RETURNING value.
func TestBudgetContentionDeferralWithAnAcknowledgedCommitReturnsTheStatementCount(t *testing.T) {
	w := &storeWire{}
	run := runWireDeferral(t, w, "budget", 7)
	if run.err != nil || run.deferrals != 1 || w.updates != 1 || w.commits != 1 || w.reads != 0 {
		t.Fatalf("err=%v deferrals=%d updates=%d commits=%d reads=%d, want nil, 1 (the RETURNING value), 1, 1, 0",
			run.err, run.deferrals, w.updates, w.commits, w.reads)
	}
}

// Controls that hold before and after: no row for this owner, and a lost reply
// while another owner holds the unit, are ErrLeaseLost for both writes.
func TestDeferralWritesThatDidNotLandKeepTheirAnswers(t *testing.T) {
	for _, write := range []string{"rate_limit", "budget"} {
		t.Run(write+", no row", func(t *testing.T) {
			w := &storeWire{done: true}
			if run := runWireDeferral(t, w, write, 0); !errors.Is(run.err, ErrLeaseLost) {
				t.Fatalf("err=%v, want ErrLeaseLost", run.err)
			}
		})
		t.Run(write+", lost reply, another owner holds the unit", func(t *testing.T) {
			w := &storeWire{firstCommitState: "other", faults: []*wireFault{{step: "commit", code: "08006", times: 1}}}
			if run := runWireDeferral(t, w, write, 0); !errors.Is(run.err, ErrLeaseLost) {
				t.Fatalf("err=%v, want ErrLeaseLost", run.err)
			}
		})
	}
}

// A deadlock refused the write at COMMIT (nothing applied): the write is retried
// in place by retryUnitWrite (a deadlock is transient in IsTransientStoreError,
// not through dialclass) and lands.
func TestDeferralWritesRetryADeadlockInPlace(t *testing.T) {
	for _, write := range []string{"rate_limit", "budget"} {
		t.Run(write, func(t *testing.T) {
			w := &storeWire{faults: []*wireFault{{step: "commit", code: "40P01", times: 1}}}
			run := runWireDeferral(t, w, write, 0)
			if run.err != nil || w.updates != 2 || w.commits != 1 {
				t.Fatalf("err=%v updates=%d commits=%d, want nil, 2, 1 (retried in place after the deadlock)", run.err, w.updates, w.commits)
			}
		})
	}
}
