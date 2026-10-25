package workerservice

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

const leaseTestWindow = 60 * time.Second

func leaseTestClaim(t0 time.Time) *claimLiveness {
	claim := newClaimLiveness(t0, []string{"heavy"})
	claim.SetStaleWindow(leaseTestWindow)
	claim.markRuntimeLive()
	return claim
}

func leaseTestSnapshot(capacity, running, available int64) riverstore.QueueTelemetrySnapshot {
	return riverstore.QueueTelemetrySnapshot{
		Jobs:            []riverstore.QueueJobTelemetry{{Queue: "heavy", Kind: "metrics", Available: available}},
		QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "heavy", Capacity: capacity, Running: running}},
	}
}

func leaseTestJudge(claim *claimLiveness, snapshot riverstore.QueueTelemetrySnapshot, polls []time.Time) queueVerdict {
	var verdict queueVerdict
	for _, at := range polls {
		facts := collectQueueFacts(snapshot, claim, at, false)
		verdict = judgeQueue(facts[0])
	}
	return verdict
}

func leaseTestPolls(t0 time.Time) []time.Time {
	return []time.Time{t0.Add(10 * time.Second), t0.Add(80 * time.Second), t0.Add(150 * time.Second)}
}

func TestLeaseContentionDoesNotTripSlotStuck(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC)
	claim := leaseTestClaim(t0)
	claim.leaseWaitStarted("heavy", t0.Add(time.Second))
	claim.leaseWaitStarted("heavy", t0.Add(2*time.Second))
	got := leaseTestJudge(claim, leaseTestSnapshot(2, 2, 900), leaseTestPolls(t0))
	if got != verdictHealthy {
		t.Fatalf("2/2 slots waiting on a contended lease: verdict %d, want healthy", got)
	}
}

func TestSlotStuckWithoutLeaseWaitStillTrips(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC)
	claim := leaseTestClaim(t0)
	got := leaseTestJudge(claim, leaseTestSnapshot(2, 2, 0), leaseTestPolls(t0))
	if got != verdictSlotStuck {
		t.Fatalf("running slots with no handler and no lease wait: verdict %d, want slot-stuck", got)
	}
}

func TestPartialLeaseWaitStillTrips(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC)
	claim := leaseTestClaim(t0)
	claim.leaseWaitStarted("heavy", t0.Add(time.Second))
	got := leaseTestJudge(claim, leaseTestSnapshot(2, 2, 0), leaseTestPolls(t0))
	if got != verdictSlotStuck {
		t.Fatalf("one of two slots unaccounted: verdict %d, want slot-stuck", got)
	}
}

func TestLeaseWaitCreditEndsAfterLeaseTTLPlusWindow(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC)
	claim := leaseTestClaim(t0)
	claim.leaseWaitStarted("heavy", t0)
	edge := t0.Add(jobruntime.BudgetLeaseTTL + leaseTestWindow)
	if got := claim.leaseWaiting("heavy", edge); got != 1 {
		t.Fatalf("at TTL+window credit = %d, want 1", got)
	}
	if got := claim.leaseWaiting("heavy", edge.Add(time.Nanosecond)); got != 0 {
		t.Fatalf("past TTL+window credit = %d, want 0", got)
	}
	past := []time.Time{edge.Add(time.Second), edge.Add(2 * time.Minute), edge.Add(4 * time.Minute)}
	if got := leaseTestJudge(claim, leaseTestSnapshot(1, 1, 0), past); got != verdictSlotStuck {
		t.Fatalf("a holder that never releases: verdict %d, want slot-stuck", got)
	}
}

func TestLeaseWaitCreditEndsWhenTheWaitEnds(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC)
	claim := leaseTestClaim(t0)
	observer := claimLivenessObserver{liveness: claim}
	labels := jobruntime.JobLabels{Queue: "heavy", Kind: "metrics"}
	same := t0.Add(time.Second)
	observer.BudgetContended(context.Background(), labels, same)
	observer.BudgetContended(context.Background(), labels, same)
	observer.BudgetContended(context.Background(), labels, t0.Add(2*time.Second))
	if got := claim.leaseWaiting("heavy", t0.Add(5*time.Second)); got != 3 {
		t.Fatalf("credit = %d, want 3", got)
	}
	observer.BudgetContentionEnded(context.Background(), labels, same)
	if got := claim.leaseWaiting("heavy", t0.Add(5*time.Second)); got != 2 {
		t.Fatalf("after one end credit = %d, want 2", got)
	}
	observer.BudgetContentionEnded(context.Background(), labels, same)
	observer.BudgetContentionEnded(context.Background(), labels, t0.Add(2*time.Second))
	if got := claim.leaseWaiting("heavy", t0.Add(5*time.Second)); got != 0 {
		t.Fatalf("after every end credit = %d, want 0", got)
	}
	if got := leaseTestJudge(claim, leaseTestSnapshot(2, 2, 0), leaseTestPolls(t0)); got != verdictSlotStuck {
		t.Fatalf("waits ended, slots still outside a handler: verdict %d, want slot-stuck", got)
	}
}

func TestLeaseWaitDoesNotHideAnIdleConsumerWithBacklog(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC)
	claim := leaseTestClaim(t0)
	claim.leaseWaitStarted("heavy", t0.Add(time.Second))
	got := leaseTestJudge(claim, leaseTestSnapshot(4, 0, 900), leaseTestPolls(t0))
	if got != verdictStalledBacklog {
		t.Fatalf("backlog, free capacity, nothing running: verdict %d, want stalled-backlog", got)
	}
}
