package daily

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/riverqueue/river/rivertype"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// The sweep must not redrive a finalize River can still run: every attempt's
// timeout plus the largest backoff before each retry stays below the horizon.
func TestFinalizeStrandHorizonExceedsTheRetryBudget(t *testing.T) {
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := registry.Descriptor(jobcontract.KindDailyMetricsFinalize)
	if !ok {
		t.Fatalf("registry has no %s", jobcontract.KindDailyMetricsFinalize)
	}
	if descriptor.MaxAttempts < 1 || descriptor.Timeout <= 0 {
		t.Fatalf("descriptor %+v has no attempt budget", descriptor)
	}
	base := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	budget := time.Duration(descriptor.MaxAttempts) * descriptor.Timeout
	for attempt := 1; attempt < descriptor.MaxAttempts; attempt++ {
		var longest time.Duration
		for id := int64(1); id <= 1000; id++ {
			next := jobruntime.NextRetryAt(descriptor, &rivertype.JobRow{ID: id, Attempt: attempt, AttemptedAt: &base})
			if next.IsZero() {
				t.Fatalf("retry policy %q schedules no retry", descriptor.RetryPolicy)
			}
			if delay := next.Sub(base); delay > longest {
				longest = delay
			}
		}
		budget += longest
	}
	t.Logf("retry budget %s (max_attempts %d, timeout %s), horizon %s",
		budget, descriptor.MaxAttempts, descriptor.Timeout, finalizeStrandHorizon)
	if finalizeStrandHorizon <= budget {
		t.Fatalf("finalizeStrandHorizon %s <= retry budget %s", finalizeStrandHorizon, budget)
	}
}
