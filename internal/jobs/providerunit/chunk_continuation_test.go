package providerunit

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// TestChunkContinuationDeferRecordsTheMetric pins the CHAOS-4592 gate round
// 3, P3 fix. Handler.Work's ChunkContinuationDelay branch
// (providerunit.go:685) calls handler.ProviderMetrics.RecordChunkContinuation
// on every deferred continuation, but before this test the ONLY coverage was
// TestChunkContinuationCounterRendersPerProviderAndDataset
// (providerfoundation/unit_claim_failure_metrics_test.go), which calls
// RecordChunkContinuation directly -- never through Handler.Work at all. A
// regression deleting or misgating that call site would leave the counter
// permanently silent in production (the exact "no durable signal of how many
// continuations were needed" problem this counter exists to close, per its
// own doc comment) while every existing test stayed green.
//
// This drives Handler.Work end-to-end -- the same BuildExecutor-returns-an-
// error harness TestProviderRateLimitDefersWithoutConsumingTheAttempt uses
// for the sibling RateLimitDeferralRepository path -- with a real
// *providerfoundation.Metrics and asserts the rendered Prometheus output
// actually carries the increment.
func TestChunkContinuationDeferRecordsTheMetric(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	metrics := providerfoundation.NewMetrics()
	handler := &Handler{
		Repository:      repository,
		ProviderMetrics: metrics,
		LeaseDuration:   time.Minute,
		Heartbeat:       10 * time.Second,
		Now:             func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, providersync.ChunkContinuationError{
				Next: now.Add(30 * time.Second),
			}
		},
	}
	execution := providerExecution(unit, now, 5)
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)
	if _, snoozed := jobruntime.SnoozeDelay(err); !snoozed {
		t.Fatalf("Work() = %v; want an attempt-neutral snooze", err)
	}
	if repository.chunkContinuationDeferrals != 1 {
		t.Fatalf("chunk_continuation_deferrals=%d, want 1", repository.chunkContinuationDeferrals)
	}

	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	want := `dev_health_provider_chunk_continuation_total{provider="` + unit.Provider + `",dataset="` + unit.Dataset + `"} 1`
	if !strings.Contains(rendered, want) {
		t.Fatalf("missing %q in:\n%s", want, rendered)
	}
}
