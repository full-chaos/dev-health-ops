package rivercompat

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestDirectCompatibilityProbe(t *testing.T) {
	databaseURL := os.Getenv("RIVER_COMPAT_DIRECT_URL")
	if databaseURL == "" {
		t.Skip("set RIVER_COMPAT_DIRECT_URL to run the direct River integration probe")
	}
	runIntegrationProbe(t, databaseURL, ModeDirect)
}

func TestPollOnlyCompatibilityProbe(t *testing.T) {
	databaseURL := os.Getenv("RIVER_COMPAT_PGBOUNCER_URL")
	if databaseURL == "" {
		t.Skip("set RIVER_COMPAT_PGBOUNCER_URL to run the PollOnly River integration probe")
	}
	runIntegrationProbe(t, databaseURL, ModePollOnly)
}

func runIntegrationProbe(t *testing.T, databaseURL string, mode Mode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := Run(ctx, Options{
		DatabaseURL:       databaseURL,
		FetchPollInterval: 100 * time.Millisecond,
		MaxAttempts:       3,
		Mode:              mode,
		Priority:          2,
		Queue:             "chaos3034",
		Samples:           5,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Workload == nil || result.Workload.Execute == nil || result.Workload.Cancel == nil || result.Workload.Recovery == nil || result.Workload.Scheduled == nil {
		t.Fatalf("Run() workload incomplete: %#v", result.Workload)
	}
	if result.Workload.Execute.State != "completed" {
		t.Fatalf("execute state = %q, want completed", result.Workload.Execute.State)
	}
	if result.Workload.ExecuteLatencyMS.Count != 5 {
		t.Fatalf("execute latency sample count = %d, want 5", result.Workload.ExecuteLatencyMS.Count)
	}
	if !result.Workload.ExecuteLatencyMS.WithinLimit {
		t.Fatalf("execute p95 = %.3fms, exceeds %.3fms limit", result.Workload.ExecuteLatencyMS.P95, result.Workload.ExecuteLatencyMS.Limit)
	}
	if result.Workload.RunningCancellation == nil {
		t.Fatal("running cancellation evidence is missing")
	}
	if mode == ModeDirect {
		if !result.Workload.RunningCancellation.CrossClientContextCancelled || result.Workload.RunningCancellation.ProbeReleaseUsed {
			t.Fatalf("direct cancellation result = %#v", result.Workload.RunningCancellation)
		}
		if result.Workload.Cancel.Outcome != "running_context_cancelled_cross_client" {
			t.Fatalf("direct cancel outcome = %q", result.Workload.Cancel.Outcome)
		}
	} else {
		if result.Workload.RunningCancellation.CrossClientContextCancelled ||
			result.Workload.RunningCancellation.SameClientContextCancelled ||
			!result.Workload.RunningCancellation.ProbeReleaseUsed {
			t.Fatalf("PollOnly cancellation result = %#v", result.Workload.RunningCancellation)
		}
		if result.Workload.Cancel.Outcome != "running_cancel_not_propagated_probe_released" {
			t.Fatalf("PollOnly cancel outcome = %q", result.Workload.Cancel.Outcome)
		}
	}
	if result.Workload.Recovery.Attempt != 2 {
		t.Fatalf("recovery attempt = %d, want 2", result.Workload.Recovery.Attempt)
	}
	if !result.Workload.Scheduled.Scheduled {
		t.Fatal("scheduled job did not preserve a future scheduled_at")
	}
}
