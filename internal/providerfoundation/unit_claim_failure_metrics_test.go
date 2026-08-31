package providerfoundation

import (
	"bytes"
	"strings"
	"testing"
)

// CHAOS-4078: the planned/failed-by-dataset counters CHAOS-4125's own
// forensics comment asked for. A dataset stuck at 100% failure previously
// had no live series at all -- only a durable sync_run_units row an operator
// had to query for by hand.
func TestUnitClaimedCounterRendersPerProviderAndDataset(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordUnitClaimed("GitHub", "pr-comments")
	metrics.RecordUnitClaimed("github", "pr-comments")
	metrics.RecordUnitClaimed("github", "cicd")
	metrics.RecordUnitClaimed("mystery", "org-4711/whatever")
	var nilMetrics *Metrics
	nilMetrics.RecordUnitClaimed("github", "prs")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_provider_unit_claimed_total counter",
		`dev_health_provider_unit_claimed_total{provider="github",dataset="pr-comments"} 2`,
		`dev_health_provider_unit_claimed_total{provider="github",dataset="cicd"} 1`,
		`dev_health_provider_unit_claimed_total{provider="other",dataset="other"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if got := strings.Count(rendered, "dev_health_provider_unit_claimed_total{"); got != 3 {
		t.Fatalf("series=%d want 3 in:\n%s", got, rendered)
	}
}

// CHAOS-4592: a chunked route that never completes within one attempt (the
// dev-health-ops cicd/tests walk observed reaching 178+ continuations before
// being cancelled) previously left no durable signal of HOW MANY
// continuations were needed -- only river_job.metadata->>'snoozes' on
// whichever job row happened to still exist, which resets to zero on
// re-dispatch. This counter is process-durable across re-dispatches instead.
func TestChunkContinuationCounterRendersPerProviderAndDataset(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordChunkContinuation("GitHub", "cicd")
	metrics.RecordChunkContinuation("github", "cicd")
	metrics.RecordChunkContinuation("gitlab", "cicd")
	var nilMetrics *Metrics
	nilMetrics.RecordChunkContinuation("github", "cicd")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_provider_chunk_continuation_total counter",
		`dev_health_provider_chunk_continuation_total{provider="github",dataset="cicd"} 2`,
		`dev_health_provider_chunk_continuation_total{provider="gitlab",dataset="cicd"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if got := strings.Count(rendered, "dev_health_provider_chunk_continuation_total{"); got != 2 {
		t.Fatalf("series=%d want 2 in:\n%s", got, rendered)
	}
}

func TestUnitFailedCounterRendersPerProviderDatasetAndReason(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordUnitFailed("GitHub", "pr-comments", "feature_disabled")
	metrics.RecordUnitFailed("github", "pr-comments", "feature_disabled")
	metrics.RecordUnitFailed("github", "tests", "pagination_incomplete")
	metrics.RecordUnitFailed("gitlab", "cicd", "provider_unit_exhausted")
	metrics.RecordUnitFailed("github", "prs", "a-reason-nobody-registered")
	var nilMetrics *Metrics
	nilMetrics.RecordUnitFailed("github", "prs", "feature_disabled")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_provider_unit_failed_total counter",
		`dev_health_provider_unit_failed_total{provider="github",dataset="pr-comments",reason="feature_disabled"} 2`,
		`dev_health_provider_unit_failed_total{provider="github",dataset="tests",reason="pagination_incomplete"} 1`,
		`dev_health_provider_unit_failed_total{provider="gitlab",dataset="cicd",reason="provider_unit_exhausted"} 1`,
		`dev_health_provider_unit_failed_total{provider="github",dataset="prs",reason="other"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if got := strings.Count(rendered, "dev_health_provider_unit_failed_total{"); got != 4 {
		t.Fatalf("series=%d want 4 in:\n%s", got, rendered)
	}
}
