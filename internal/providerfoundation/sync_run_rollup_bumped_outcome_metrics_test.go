package providerfoundation

import (
	"bytes"
	"strings"
	"testing"
)

// TestRecordSyncRunRollupBumpedDelegatesToTheProcessWideSingleton pins
// CHAOS-4586's codex round 1 P1 fix: dev_health_sync_run_rollup_bumped_total
// must NOT be a per-Metrics-instance field (a worker process constructing
// TWO Metrics instances -- one per buildProviderSyncWorker,one per
// buildSyncCoordinatorWorker -- would then declare this metric's HELP/TYPE
// twice in one scrape, which most Prometheus parsers reject outright). This
// is the ONLY test in the package that calls RecordSyncRunRollupBumped, so
// exact counts against the process-wide singleton are safe here.
//
// It also pins CHAOS-4586's widened path vocabulary: syncdispatchruntime's
// five denial/exhaustion path labels must render as their OWN series, not
// fold into "other" the way an unrecognized value does. outcome and path
// are two INDEPENDENT closed vocabularies (chris: "same family name so it
// is one counter with a path label"), each folding unknown values to
// "other" on its own axis.
func TestRecordSyncRunRollupBumpedDelegatesToTheProcessWideSingleton(t *testing.T) {
	// Deliberately TWO separate, otherwise-unrelated *Metrics instances
	// (never registered with any health.Registry, matching
	// buildSyncCoordinatorWorker's own syncCoordinatorMetrics) -- proving
	// the recorded counts land on the ONE shared singleton regardless of
	// which instance the method is called on.
	first := NewMetrics()
	second := NewMetrics()
	first.RecordSyncRunRollupBumped("success", "provider_unit")
	second.RecordSyncRunRollupBumped("failed", "provider_unit")
	first.RecordSyncRunRollupBumped("failed", "denied")
	second.RecordSyncRunRollupBumped("failed", "unroutable")
	first.RecordSyncRunRollupBumped("failed", "invalid_claim")
	second.RecordSyncRunRollupBumped("failed", "budget_exhausted")
	first.RecordSyncRunRollupBumped("failed", "reference_discovery_failed")
	second.RecordSyncRunRollupBumped("Failed", "Budget_Exhausted") // case-insensitive, same series as above
	first.RecordSyncRunRollupBumped("failed", "a-path-nobody-registered")
	second.RecordSyncRunRollupBumped("an-outcome-nobody-registered", "provider_unit")
	var nilMetrics *Metrics
	nilMetrics.RecordSyncRunRollupBumped("failed", "denied") // no-op, never reaches the singleton

	// Neither instance's OWN WritePrometheus emits this metric name -- the
	// actual P1 regression guard: two Metrics instances in one process must
	// never each declare dev_health_sync_run_rollup_bumped_total.
	var perInstance bytes.Buffer
	if err := first.WritePrometheus(&perInstance); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(perInstance.String(), "dev_health_sync_run_rollup_bumped_total") {
		t.Fatalf("Metrics.WritePrometheus must never emit dev_health_sync_run_rollup_bumped_total (CHAOS-4586 P1: it is a process-wide singleton, not a per-instance field):\n%s", perInstance.String())
	}

	var output bytes.Buffer
	if err := SyncRunRollupBumpedMetricsSource().WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_sync_run_rollup_bumped_total counter",
		`dev_health_sync_run_rollup_bumped_total{outcome="success",path="provider_unit"} 1`,
		`dev_health_sync_run_rollup_bumped_total{outcome="failed",path="provider_unit"} 1`,
		`dev_health_sync_run_rollup_bumped_total{outcome="failed",path="denied"} 1`,
		`dev_health_sync_run_rollup_bumped_total{outcome="failed",path="unroutable"} 1`,
		`dev_health_sync_run_rollup_bumped_total{outcome="failed",path="invalid_claim"} 1`,
		`dev_health_sync_run_rollup_bumped_total{outcome="failed",path="budget_exhausted"} 2`,
		`dev_health_sync_run_rollup_bumped_total{outcome="failed",path="reference_discovery_failed"} 1`,
		`dev_health_sync_run_rollup_bumped_total{outcome="failed",path="other"} 1`,
		`dev_health_sync_run_rollup_bumped_total{outcome="other",path="provider_unit"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if got := strings.Count(rendered, "dev_health_sync_run_rollup_bumped_total{"); got != 9 {
		t.Fatalf("series=%d want 9 in:\n%s", got, rendered)
	}
}
