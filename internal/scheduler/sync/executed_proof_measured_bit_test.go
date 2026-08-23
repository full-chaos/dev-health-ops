package sync

import (
	"bytes"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/jackc/pgx/v5/pgxpool"
)

func gateMetricValue(t *testing.T, materializer *NativeMaterializer, name string) string {
	t.Helper()
	var output bytes.Buffer
	if err := materializer.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(output.String(), "\n") {
		if strings.HasPrefix(line, name+" ") {
			return strings.TrimPrefix(line, name+" ")
		}
	}
	t.Fatalf("no sample line for %q in:\n%s", name, output.String())
	return ""
}

// TestExecutedProofEvidenceStateSeparatesNeverLoadedFromStale is the
// CHAOS-4124 measured bit.
//
// The outage's two ingredients were that a first-load failure is
// categorically worse than a later one (it has no Proven facts to carry
// forward, so it blocks EVERYTHING), and that nothing distinguished the two.
// devhealth_scheduler_executed_proof_gate_degraded reads 1 for both, so an
// operator looking at it cannot tell a bounded degradation from a total
// planning outage. This asserts the new series moves through all three
// states, and -- the part that matters -- that it reads -1 ONLY in the
// never-loaded one.
func TestExecutedProofEvidenceStateSeparatesNeverLoadedFromStale(t *testing.T) {
	materializer, err := NewNativeMaterializer(&pgxpool.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	const series = "devhealth_scheduler_executed_proof_evidence_state"

	// Unwired: no caller composed the gate, so there is nothing to be stale
	// about. Not an outage state.
	if got := gateMetricValue(t, materializer, series); got != "1" {
		t.Fatalf("unwired state=%s, want 1", got)
	}
	if materializer.HasLoadedExecutedProof() {
		t.Fatal("HasLoadedExecutedProof true before any refresh ran at all")
	}

	// The 4124 state: a FIRST load failed, so the snapshot is empty and
	// Degraded and every non-waived pair is blocked.
	materializer.executedProofRefreshFailuresTotal.Add(1)
	materializer.executedProofLastRefreshOK.Store(false)
	materializer.executedProof.Store(&providersync.ExecutedProofEvidence{
		Proven: map[string]bool{}, Attempted: map[string]bool{}, Degraded: true,
	})
	if got := gateMetricValue(t, materializer, series); got != "-1" {
		t.Fatalf("never-loaded state=%s, want -1 -- this is the state that must page", got)
	}
	if materializer.HasLoadedExecutedProof() {
		t.Fatal("HasLoadedExecutedProof true after only a FAILED refresh")
	}

	// A refresh succeeds. Clean.
	materializer.executedProofLastRefreshOK.Store(true)
	materializer.executedProofEverLoadedOK.Store(true)
	materializer.executedProof.Store(&providersync.ExecutedProofEvidence{
		Proven: map[string]bool{"github/prs": true}, Attempted: map[string]bool{"github/prs": true},
	})
	if got := gateMetricValue(t, materializer, series); got != "1" {
		t.Fatalf("healthy state=%s, want 1", got)
	}
	if !materializer.HasLoadedExecutedProof() {
		t.Fatal("HasLoadedExecutedProof false after a successful refresh")
	}

	// A LATER failure. Degraded, but bounded: proven facts carry forward, so
	// this is 0 and not -1. Readiness must NOT flap here -- turning a bounded
	// degradation into an unhealthy deploy would be its own outage.
	materializer.executedProofRefreshFailuresTotal.Add(1)
	materializer.executedProofLastRefreshOK.Store(false)
	if got := gateMetricValue(t, materializer, series); got != "0" {
		t.Fatalf("stale-after-success state=%s, want 0", got)
	}
	if !materializer.HasLoadedExecutedProof() {
		t.Fatal("a later refresh failure revoked readiness; only a never-loaded gate may")
	}
	if got := gateMetricValue(t, materializer, "devhealth_scheduler_executed_proof_gate_degraded"); got != "1" {
		t.Fatalf("the original degraded gauge stopped reporting a stale snapshot: %s", got)
	}
}

// TestPlannedUnitSeriesMoveOnRealMaterializations is the per-dimension
// non-vacuity check for the planning-volume signals. CHAOS-4124 ran for eight
// hours with planning collapsed 17 datasets to 1 and nothing paged, because a
// zero-unit run completes successfully and nothing counted units at all.
//
// Each series is observed MOVING under the condition it claims to report, and
// held still under the condition it must not report -- a gauge that never
// leaves zero is indistinguishable from one that is not wired.
func TestPlannedUnitSeriesMoveOnRealMaterializations(t *testing.T) {
	materializer, err := NewNativeMaterializer(&pgxpool.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	const (
		planned      = "devhealth_scheduler_planned_units"
		zeroPlanned  = "devhealth_scheduler_zero_planned_occurrences_total"
		materialized = "devhealth_scheduler_materialized_occurrences_total"
	)
	for _, series := range []string{planned, zeroPlanned, materialized} {
		if got := gateMetricValue(t, materializer, series); got != "0" {
			t.Fatalf("%s starts at %s, want 0", series, got)
		}
	}

	// A materialization that planned work.
	materializer.materializedOccurrences.Add(1)
	materializer.plannedUnitsLast.Store(9)
	if got := gateMetricValue(t, materializer, planned); got != "9" {
		t.Fatalf("%s=%s, want 9", planned, got)
	}
	if got := gateMetricValue(t, materializer, materialized); got != "1" {
		t.Fatalf("%s=%s, want 1", materialized, got)
	}
	if got := gateMetricValue(t, materializer, zeroPlanned); got != "0" {
		t.Fatalf("%s=%s, want 0: a productive pass must not count as an empty one",
			zeroPlanned, got)
	}

	// The outage shape: passes keep completing, and plan nothing.
	materializer.materializedOccurrences.Add(1)
	materializer.plannedUnitsLast.Store(0)
	materializer.zeroPlannedOccurrencesTotal.Add(1)
	if got := gateMetricValue(t, materializer, planned); got != "0" {
		t.Fatalf("%s=%s, want 0", planned, got)
	}
	if got := gateMetricValue(t, materializer, zeroPlanned); got != "1" {
		t.Fatalf("%s=%s, want 1", zeroPlanned, got)
	}
	if got := gateMetricValue(t, materializer, materialized); got != "2" {
		t.Fatalf("%s=%s, want 2 -- the denominator that makes the ratio readable",
			materialized, got)
	}
}
