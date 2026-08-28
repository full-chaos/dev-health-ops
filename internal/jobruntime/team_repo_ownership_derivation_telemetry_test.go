package jobruntime

import (
	"strings"
	"testing"
)

// TestObserveTeamRepoOwnershipDerivationExposesCounterAndHistogram is the
// audit-required coverage for CHAOS-4365 item 1b's telemetry (per-file fork
// audit, team-lead order). ObserveTeamRepoOwnershipDerivation recorded into
// collector.teamRepoOwnershipDerivation and
// collector.teamRepoOwnershipDerivationRowCount, but PrometheusText() never
// rendered either field -- the call site was simply missing, so the data
// was recorded in memory and never actually exported. This test would have
// caught that: it asserts the counter and histogram both appear in the
// exposition text with the observed values.
func TestObserveTeamRepoOwnershipDerivationExposesCounterAndHistogram(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamRepoOwnershipDerivation(TeamRepoOwnershipDerivationOutcomeRowsWritten, 5); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamRepoOwnershipDerivation(TeamRepoOwnershipDerivationOutcomeRowsWritten, 3); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamRepoOwnershipDerivation(TeamRepoOwnershipDerivationOutcomeNoSignal, 0); err != nil {
		t.Fatal(err)
	}
	// error is deliberately left unobserved so its pre-seeded zero (asserted
	// below) is proven, not merely assumed.

	text := collector.PrometheusText()
	if !strings.Contains(text, "# HELP dev_health_team_repo_ownership_derivation_total ") {
		t.Fatalf("missing counter HELP line:\n%s", text)
	}
	if !strings.Contains(text, "# HELP dev_health_team_repo_ownership_derivation_row_count ") {
		t.Fatalf("missing histogram HELP line:\n%s", text)
	}
	for _, want := range []string{
		`dev_health_team_repo_ownership_derivation_total{outcome="rows_written"} 2`,
		`dev_health_team_repo_ownership_derivation_total{outcome="no_signal"} 1`,
		`dev_health_team_repo_ownership_derivation_total{outcome="error"} 0`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Fatalf("missing exposition line %q:\n%s", want, text)
		}
	}
	if !strings.Contains(text, `dev_health_team_repo_ownership_derivation_row_count_count 2`) {
		t.Fatalf("expected the row-count histogram to have observed exactly the 2 rows_written outcomes (not the no_signal one):\n%s", text)
	}
}

// TestObserveTeamRepoOwnershipDerivationRejectsUnregisteredOutcome pins the
// same closed-outcome-set guard every other bounded outcome enum in this
// file has.
func TestObserveTeamRepoOwnershipDerivationRejectsUnregisteredOutcome(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamRepoOwnershipDerivation(TeamRepoOwnershipDerivationOutcome("bogus"), 0); err == nil {
		t.Fatal("expected an unregistered outcome to be rejected")
	}
}
