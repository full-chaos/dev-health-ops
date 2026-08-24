package jobruntime

import (
	"strings"
	"testing"
)

func newExternalTelemetryCollector(t *testing.T) *MetricsCollector {
	t.Helper()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	return collector
}

// TestExternalIngestCountersExposeBothHalvesOfTheOutcome pins the pair. A sunk
// counter alone cannot tell a quiet day from a registry refusing everything --
// both are a flat line -- so the exposition has to carry the refusals beside
// it or the signal is unreadable.
func TestExternalIngestCountersExposeBothHalvesOfTheOutcome(t *testing.T) {
	collector := newExternalTelemetryCollector(t)
	if err := collector.ObserveExternalProjectMembershipsSunk("github", 3); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveExternalProjectMembershipsSunk("github", 2); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveExternalProjectMembershipsSunk("linear", 1); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveExternalKindRefused("jira", ExternalRefusedUnsupportedKind); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveExternalKindRefused("jira", ExternalRefusedUnsupportedKind); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveExternalKindRefused("jira", ExternalRefusedInvalidField); err != nil {
		t.Fatal(err)
	}
	text := collector.PrometheusText()
	for _, want := range []string{
		`worker_external_project_memberships_sunk_total{provider="github"} 5`,
		`worker_external_project_memberships_sunk_total{provider="linear"} 1`,
		`worker_external_record_refused_total{source_system="jira",reason="invalid_field"} 1`,
		`worker_external_record_refused_total{source_system="jira",reason="unsupported_kind_for_system"} 2`,
		"# TYPE worker_external_project_memberships_sunk_total counter",
		"# TYPE worker_external_record_refused_total counter",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("exposition is missing %q\n%s", want, text)
		}
	}
}

// TestExternalIngestCountersBoundTheirLabels is the cardinality guard. Both
// label values originate in a customer-pushed batch, so an unbounded label
// here is a customer-controlled series explosion, not a cosmetic defect.
func TestExternalIngestCountersBoundTheirLabels(t *testing.T) {
	collector := newExternalTelemetryCollector(t)
	// An unrecognised source system folds into a single shared bucket rather
	// than minting a series per value seen.
	for _, system := range []string{"attacker-controlled-1", "attacker-controlled-2", ""} {
		if err := collector.ObserveExternalProjectMembershipsSunk(system, 1); err != nil {
			t.Fatal(err)
		}
		if err := collector.ObserveExternalKindRefused(system, ExternalRefusedInvalidField); err != nil {
			t.Fatal(err)
		}
	}
	text := collector.PrometheusText()
	if !strings.Contains(text, `worker_external_project_memberships_sunk_total{provider="unknown"} 3`) {
		t.Errorf("unknown source systems did not fold into one series:\n%s", text)
	}
	if !strings.Contains(text, `worker_external_record_refused_total{source_system="unknown",reason="invalid_field"} 3`) {
		t.Errorf("unknown source systems did not fold into one refusal series:\n%s", text)
	}
	if strings.Contains(text, "attacker-controlled") {
		t.Errorf("a caller-supplied label value reached the exposition:\n%s", text)
	}
}

// TestExternalRefusalReasonSetMatchesTheIngestPath is what stops the reason
// vocabulary from drifting. A refusal path added to normalizeExternalRecords
// without a matching constant here would be REJECTED at runtime rather than
// silently uncounted -- and this test is what makes that a build-time
// conversation instead of a production surprise.
func TestExternalRefusalReasonSetMatchesTheIngestPath(t *testing.T) {
	collector := newExternalTelemetryCollector(t)
	// Every code normalizeExternalRecords emits today
	// (internal/streamhandlers/external_ingest.go) must be accepted.
	for _, reason := range []string{
		"unsupported_kind_for_system", "entity_family_mismatch",
		"invalid_field", "record_outside_source_instance",
	} {
		if err := collector.ObserveExternalKindRefused("github", reason); err != nil {
			t.Errorf("live refusal reason %q is not counted: %v", reason, err)
		}
	}
	if err := collector.ObserveExternalKindRefused("github", "reason_nothing_emits"); err == nil {
		t.Error("an unknown refusal reason was accepted; it must be an error, not an 'other' bucket")
	}
}

// TestExternalProjectTransitionsSunkIgnoresEmptyBatches keeps the exposition
// from claiming a provider is active on the strength of batches that carried
// no project transitions at all. Every external batch of any kind reaches this
// call site, so counting zeros would mint a series for every source system
// that ever pushed anything.
func TestExternalProjectTransitionsSunkIgnoresEmptyBatches(t *testing.T) {
	collector := newExternalTelemetryCollector(t)
	if err := collector.ObserveExternalProjectMembershipsSunk("github", 0); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(collector.PrometheusText(), `worker_external_project_memberships_sunk_total{provider="github"}`) {
		t.Error("a batch with no project transitions minted a provider series")
	}
	if err := collector.ObserveExternalProjectMembershipsSunk("github", -1); err == nil {
		t.Error("a negative row count was accepted")
	}
}
