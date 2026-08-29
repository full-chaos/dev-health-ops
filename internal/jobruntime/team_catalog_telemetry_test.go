package jobruntime

import (
	"strings"
	"testing"
)

// TestObserveTeamCatalogDispatchExposesCounter is CHAOS-4431's telemetry
// coverage, following the exact pattern
// TestObserveTeamRepoOwnershipDerivationExposesCounterAndHistogram caught a
// past "recorded but never exported" bug with: assert the counter actually
// appears in PrometheusText(), not just that Observe returns nil.
func TestObserveTeamCatalogDispatchExposesCounter(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamCatalogDispatch("linear", TeamCatalogEntryPointReferenceDiscovery, TeamCatalogOutcomeNative); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamCatalogDispatch("linear", TeamCatalogEntryPointReferenceDiscovery, TeamCatalogOutcomeNative); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamCatalogDispatch("linear", TeamCatalogEntryPointPostSync, TeamCatalogOutcomeSkipped); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamCatalogDispatch("github", TeamCatalogEntryPointReferenceDiscovery, TeamCatalogOutcomeBridge); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamCatalogDispatch("linear", TeamCatalogEntryPointPostSync, TeamCatalogOutcomeNativeFailedNonfatal); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamCatalogDispatch("linear", TeamCatalogEntryPointPostSync, TeamCatalogOutcomeRosterPreservationFailed); err != nil {
		t.Fatal(err)
	}
	// CHAOS-4432 codex review finding: distinct from TeamCatalogOutcomeSkipped
	// ("nothing selected, the collector never ran") -- this is "the
	// collector WAS called and chose to report a clean skip", e.g.
	// GitLab's non-strict walk-failure Python parity.
	if err := collector.ObserveTeamCatalogDispatch("gitlab", TeamCatalogEntryPointPostSync, TeamCatalogOutcomeCollectorSkipped); err != nil {
		t.Fatal(err)
	}
	// An unrecognized provider clamps to "unknown" -- same cardinality
	// discipline as zeroUnitFinalizationProvider.
	if err := collector.ObserveTeamCatalogDispatch("bogus-provider", TeamCatalogEntryPointPostSync, TeamCatalogOutcomeBridge); err != nil {
		t.Fatal(err)
	}

	text := collector.PrometheusText()
	if !strings.Contains(text, "# HELP dev_health_team_catalog_dispatch_total ") {
		t.Fatalf("missing HELP line:\n%s", text)
	}
	for _, want := range []string{
		`dev_health_team_catalog_dispatch_total{provider="linear",entry_point="reference_discovery",outcome="native"} 2`,
		`dev_health_team_catalog_dispatch_total{provider="linear",entry_point="post_sync",outcome="skipped"} 1`,
		`dev_health_team_catalog_dispatch_total{provider="github",entry_point="reference_discovery",outcome="bridge"} 1`,
		`dev_health_team_catalog_dispatch_total{provider="unknown",entry_point="post_sync",outcome="bridge"} 1`,
		`dev_health_team_catalog_dispatch_total{provider="linear",entry_point="post_sync",outcome="native_failed_nonfatal"} 1`,
		`dev_health_team_catalog_dispatch_total{provider="linear",entry_point="post_sync",outcome="roster_preservation_failed"} 1`,
		`dev_health_team_catalog_dispatch_total{provider="gitlab",entry_point="post_sync",outcome="collector_skipped"} 1`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Fatalf("missing exposition line %q:\n%s", want, text)
		}
	}
}

// TestObserveTeamCatalogRowsWrittenExposesCounter mirrors the dispatch test
// for the paired per-table rows-written counter.
func TestObserveTeamCatalogRowsWrittenExposesCounter(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamCatalogRowsWritten("linear", TeamCatalogTableTeams, 3); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamCatalogRowsWritten("linear", TeamCatalogTableTeams, 2); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamCatalogRowsWritten("linear", TeamCatalogTableTeamProjectOwnership, 807); err != nil {
		t.Fatal(err)
	}
	// GitHub's team_repo_ownership MUST get its own table label, never
	// team_project_ownership's -- the whole point of splitting
	// TeamCatalogResult.OwnershipWritten/RepoOwnershipWritten apart.
	if err := collector.ObserveTeamCatalogRowsWritten("github", TeamCatalogTableTeamRepoOwnership, 42); err != nil {
		t.Fatal(err)
	}

	text := collector.PrometheusText()
	if !strings.Contains(text, "# HELP dev_health_team_catalog_rows_written_total ") {
		t.Fatalf("missing HELP line:\n%s", text)
	}
	for _, want := range []string{
		`dev_health_team_catalog_rows_written_total{provider="linear",table="teams"} 5`,
		`dev_health_team_catalog_rows_written_total{provider="linear",table="team_project_ownership"} 807`,
		`dev_health_team_catalog_rows_written_total{provider="github",table="team_repo_ownership"} 42`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Fatalf("missing exposition line %q:\n%s", want, text)
		}
	}
}

// TestObserveTeamCatalogDispatchRejectsUnregisteredValues pins the closed
// entry-point/outcome enum guard, same convention as every other bounded
// outcome in this package.
func TestObserveTeamCatalogDispatchRejectsUnregisteredValues(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTeamCatalogDispatch("linear", TeamCatalogEntryPoint("bogus"), TeamCatalogOutcomeNative); err == nil {
		t.Fatal("expected an unregistered entry point to be rejected")
	}
	if err := collector.ObserveTeamCatalogDispatch("linear", TeamCatalogEntryPointPostSync, TeamCatalogOutcome("bogus")); err == nil {
		t.Fatal("expected an unregistered outcome to be rejected")
	}
	if err := collector.ObserveTeamCatalogRowsWritten("linear", TeamCatalogTable("bogus"), 1); err == nil {
		t.Fatal("expected an unregistered table to be rejected")
	}
	if err := collector.ObserveTeamCatalogRowsWritten("linear", TeamCatalogTableTeams, -1); err == nil {
		t.Fatal("expected a negative row count to be rejected")
	}
}
