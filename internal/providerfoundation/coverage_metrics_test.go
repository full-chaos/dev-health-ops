package providerfoundation

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

// The two CHAOS-4130 counters ride the already-registered dev_health_provider_*
// family, which is what makes them scraped rather than constructed-and-
// discarded. Both carry a dataset label: a page cap or a destroyed unit is
// only actionable when an operator can see WHICH dataset it belongs to.
func TestCoverageCountersRenderPerProviderAndDataset(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordInventoryPageCap("GitHub", "tests")
	metrics.RecordInventoryPageCap("github", "tests")
	metrics.RecordInventoryPageCap("github", "cicd")
	metrics.RecordUnitTerminalWithRows("gitlab", "tests")
	// Unbounded label sources must collapse, never mint a series.
	metrics.RecordUnitTerminalWithRows("mystery-provider", "org-4711/repo-with-a-very-long-identifier")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		`dev_health_provider_inventory_page_cap_total{provider="github",dataset="tests"} 2`,
		`dev_health_provider_inventory_page_cap_total{provider="github",dataset="cicd"} 1`,
		`dev_health_provider_unit_terminal_with_rows_total{provider="gitlab",dataset="tests"} 1`,
		`dev_health_provider_unit_terminal_with_rows_total{provider="other",dataset="other"} 1`,
		"# TYPE dev_health_provider_inventory_page_cap_total counter",
		"# TYPE dev_health_provider_unit_terminal_with_rows_total counter",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	// A nil registry is the no-metrics deployment; recording must stay safe.
	var absent *Metrics
	absent.RecordInventoryPageCap("github", "tests")
	absent.RecordUnitTerminalWithRows("github", "tests")
}

func TestMetricDatasetLabelIsAllowlisted(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{
		"tests": "tests", "pr-reviews": "pr-reviews", "work-item-labels": "work-item-labels",
		"TESTS": "tests", " tests ": "tests",
		"": "other", "acme/api": "other", "tests;drop": "other",
		// Well-formed but UNREGISTERED. A syntactic bound would have minted a
		// series for each of these; the allowlist is what actually caps the
		// count (CHAOS-4130 review round 3).
		"work_items": "other", "testsx": "other", "prs2": "other",
		strings.Repeat("d", 33): "other",
	} {
		if got := MetricDatasetLabel(input); got != want {
			t.Fatalf("MetricDatasetLabel(%q)=%q, want %q", input, got, want)
		}
	}
}

// The label bound has to hold against VOLUME, not just against one bad string:
// the failure mode is a producer minting one series per distinct value.
func TestUnregisteredDatasetsCollapseToASingleSeries(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	for index := 0; index < 5000; index++ {
		metrics.RecordUnitTerminalWithRows("github", "tenant-dataset-"+strconv.Itoa(index))
		metrics.RecordInventoryPageCap("github", "tenant-dataset-"+strconv.Itoa(index))
	}
	metrics.RecordUnitTerminalWithRows("github", "tests")
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	if !strings.Contains(rendered,
		`dev_health_provider_unit_terminal_with_rows_total{provider="github",dataset="other"} 5000`) {
		t.Fatalf("5000 distinct unregistered datasets did not collapse:\n%s", rendered)
	}
	if !strings.Contains(rendered,
		`dev_health_provider_unit_terminal_with_rows_total{provider="github",dataset="tests"} 1`) {
		t.Fatalf("collapsing swallowed the registered dataset too:\n%s", rendered)
	}
	series := strings.Count(rendered, "dev_health_provider_unit_terminal_with_rows_total{") +
		strings.Count(rendered, "dev_health_provider_inventory_page_cap_total{")
	if series != 3 {
		t.Fatalf("expected 3 series (2 terminal + 1 page-cap), got %d:\n%s", series, rendered)
	}
}

// The per-run truncation counter is a SEPARATE series from the inventory page
// cap, not a new label on it: an inventory cap withholds the watermark and
// stalls coverage, while a per-run truncation advances it and is bounded and
// self-limiting. An operator alerting on "coverage is going stale" must not be
// woken by a repository that merely has one enormous workflow run in it
// (CHAOS-4142).
func TestPerRunTruncationCounterRendersPerProviderDatasetAndComponent(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordPerRunTruncation("GitHub", "cicd", "run_jobs")
	metrics.RecordPerRunTruncation("github", "cicd", "run_jobs")
	metrics.RecordPerRunTruncation("github", "cicd", "run_artifacts")
	metrics.RecordPerRunTruncation("gitlab", "tests", "run_jobs")
	// Every unbounded label source must collapse rather than mint a series.
	metrics.RecordPerRunTruncation("mystery", "org-4711/repo", "run_"+strings.Repeat("x", 64))

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		`dev_health_provider_per_run_truncation_total{provider="github",dataset="cicd",component="run_jobs"} 2`,
		`dev_health_provider_per_run_truncation_total{provider="github",dataset="cicd",component="run_artifacts"} 1`,
		`dev_health_provider_per_run_truncation_total{provider="gitlab",dataset="tests",component="run_jobs"} 1`,
		`dev_health_provider_per_run_truncation_total{provider="other",dataset="other",component="other"} 1`,
		"# TYPE dev_health_provider_per_run_truncation_total counter",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	// A nil registry is the no-metrics deployment; recording must stay safe.
	var absent *Metrics
	absent.RecordPerRunTruncation("github", "cicd", "run_jobs")
}

func TestMetricPerRunComponentLabelIsAllowlisted(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{
		"run_jobs": "run_jobs", "run_artifacts": "run_artifacts", "run_reports": "run_reports",
		"RUN_JOBS": "run_jobs", " run_jobs ": "run_jobs",
		// Well-formed but UNREGISTERED collapses, the same way the dataset
		// allowlist behaves.
		"": "other", "run_job": "other", "run_jobsx": "other",
		"report_member": "other", "run_inventory": "other",
	} {
		if got := MetricPerRunComponentLabel(input); got != want {
			t.Fatalf("MetricPerRunComponentLabel(%q)=%q, want %q", input, got, want)
		}
	}
}

// The bound must hold against VOLUME, not just one bad string.
func TestUnregisteredPerRunComponentsCollapseToASingleSeries(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	for index := 0; index < 5000; index++ {
		metrics.RecordPerRunTruncation("github", "cicd", "component-"+strconv.Itoa(index))
	}
	metrics.RecordPerRunTruncation("github", "cicd", "run_jobs")
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	if !strings.Contains(rendered,
		`dev_health_provider_per_run_truncation_total{provider="github",dataset="cicd",component="other"} 5000`) {
		t.Fatalf("5000 unregistered components did not collapse to one series:\n%s", rendered)
	}
	series := strings.Count(rendered, "dev_health_provider_per_run_truncation_total{")
	if series != 2 {
		t.Fatalf("rendered %d per-run series, want 2 (other + run_jobs)", series)
	}
}
