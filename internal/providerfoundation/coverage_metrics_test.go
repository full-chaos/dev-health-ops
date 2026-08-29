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
	metrics.RecordPerRunTruncation("GitHub", "cicd", "run_jobs", "per_run_cap")
	metrics.RecordPerRunTruncation("github", "cicd", "run_jobs", "per_run_cap")
	metrics.RecordPerRunTruncation("github", "cicd", "run_artifacts", "per_run_cap")
	metrics.RecordPerRunTruncation("gitlab", "tests", "run_jobs", "per_run_cap")
	// Every unbounded label source must collapse rather than mint a series.
	metrics.RecordPerRunTruncation("mystery", "org-4711/repo", "run_"+strings.Repeat("x", 64), "nonsense")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		`dev_health_provider_per_run_truncation_total{provider="github",dataset="cicd",component="run_jobs",cause="per_run_cap"} 2`,
		`dev_health_provider_per_run_truncation_total{provider="github",dataset="cicd",component="run_artifacts",cause="per_run_cap"} 1`,
		`dev_health_provider_per_run_truncation_total{provider="gitlab",dataset="tests",component="run_jobs",cause="per_run_cap"} 1`,
		`dev_health_provider_per_run_truncation_total{provider="other",dataset="other",component="other",cause="other"} 1`,
		"# TYPE dev_health_provider_per_run_truncation_total counter",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	// A nil registry is the no-metrics deployment; recording must stay safe.
	var absent *Metrics
	absent.RecordPerRunTruncation("github", "cicd", "run_jobs", "per_run_cap")
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
		metrics.RecordPerRunTruncation("github", "cicd", "component-"+strconv.Itoa(index), "per_run_cap")
	}
	metrics.RecordPerRunTruncation("github", "cicd", "run_jobs", "per_run_cap")
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	if !strings.Contains(rendered,
		`dev_health_provider_per_run_truncation_total{provider="github",dataset="cicd",component="other",cause="per_run_cap"} 5000`) {
		t.Fatalf("5000 unregistered components did not collapse to one series:\n%s", rendered)
	}
	series := strings.Count(rendered, "dev_health_provider_per_run_truncation_total{")
	if series != 2 {
		t.Fatalf("rendered %d per-run series, want 2 (other + run_jobs)", series)
	}
}

// The artifact skip reason is a closed vocabulary for the same reason the
// per-run component and cause are: a route must not be able to open an
// unbounded label dimension. An unknown reason collapses to "other".
func TestArtifactSkipReasonLabelIsBounded(t *testing.T) {
	if got := MetricArtifactSkipReasonLabel("  UNREADABLE_ARCHIVE "); got != "unreadable_archive" {
		t.Fatalf("known reason label = %q, want unreadable_archive", got)
	}
	// artifact_unavailable (CHAOS-4191): an artifact whose bytes could never
	// be fetched at all, distinct from unreadable_archive (bytes obtained,
	// container would not open). Added to the vocabulary alongside it and
	// must not collapse to "other".
	if got := MetricArtifactSkipReasonLabel("  ARTIFACT_UNAVAILABLE "); got != "artifact_unavailable" {
		t.Fatalf("known reason label = %q, want artifact_unavailable", got)
	}
	// artifact_oversized (CHAOS-4315): an artifact download that exceeded the
	// route's size cap, now skipped-and-counted like the other two reasons
	// instead of failing the unit closed. Must not collapse to "other".
	if got := MetricArtifactSkipReasonLabel("  ARTIFACT_OVERSIZED "); got != "artifact_oversized" {
		t.Fatalf("known reason label = %q, want artifact_oversized", got)
	}
	if got := MetricArtifactSkipReasonLabel("repo-full-chaos/dev-health-ops"); got != "other" {
		t.Fatalf("unknown reason label = %q, want other", got)
	}
}

// TestDuplicateTestCaseCounterAggregatesByProviderDatasetOnly (CHAOS-4392)
// pins that repo is NOT a label -- codex's CHAOS-4394 finding on the
// sibling RecordCicdPartialSuccess counter applies identically here: a
// synced repository is not drawn from a small, fixed vocabulary, so it
// would grow the series unboundedly over a long-lived worker's lifetime.
// Two calls with different repos but the same provider/dataset must
// aggregate into ONE series, not two.
func TestDuplicateTestCaseCounterAggregatesByProviderDatasetOnly(t *testing.T) {
	metrics := NewMetrics()
	metrics.RecordDuplicateTestCase("github", "cicd", 3)
	metrics.RecordDuplicateTestCase("github", "cicd", 2)
	metrics.RecordDuplicateTestCase("mystery-provider", "mystery-dataset", 1)
	metrics.RecordDuplicateTestCase("gitlab", "tests", 0)

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		`dev_health_cicd_duplicate_test_case_total{provider="github",dataset="cicd"} 5`,
		`dev_health_cicd_duplicate_test_case_total{provider="other",dataset="other"} 1`,
		"# TYPE dev_health_cicd_duplicate_test_case_total counter",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, `provider="gitlab",dataset="tests"`) {
		t.Fatalf("a zero-count call must not mint a series: %s", rendered)
	}
}

// The suite twin of TestDuplicateTestCaseCounterAggregatesByProviderDatasetOnly
// (CHAOS-4508): repo must not become a label dimension, and a zero-count call
// must not mint an empty series.
func TestDuplicateTestSuiteCounterAggregatesByProviderDatasetOnly(t *testing.T) {
	metrics := NewMetrics()
	metrics.RecordDuplicateTestSuite("github", "cicd", 1)
	metrics.RecordDuplicateTestSuite("github", "cicd", 1)
	metrics.RecordDuplicateTestSuite("mystery-provider", "mystery-dataset", 1)
	metrics.RecordDuplicateTestSuite("gitlab", "tests", 0)

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		`dev_health_cicd_duplicate_test_suite_total{provider="github",dataset="cicd"} 2`,
		`dev_health_cicd_duplicate_test_suite_total{provider="other",dataset="other"} 1`,
		"# TYPE dev_health_cicd_duplicate_test_suite_total counter",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, `dev_health_cicd_duplicate_test_suite_total{provider="gitlab",dataset="tests"}`) {
		t.Fatalf("a zero-count call must not mint a series: %s", rendered)
	}
}
