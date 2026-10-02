package home

import "testing"

// CHAOS-7776: the polarity the Go API uses (lowerIsBetter) must equal the one
// the web shows (the `polarity` field of every entry in
// web/src/lib/metrics/catalog.ts, read 2026-10-02 at web main + the redesign
// line). The web side is listed here by metric; a metric Go knows must have the
// same polarity as the web catalog. If either side changes, this test fails
// until the other follows.
var webCatalogLowerIsBetter = map[string]bool{
	"cycle_time":          true,
	"review_latency":      true,
	"throughput":          false,
	"deploy_freq":         false,
	"churn":               true,
	"wip_saturation":      true,
	"blocked_work":        true,
	"change_failure_rate": true,
	"ci_success":          false,
	"pr_rework_ratio":     true,
	"rework_ratio":        true,
	"compounding_risk":    true,
}

func TestLowerIsBetterEqualsTheWebCatalogPolarity(t *testing.T) {
	// Every metric Go categorizes has a web polarity, and they agree.
	for metric := range metricCategories {
		want, ok := webCatalogLowerIsBetter[metric]
		if !ok {
			t.Errorf("metric %q is in the Go categories but not in the web catalog list", metric)
			continue
		}
		if got := LowerIsBetter(metric); got != want {
			t.Errorf("polarity of %q: Go lowerIsBetter = %v, web catalog = %v", metric, got, want)
		}
	}
	// Every Go lower-is-better metric is in the web list (no Go-only entry).
	for metric, lower := range lowerIsBetter {
		if web, ok := webCatalogLowerIsBetter[metric]; !ok || web != lower {
			t.Errorf("Go lowerIsBetter lists %q (%v) but the web catalog list says (%v, present=%v)", metric, lower, web, ok)
		}
	}
	// An unknown metric is higher-is-better, as in the web catalog's default.
	if LowerIsBetter("not_a_metric") {
		t.Error("an unknown metric must not be lower-is-better")
	}
}
