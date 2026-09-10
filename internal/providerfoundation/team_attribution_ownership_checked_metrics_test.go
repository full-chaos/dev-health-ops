package providerfoundation

import (
	"bytes"
	"strings"
	"testing"
)

// CHAOS-4320 (codex round 1, F1/P1: this counter was previously 0.0%
// covered -- nothing exercised RecordTeamAttributionOwnershipChecked or its
// bounded label function at all): pins the exported series name, the
// bounded reason vocabulary (owned/repo_not_owned/ownership_unknown), and
// the "other" collapse for an unrecognized value.
func TestTeamAttributionOwnershipCheckedCounterRendersByReason(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordTeamAttributionOwnershipChecked("owned")
	metrics.RecordTeamAttributionOwnershipChecked("OWNED")
	metrics.RecordTeamAttributionOwnershipChecked("ownership_unknown")
	metrics.RecordTeamAttributionOwnershipChecked("not-a-real-reason")
	var nilMetrics *Metrics
	nilMetrics.RecordTeamAttributionOwnershipChecked("owned")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_team_attribution_ownership_checked_total counter",
		`dev_health_team_attribution_ownership_checked_total{reason="owned"} 2`,
		`dev_health_team_attribution_ownership_checked_total{reason="ownership_unknown"} 1`,
		`dev_health_team_attribution_ownership_checked_total{reason="other"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
}

func TestMetricTeamAttributionOwnershipCheckedLabelIsBounded(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ in, want string }{
		{"owned", "owned"},
		{"Owned", "owned"},
		{" owned ", "owned"},
		{"repo_not_owned", "repo_not_owned"},
		{"ownership_unknown", "ownership_unknown"},
		{"", "other"},
		{"laundered", "other"},
	} {
		if got := MetricTeamAttributionOwnershipCheckedLabel(test.in); got != test.want {
			t.Fatalf("MetricTeamAttributionOwnershipCheckedLabel(%q) = %q, want %q", test.in, got, test.want)
		}
	}
}
