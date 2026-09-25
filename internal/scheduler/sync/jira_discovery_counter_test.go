package sync

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// jiraDiscoveryCounts reads jira_project_discovery_total: outcome -> count.
func jiraDiscoveryCounts(t *testing.T) map[string]int64 {
	t.Helper()
	var resource metricdata.ResourceMetrics
	if err := meterReader.Collect(context.Background(), &resource); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "jira_project_discovery_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				outcome, _ := point.Attributes.Value("outcome")
				out[outcome.AsString()] += point.Value
			}
		}
	}
	return out
}

// TestRecordJiraProjectDiscoveryMirrorsThePythonEmission pins
// _record_jira_project_discovery and the three steps that count beside it:
// discovered_zero for an empty listing, else discovered and created (created
// even at zero, a series that exists without moving) and existing only when
// positive, skipped_no_planner_parent for a config without a planner parent,
// and superseded, capped and recovered only when positive.
func TestRecordJiraProjectDiscoveryMirrorsThePythonEmission(t *testing.T) {
	for _, c := range []struct {
		name                                               string
		discovered, created, superseded, capped, recovered int
		plannerManaged                                     bool
		want                                               map[string]int64
	}{
		{"empty listing under a planner parent", 0, 0, 0, 0, 0, true, map[string]int64{"discovered_zero": 1}},
		{"empty listing without a planner parent", 0, 0, 0, 0, 0, false, map[string]int64{"discovered_zero": 1, "skipped_no_planner_parent": 1}},
		{"all new", 3, 3, 0, 0, 0, true, map[string]int64{"discovered": 3, "created": 3}},
		{"all existing", 4, 0, 0, 0, 0, true, map[string]int64{"discovered": 4, "existing": 4}},
		{"mixed", 5, 2, 0, 0, 0, true, map[string]int64{"discovered": 5, "created": 2, "existing": 3}},
		{"no planner parent", 2, 2, 0, 0, 0, false, map[string]int64{"discovered": 2, "created": 2, "skipped_no_planner_parent": 1}},
		{"superseded capped recovered", 1, 1, 2, 3, 4, true, map[string]int64{"discovered": 1, "created": 1, "superseded_by_scope_change": 2, "capped_by_repo_limit": 3, "recovered_from_repo_limit_cap": 4}},
	} {
		before := jiraDiscoveryCounts(t)
		recordJiraProjectDiscovery(context.Background(), c.discovered, c.created, c.superseded, c.capped, c.recovered, c.plannerManaged)
		after := jiraDiscoveryCounts(t)
		moved := map[string]int64{}
		for outcome, value := range after {
			if delta := value - before[outcome]; delta != 0 {
				moved[outcome] = delta
			}
		}
		if len(moved) != len(c.want) {
			t.Errorf("%s: moved %v, want %v", c.name, moved, c.want)
			continue
		}
		for outcome, want := range c.want {
			if moved[outcome] != want {
				t.Errorf("%s: moved %v, want %v", c.name, moved, c.want)
			}
		}
	}
}
