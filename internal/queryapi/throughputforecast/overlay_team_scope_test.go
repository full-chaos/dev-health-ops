package throughputforecast

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// bindsValue reports whether a binding carries want, alone or in a list.
func bindsValue(bindings []clickhouse.Binding, want string) bool {
	for _, binding := range bindings {
		switch value := binding.Value.(type) {
		case string:
			if value == want {
				return true
			}
		case []string:
			if slices.Contains(value, want) {
				return true
			}
		}
	}
	return false
}

// A team forecast reads its review and incident overlays for the team: the team scope reaches those two reads as it
// reaches the history read. The statements are checked, not a store.
func TestThroughputForecastOverlaysReadTheTeamScope(t *testing.T) {
	const team = "team-synthetic-a"
	client := &fakeClient{responses: fullResponseSet(t, 120, 7)}
	if _, err := Resolve(context.Background(), client, "org-synthetic-1",
		model.ThroughputForecastInput{TeamIds: []string{team}, HistoryWeeks: 12}, mustDay(t, "2026-09-01"),
	); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if client.calls != 7 {
		t.Fatalf("issued %d queries, want all 7 reads", client.calls)
	}
	if !bindsValue(client.bindings[0], team) {
		t.Fatalf("control: the history read binds no team %q; the cell cannot tell a dropped scope from a renamed one", team)
	}
	t.Logf("control: the history read binds team %q", team)
	for _, read := range []struct{ name, table string }{
		{"reviewBottleneck", "FROM repo_metrics_daily"},
		{"incidentLoad", "FROM incident_metrics_daily"},
	} {
		found := false
		for index, statement := range client.statements {
			if !strings.Contains(statement, read.table) {
				continue
			}
			found = true
			if !bindsValue(client.bindings[index], team) {
				t.Errorf("%s: teamIds [%s] is absent from the bindings of its read (statement %d): the team forecast serves the org-wide overlay",
					read.name, team, index)
			}
		}
		if !found {
			t.Errorf("%s: no statement reads %q", read.name, read.table)
		}
	}
}
