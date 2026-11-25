package throughputforecast

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// overlayTeamScopeBindsValue reports whether a binding carries want, alone or in a list.
func overlayTeamScopeBindsValue(bindings []clickhouse.Binding, want string) bool {
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
	if !overlayTeamScopeBindsValue(client.bindings[0], team) {
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
			if !overlayTeamScopeBindsValue(client.bindings[index], team) {
				t.Errorf("%s: teamIds [%s] is absent from the bindings of its read (statement %d): the team forecast serves the org-wide overlay",
					read.name, team, index)
			}
		}
		if !found {
			t.Errorf("%s: no statement reads %q", read.name, read.table)
		}
	}
}

// overlayTeamScopeStatementsAtMain are the sha256 digests (first 12 hex) of the review and incident overlay statements of an
// org forecast before the team scope reached them: no team keeps those statements byte for byte.
var overlayTeamScopeStatementsAtMain = map[string]string{
	"FROM repo_metrics_daily":     "dbae383e3b50",
	"FROM incident_metrics_daily": "b78743db0b31",
}

type overlayTeamScopeStatement struct {
	statement string
	bindings  []string
}

func overlayTeamScopeReadsFor(t *testing.T, teamIDs []string) map[string]overlayTeamScopeStatement {
	t.Helper()
	client := &fakeClient{responses: fullResponseSet(t, 120, 7)}
	if _, err := Resolve(context.Background(), client, "org-synthetic-1",
		model.ThroughputForecastInput{TeamIds: teamIDs, HistoryWeeks: 12}, mustDay(t, "2026-09-01"),
	); err != nil {
		t.Fatalf("Resolve %v: %v", teamIDs, err)
	}
	out := map[string]overlayTeamScopeStatement{}
	for table := range overlayTeamScopeStatementsAtMain {
		for index, statement := range client.statements {
			if !strings.Contains(statement, table) {
				continue
			}
			var names []string
			for _, binding := range client.bindings[index] {
				names = append(names, fmt.Sprintf("%s=%v", binding.Name, binding.Value))
			}
			out[table] = overlayTeamScopeStatement{statement: statement, bindings: names}
		}
	}
	return out
}

// The team scope only narrows the overlays: no team is the org statement of main, a blank team id is the org scope
// (teamscope.RepoCondition drops it, as for hotspots), and a team set binds every team once.
func TestThroughputForecastOverlayTeamScopeEqualitySweep(t *testing.T) {
	org := overlayTeamScopeReadsFor(t, nil)
	for table, want := range overlayTeamScopeStatementsAtMain {
		read := org[table]
		got := fmt.Sprintf("%x", sha256.Sum256([]byte(read.statement)))[:12]
		if got != want {
			t.Errorf("no team, %s: statement digest %s, want %s (main's org statement)", table, got, want)
		}
		if strings.Contains(read.statement, teamscope.Marker) || len(read.bindings) != 2 {
			t.Errorf("no team, %s: carries a team condition or extra bindings %v", table, read.bindings)
		}
	}
	for _, blank := range [][]string{{""}, {"", ""}} {
		got := overlayTeamScopeReadsFor(t, blank)
		for table := range overlayTeamScopeStatementsAtMain {
			if got[table].statement != org[table].statement || strings.Join(got[table].bindings, ";") != strings.Join(org[table].bindings, ";") {
				t.Errorf("teamIds %q, %s: not the org statement and bindings", blank, table)
			}
		}
	}
	set := overlayTeamScopeReadsFor(t, []string{"team-synthetic-a", "team-synthetic-b", "team-synthetic-a"})
	for table := range overlayTeamScopeStatementsAtMain {
		read := set[table]
		if !strings.Contains(read.statement, teamscope.Marker) {
			t.Errorf("team set, %s: no ownership condition", table)
		}
		want := "team_scope_ids=[team-synthetic-a team-synthetic-b]"
		found := false
		for _, binding := range read.bindings {
			if binding == want {
				found = true
			}
		}
		if !found {
			t.Errorf("team set, %s: bindings %v, want %s", table, read.bindings, want)
		}
	}
}
