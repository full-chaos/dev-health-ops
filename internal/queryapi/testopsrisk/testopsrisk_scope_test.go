package testopsrisk

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

func between(t *testing.T, s, from, to string) string {
	t.Helper()
	i := strings.Index(s, from)
	if i < 0 {
		t.Fatalf("no %q in the statement", from)
	}
	rest := s[i:]
	j := strings.Index(rest, to)
	if j < 0 {
		t.Fatalf("no %q after %q", to, from)
	}
	return rest[:j]
}

func bindingMap(b []clickhouse.Binding) map[string]any {
	m := map[string]any{}
	for _, x := range b {
		m[x.Name] = x.Value
	}
	return m
}

// CHAOS-13814: the scope clause is in each of the four reads, in the read itself (not after a GROUP BY that has
// already pooled the repositories), and the org predicate of every read stays.
func TestResolve_ScopeIsInEveryRead(t *testing.T) {
	cl := &fakeClient{}
	in := model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-31"), RepoIds: []string{"acme/web"}, TeamIds: []string{"team-1"}}
	if _, err := Resolve(context.Background(), cl, "org-1", in); err != nil {
		t.Fatal(err)
	}
	if len(cl.statements) != 2 {
		t.Fatalf("%d statements", len(cl.statements))
	}
	daily, quadrant := cl.statements[0], cl.statements[1]
	repoClause := "repo_id IN (\n              SELECT id FROM repos\n              WHERE org_id = {org_id:String}"
	teamClause := "toString(repo_id) IN ("
	for name, read := range map[string]string{
		"release confidence": between(t, daily, "FROM testops_release_confidence", "GROUP BY day, repo_id"),
		"quality drag":       between(t, daily, "FROM testops_quality_drag FINAL", "GROUP BY day"),
		"quadrant":           between(t, quadrant, "FROM testops_release_confidence", "GROUP BY repo_id, day"),
	} {
		if !strings.Contains(read, repoClause) || !strings.Contains(read, teamClause) {
			t.Errorf("%s read lacks the repository or the team clause:\n%s", name, read)
		}
		if !strings.Contains(read, "WHERE org_id = {org_id:String}\n") {
			t.Errorf("%s read lost its org predicate", name)
		}
	}
	stability := between(t, daily, "AS pipeline ON pipeline.day = stability.day", "AS stability_daily")
	if !strings.Contains(stability, repoClause) || !strings.Contains(stability, teamClause) {
		t.Errorf("the stability read lacks the repository or the team clause:\n%s", stability)
	}
	if !strings.Contains(stability, "\n    GROUP BY day") || strings.Index(stability, repoClause) > strings.Index(stability, "GROUP BY day") {
		t.Errorf("the stability clause must filter the repo-days before GROUP BY day:\n%s", stability)
	}
	if !strings.Contains(daily, testops.StabilityLiveRowsSQL) {
		t.Error("the stability read left the shared statement")
	}
	for i, b := range cl.bindings {
		m := bindingMap(b)
		if m["org_id"] != "org-1" || m["start"] != "2026-01-01" || m["end"] != "2026-01-31" {
			t.Errorf("statement %d bindings %#v", i, m)
		}
		if ids, ok := m["repo_ids"].([]string); !ok || len(ids) != 1 || ids[0] != "acme/web" {
			t.Errorf("statement %d repo_ids = %#v", i, m["repo_ids"])
		}
		if ids, ok := m[teamscope.BindingTeamIDs].([]string); !ok || len(ids) != 1 || ids[0] != "team-1" {
			t.Errorf("statement %d team ids = %#v", i, m[teamscope.BindingTeamIDs])
		}
	}
}

// With no scope the request carries the three bindings it always did and the two statements are the base text.
func TestResolve_NoScopeSendsTheBaseStatementsAndBindings(t *testing.T) {
	for name, in := range map[string]model.TestOpsRiskInput{
		"absent":     {StartDate: date("2026-01-01"), EndDate: date("2026-01-31")},
		"empty":      {StartDate: date("2026-01-01"), EndDate: date("2026-01-31"), RepoIds: []string{}, TeamIds: []string{}},
		"blank team": {StartDate: date("2026-01-01"), EndDate: date("2026-01-31"), TeamIds: []string{"", ""}},
	} {
		t.Run(name, func(t *testing.T) {
			cl := &fakeClient{}
			if _, err := Resolve(context.Background(), cl, "org-1", in); err != nil {
				t.Fatal(err)
			}
			if cl.statements[0] != dailyQuery || cl.statements[1] != quadrantQuery {
				t.Error("a request with no scope changed a statement")
			}
			for i, b := range cl.bindings {
				if len(b) != 3 {
					t.Errorf("statement %d carries %d bindings, want 3: %#v", i, len(b), bindingMap(b))
				}
			}
		})
	}
}
