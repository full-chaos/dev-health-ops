package aianalytics

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

func loadDCases(t *testing.T) map[string]oracleDCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/oracle_d_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []oracleDCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]oracleDCase{}
	for _, c := range doc.Cases {
		out[c.Name] = c
	}
	return out
}

// Every table each detector read touches is filtered on the caller's org, in
// every join and subquery, and the org is the one bound.
func TestDetectorStatements_CarryTheOrgPredicateOnEveryTable(t *testing.T) {
	cases := loadDCases(t)
	ctx := context.Background()
	ai := &fixtureClientD{c: cases["ai/everything"]}
	if _, err := AiOpportunities(ctx, ai, "org-1", nil, 25); err != nil {
		t.Fatal(err)
	}
	flow := &fixtureClientD{c: cases["flow/all"]}
	if _, err := FlowOpportunities(ctx, flow, "org-1", nil, 10, 30); err != nil {
		t.Fatal(err)
	}
	statements := append(append([]string(nil), ai.statements...), flow.statements...)
	bindings := append(append(ai.bindings[:0:0], ai.bindings...), flow.bindings...)
	want := map[string]map[string]int{
		"FROM ai_impact_metrics_daily": {"org_id = {org_id:String}": 1},
		"title_prefix": {
			"pr.org_id = {org_id:String}":             1,
			"link.org_id = {org_id:String}":           1,
			"wi.org_id = {org_id:String}":             1,
			"toString(attr.org_id) = {org_id:String}": 2,
		},
		"LEFT ANTI JOIN": {
			"pr.org_id = {org_id:String}":             1,
			"toString(attr.org_id) = {org_id:String}": 1,
		},
		"FROM git_commits": {
			"c.org_id = {org_id:String}": 1,
			"s.org_id = c.org_id":        1,
		},
		"testops_test_metrics_daily": {"org_id = {org_id:String}": 2},
		"AS repo_metrics_daily":      {"org_id = {org_id:String}": 2},
		"work_item_metrics_daily":    {"org_id = {org_id:String}": 1},
	}
	seen := map[string]bool{}
	for _, st := range statements {
		for marker, preds := range want {
			if !strings.Contains(st, marker) {
				continue
			}
			seen[marker] = true
			for pred, n := range preds {
				if got := strings.Count(st, pred); got < n {
					t.Errorf("%q: %d occurrence(s) of %q, want at least %d", marker, got, pred, n)
				}
			}
		}
	}
	for marker := range want {
		if !seen[marker] {
			t.Errorf("no statement matched %q", marker)
		}
	}
	for _, b := range bindings {
		found := false
		for _, x := range b {
			if x.Name == "org_id" {
				found = true
				if x.Value != "org-1" {
					t.Errorf("org_id = %v", x.Value)
				}
			}
		}
		if !found {
			t.Errorf("a statement does not bind org_id: %v", b)
		}
	}
	for _, st := range statements {
		if !strings.HasPrefix(st, "SELECT") || strings.Contains(st, ";") {
			t.Errorf("not a single SELECT:\n%s", st)
		}
	}
}

// Both title-pattern rules carry their own pattern text, and the regular
// expression escapes reach the statement as the reference sends them.
func TestTitlePatternRules_CarryTheirPatterns(t *testing.T) {
	cases := loadDCases(t)
	ai := &fixtureClientD{c: cases["ai/everything"]}
	if _, err := AiOpportunities(context.Background(), ai, "org-1", nil, 25); err != nil {
		t.Fatal(err)
	}
	var dep, mig string
	for _, st := range ai.statements {
		switch {
		case strings.Contains(st, "bump"):
			dep = st
		case strings.Contains(st, "migrat"):
			mig = st
		}
	}
	if !strings.Contains(dep, `chore\\(deps\\)|build\\(deps\\)`) || !strings.Contains(dep, "'(depend|deps|version|package|requirement|lockfile| from .* to )'") {
		t.Errorf("dependency pattern:\n%s", dep)
	}
	if !strings.Contains(mig, "'(migrat|mass rename|codemod|deprecat.* api|bulk (rename|move))'") {
		t.Errorf("migration pattern:\n%s", mig)
	}
	for name, st := range map[string]string{"dep": dep, "mig": mig} {
		if !strings.Contains(st, "NOT LIKE '%bot%'") || !strings.Contains(st, "NOT LIKE '%renovate%'") {
			t.Errorf("%s: bot exclusions missing", name)
		}
	}
}

// Scope and window reach the statements as predicates with bindings, and a
// team scope silences every read that carries no team.
func TestDetectorStatements_ApplyScopeAndWindow(t *testing.T) {
	cases := loadDCases(t)
	repo, team := "11111111-1111-1111-1111-111111111111", "team-a"
	ai := &fixtureClientD{c: cases["ai/everything"]}
	if _, err := AiOpportunities(context.Background(), ai, "org-1", &model.AIScopeInput{RepoID: &repo}, 25); err != nil {
		t.Fatal(err)
	}
	if len(ai.statements) != 6 {
		t.Fatalf("repo scope must run all six reads, ran %d", len(ai.statements))
	}
	for i, st := range ai.statements {
		if !strings.Contains(st, "repo_id = {repo_id:UUID}") {
			t.Errorf("statement %d lacks the repository predicate:\n%s", i, st)
		}
		if v, _ := binding(ai.bindings[i], "repo_id"); v != repo {
			t.Errorf("statement %d binds repo_id=%v", i, v)
		}
	}
	teamRun := &fixtureClientD{c: cases["ai/everything"]}
	if _, err := AiOpportunities(context.Background(), teamRun, "org-1", &model.AIScopeInput{TeamID: &team}, 25); err != nil {
		t.Fatal(err)
	}
	if len(teamRun.statements) != 1 || !strings.Contains(teamRun.statements[0], "team_id = {team_id:String}") {
		t.Fatalf("a team scope reads only the rollups: %v", teamRun.statements)
	}
	flow := &fixtureClientD{c: cases["flow/all"]}
	if _, err := FlowOpportunities(context.Background(), flow, "org-1", &model.AIScopeInput{RepoID: &repo, TeamID: &team}, 10, 7); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(flow.statements[0], "repo_id = {repo_id:UUID}") || strings.Contains(flow.statements[0], "team_id") {
		t.Errorf("repo read:\n%s", flow.statements[0])
	}
	if !strings.Contains(flow.statements[1], "team_id = {team_id:String}") || strings.Contains(flow.statements[1], "repo_id") {
		t.Errorf("team read:\n%s", flow.statements[1])
	}
	for i := range flow.statements {
		if v, _ := binding(flow.bindings[i], "window_days"); v != uint32(7) {
			t.Errorf("window_days = %v", v)
		}
	}
}

func TestOpportunityScoring_Primitives(t *testing.T) {
	for _, c := range []struct{ got, want float64 }{
		{scoreRatio(2.0, 1.0), 1.0}, {scoreRatio(1.0, 1.0), 0.5}, {scoreRatio(0.0, 1.0), 0.0},
		{scoreRatio(1.0, 0), 0.0}, {scoreRatio(1.0, -1.0), 0.0}, {scoreDelta(0.2, 0.1), 1.0},
		{scoreDelta(0.1, 0.1), 0.5}, {scoreDelta(1.0, 0.0), 0.0},
		{clamp01(-0.1), 0.0}, {clamp01(1.5), 1.0}, {clamp01(0.05), 0.05}, {clamp01(0.99), 0.99},
	} {
		if c.got != c.want {
			t.Errorf("got %v, want %v", c.got, c.want)
		}
	}
	if ratioOrNil(1, 0) != nil || ratioOrNil(1, -1) != nil || ratioOrNil(0, 0) != nil {
		t.Error("a non-positive denominator has no ratio")
	}
	if r := ratioOrNil(0, 2); r == nil || *r != 0 {
		t.Error("zero over a positive denominator is zero")
	}
	if got := stableOpportunityID("high_rework", "repo", ""); len(got) != 24 {
		t.Errorf("id %q", got)
	}
	if pct(0.125, 0) != "12%" || pct(0.135, 0) != "14%" || pct(0.0834, 1) != "8.3%" {
		t.Errorf("percent formatting: %s %s %s", pct(0.125, 0), pct(0.135, 0), pct(0.0834, 1))
	}
}

func TestOpportunityLimits_Bounds(t *testing.T) {
	for in, want := range map[int]int{-1: 25, 0: 25, 1: 1, 24: 24, 100: 100, 101: 100, 5000: 100} {
		if got := boundedOpportunityLimit(in); got != want {
			t.Errorf("ai limit %d = %d, want %d", in, got, want)
		}
	}
	for in, want := range map[int]int{-1: 1, 0: 1, 1: 1, 50: 50, 100: 100, 101: 100} {
		if got := boundedFlowLimit(in); got != want {
			t.Errorf("flow limit %d = %d, want %d", in, got, want)
		}
	}
	for in, want := range map[int]int{-5: 1, 0: 1, 1: 1, 30: 30, 365: 365, 366: 365, 9999: 365} {
		if got := boundedWindow(in); got != want {
			t.Errorf("window %d = %d, want %d", in, got, want)
		}
	}
}

// The reads keep the shape the rules rely on: the newest version per key, the
// minimum days of data and no blank team.
func TestDetectorStatements_KeepTheirShape(t *testing.T) {
	for marker, statements := range map[string][]string{
		"LIMIT 1 BY org_id, repo_id, day": {flakyStatement, repoFlowStatement},
		"HAVING data_days >= 5":           {repoFlowStatement, teamFlowStatement},
		"AND team_id != ''":               {teamFlowStatement},
		"doc_changes = 0":                 {docDriftStatement},
		"cases_total >= {min_cases":       {flakyStatement},
	} {
		for _, st := range statements {
			if !strings.Contains(st, marker) {
				t.Errorf("statement lacks %q:\n%s", marker, st)
			}
		}
	}
}

func TestIntRatio_IsCorrectlyRounded(t *testing.T) {
	if r := intRatioOrNil(9007199254740992, 9007199254740993); r == nil || *r != 0.9999999999999999 {
		t.Errorf("got %v", r)
	}
	if intRatioOrNil(1, 0) != nil || intRatioOrNil(1, -2) != nil {
		t.Error("a non-positive denominator has no ratio")
	}
	if r := intRatioOrNil(1, 3); r == nil || *r != 1.0/3.0 {
		t.Errorf("got %v", r)
	}
}
