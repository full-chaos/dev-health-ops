package aianalytics

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

func binding(bs []clickhouse.Binding, name string) (any, bool) {
	for _, b := range bs {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

// TestStatements_CarryTheOrgPredicateOnEveryTable pins that each statement
// filters every table that has an org column on the caller's org -- in every
// join and every subquery, not just the outermost read -- and binds exactly
// that org. The counts are per table occurrence in the statement text.
func TestStatements_CarryTheOrgPredicateOnEveryTable(t *testing.T) {
	datasets, cases := loadOracle(t)
	var teamCase oracleCase
	for _, c := range cases {
		if c.Name == "review_load/teamPatterns" {
			teamCase = c
		}
	}
	client := &fixtureClient{c: teamCase, ds: datasets[teamCase.Dataset]}
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	if _, err := ReviewLoad(context.Background(), client, "org-1", dr, scopeInput(teamCase)); err != nil {
		t.Fatal(err)
	}
	if _, err := ImpactSummary(context.Background(), client, "org-1", dr, nil); err != nil {
		t.Fatal(err)
	}
	slugCase := oracleCase{Dataset: teamCase.Dataset, SlugRows: []string{"11111111-1111-1111-1111-111111111111"}}
	slug := "acme/alpha"
	if _, err := Comparison(context.Background(), &fixtureClient{c: slugCase, ds: datasets[teamCase.Dataset]}, "org-1", dr, &model.AIScopeInput{RepoID: &slug}); err != nil {
		t.Fatal(err)
	}

	want := map[string]map[string]int{
		"FROM ai_impact_metrics_daily": {"org_id = {org_id:String}": 1},
		"FROM teams\nWHERE org_id":     {"org_id = {org_id:String}": 1},
		"FROM repos\nWHERE org_id":     {"org_id = {org_id:String}": 1},
		"AS label_id":                  {"org_id = {org_id:String}": 1},
		"attr_map": {
			"pr.org_id = {org_id:String}":             2,
			"link.org_id = {org_id:String}":           1,
			"toString(attr.org_id) = {org_id:String}": 2,
		},
		"user_metrics_daily": {
			"umd.org_id = {org_id:String}":            1,
			"toString(attr.org_id) = {org_id:String}": 1,
		},
		"{slug:String}": {"org_id = {org_id:String}": 1},
	}
	seen := map[string]bool{}
	for _, st := range client.statements {
		for marker, preds := range want {
			if !strings.Contains(st, marker) {
				continue
			}
			seen[marker] = true
			for pred, n := range preds {
				if got := strings.Count(st, pred); got < n {
					t.Errorf("statement with %q has %d occurrence(s) of %q, want at least %d:\n%s", marker, got, pred, n, st)
				}
			}
		}
	}
	for _, b := range client.bindings {
		if v, ok := binding(b, "org_id"); !ok || v != "org-1" {
			t.Errorf("a statement does not bind org_id=org-1: %v", b)
		}
	}
	for marker := range want {
		if marker != "{slug:String}" && !seen[marker] {
			t.Errorf("no statement matched %q", marker)
		}
	}
}

// The statement of the slug lookup, exercised by its own client.
func TestRepositoryLookup_IsOrgScoped(t *testing.T) {
	datasets, _ := loadOracle(t)
	c := oracleCase{SlugRows: nil}
	client := &fixtureClient{c: c, ds: datasets["mixed"]}
	slug := "acme/alpha"
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	got, err := Comparison(context.Background(), client, "org-9", dr, &model.AIScopeInput{RepoID: &slug})
	if err != nil {
		t.Fatal(err)
	}
	if got.DataAvailable {
		t.Fatalf("an unresolved repository must answer the empty window")
	}
	if len(client.statements) != 1 || !strings.Contains(client.statements[0], "org_id = {org_id:String}") {
		t.Fatalf("statements: %v", client.statements)
	}
	if v, _ := binding(client.bindings[0], "org_id"); v != "org-9" {
		t.Fatalf("binding %v", client.bindings[0])
	}
}

func TestDateRange_EndBeforeStartIsRefused(t *testing.T) {
	client := &fixtureClient{}
	dr := model.AIDateRangeInput{StartDate: day("2026-08-03"), EndDate: day("2026-08-01")}
	for name, run := range map[string]func() error{
		"summary":    func() error { _, err := ImpactSummary(context.Background(), client, "o", dr, nil); return err },
		"comparison": func() error { _, err := Comparison(context.Background(), client, "o", dr, nil); return err },
		"reviewLoad": func() error { _, err := ReviewLoad(context.Background(), client, "o", dr, nil); return err },
	} {
		err := run()
		want := "AI analytics date range end_date must be >= start_date (got start=2026-08-03, end=2026-08-01)"
		if err == nil || err.Error() != want {
			t.Errorf("%s: %v", name, err)
		}
	}
	if len(client.statements) != 0 {
		t.Errorf("no statement may run for a refused range: %v", client.statements)
	}
}

// Each scope branch reaches the statement as a predicate with its binding.
func TestDailyStatement_AppliesEveryScopePredicate(t *testing.T) {
	datasets, _ := loadOracle(t)
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	repo, team, wt := "11111111-1111-1111-1111-111111111111", "team-a", "pull_request"
	for name, c := range map[string]struct {
		scope      *model.AIScopeInput
		predicates []string
		bindings   map[string]any
	}{
		"repo": {&model.AIScopeInput{RepoID: &repo}, []string{"repo_id = {repo_id:UUID}"}, map[string]any{"repo_id": repo}},
		"team": {&model.AIScopeInput{TeamID: &team}, []string{"team_id = {team_id:String}"}, map[string]any{"team_id": team}},
		"type": {&model.AIScopeInput{WorkType: &wt}, []string{"work_type = {work_type:String}"}, map[string]any{"work_type": wt}},
		"none": {nil, nil, nil},
	} {
		client := &fixtureClient{c: oracleCase{}, ds: datasets["mixed"]}
		if _, err := Comparison(context.Background(), client, "org-1", dr, c.scope); err != nil {
			t.Fatal(err)
		}
		st, bs := client.statements[0], client.bindings[0]
		for _, p := range []string{"repo_id = {repo_id:UUID}", "team_id = {team_id:String}", "work_type = {work_type:String}"} {
			want := false
			for _, q := range c.predicates {
				want = want || q == p
			}
			if got := strings.Contains(st, p); got != want {
				t.Errorf("%s: predicate %q present=%v, want %v", name, p, got, want)
			}
		}
		for k, v := range c.bindings {
			if got, _ := binding(bs, k); got != v {
				t.Errorf("%s: binding %s = %v, want %v", name, k, got, v)
			}
		}
	}
}

// The reviewer read narrows by repository and by the stored team id, and the
// label reads never look up a blank team id.
func TestReviewerAndLabelReads_AreNarrowed(t *testing.T) {
	datasets, cases := loadOracle(t)
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	repo, team := "11111111-1111-1111-1111-111111111111", "team-a"
	client := &fixtureClient{c: oracleCase{TeamRows: []map[string]any{{"id": "team-a", "name": "A", "repo_patterns": []any{"acme/alpha"}}}, RepoRows: []map[string]any{{"repo_id": repo, "full_name": "acme/alpha"}}}, ds: datasets["mixed"]}
	if _, err := ReviewLoad(context.Background(), client, "org-1", dr, &model.AIScopeInput{RepoID: &repo, TeamID: &team}); err != nil {
		t.Fatal(err)
	}
	found := false
	for i, st := range client.statements {
		if strings.Contains(st, "user_metrics_daily") {
			found = true
			for _, p := range []string{"umd.repo_id = {repo_id:UUID}", "umd.team_id = {team_id:String}"} {
				if !strings.Contains(st, p) {
					t.Errorf("reviewer read lacks %q", p)
				}
			}
			if v, _ := binding(client.bindings[i], "team_id"); v != team {
				t.Errorf("team binding %v", v)
			}
		}
	}
	if !found {
		t.Fatal("no reviewer read")
	}
	var c oracleCase
	for _, x := range cases {
		if x.Name == "impact_summary/mixed" {
			c = x
		}
	}
	lc := &fixtureClient{c: c, ds: datasets[c.Dataset]}
	if _, err := ImpactSummary(context.Background(), lc, "org-1", dr, nil); err != nil {
		t.Fatal(err)
	}
	for i, st := range lc.statements {
		if !strings.Contains(st, "AS label_id") {
			continue
		}
		ids, _ := binding(lc.bindings[i], "ids")
		for _, id := range ids.([]string) {
			if id == "" {
				t.Errorf("a label read looked up a blank id: %v", ids)
			}
		}
	}
}
