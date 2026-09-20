package aianalytics

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// TestGroupBStatements_CarryTheOrgPredicateOnEveryTable pins that each
// statement filters every table with an org column on the caller's org, in
// every join and subquery, and binds exactly that org.
func TestGroupBStatements_CarryTheOrgPredicateOnEveryTable(t *testing.T) {
	raw, err := os.ReadFile("testdata/oracle_b_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []oracleBCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	byName := map[string]oracleBCase{}
	for _, c := range doc.Cases {
		byName[c.Name] = c
	}
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	team := "team-b"
	ctx := context.Background()
	newClient := func(c oracleBCase) *fixtureClientB {
		return &fixtureClientB{
			fixtureClient: &fixtureClient{c: oracleCase{TeamRows: c.TeamRows, RepoRows: c.RepoRows}, ds: dataset{Daily: c.Daily}},
			c:             c,
		}
	}
	client := newClient(byName["risk/teamPatterns"])
	if _, err := RiskBreakdown(ctx, client, "org-1", dr, &model.AIScopeInput{TeamID: &team}); err != nil {
		t.Fatal(err)
	}
	prsClient := newClient(byName["prs/teamFilter"])
	if _, err := AttributedPrs(ctx, prsClient, "org-1", dr, &model.AIScopeInput{TeamID: &team}, 10, 0); err != nil {
		t.Fatal(err)
	}
	ovClient := newClient(byName["overview/teamFilter"])
	if _, err := AttributionOverview(ctx, ovClient, "org-1", dr, &model.AIAttributionScopeInput{TeamID: &team}, 10, 0); err != nil {
		t.Fatal(err)
	}
	client.fixtureClient.statements = append(client.fixtureClient.statements, prsClient.fixtureClient.statements...)
	client.fixtureClient.statements = append(client.fixtureClient.statements, ovClient.fixtureClient.statements...)
	client.fixtureClient.bindings = append(client.fixtureClient.bindings, prsClient.fixtureClient.bindings...)
	client.fixtureClient.bindings = append(client.fixtureClient.bindings, ovClient.fixtureClient.bindings...)

	want := map[string]map[string]int{
		"hotspots AS (": {
			"pr.org_id = {org_id:String}":             2,
			"link.org_id = {org_id:String}":           1,
			"toString(attr.org_id) = {org_id:String}": 2,
			"pc.org_id = {org_id:String}":             1,
			"cs.org_id = {org_id:String}":             1,
			"hs.org_id = {org_id:String}":             1,
		},
		"complex_files AS (": {
			"pr.org_id = {org_id:String}":             2,
			"link.org_id = {org_id:String}":           1,
			"toString(attr.org_id) = {org_id:String}": 2,
			"pc.org_id = {org_id:String}":             1,
			"cs.org_id = {org_id:String}":             1,
			"fc.org_id = {org_id:String}":             1,
		},
		"repo_id_str, number, kind, work_type": {
			"pr.org_id = {org_id:String}":             2,
			"link.org_id = {org_id:String}":           1,
			"wi.org_id = {org_id:String}":             1,
			"toString(attr.org_id) = {org_id:String}": 2,
		},
		"count() AS count": {"toString(org_id) = {org_id:String}": 1},
		"subject_type,\n":  {"toString(org_id) = {org_id:String}": 1},
		"AS full_name":     {"org_id = {org_id:String}": 1},
	}
	seen := map[string]bool{}
	for _, st := range client.fixtureClient.statements {
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
	for marker := range want {
		if !seen[marker] {
			t.Errorf("no statement matched %q", marker)
		}
	}
	for _, b := range client.fixtureClient.bindings {
		if v, ok := binding(b, "org_id"); !ok || v != "org-1" {
			t.Errorf("a statement does not bind org_id=org-1: %v", b)
		}
	}
}

// Every statement of the second group is a single SELECT the read-only client
// accepts: its first token is SELECT, it holds no semicolon.
func TestGroupBStatements_AreReadOnlySelects(t *testing.T) {
	for name, st := range map[string]string{
		"hotspot":    "SELECT * FROM (\nWITH pr_files AS (\n" + "x" + "\n),\n" + hotspotOverlapTail + "\n)",
		"complexity": "SELECT * FROM (\nWITH pr_files AS (\n" + "x" + "\n),\n" + complexityOverlapTail + "\n)",
		"attributed": attributedPRsStatement,
	} {
		if !strings.HasPrefix(st, "SELECT") || strings.Contains(st, ";") {
			t.Errorf("%s is not a single SELECT", name)
		}
	}
}

// The repository and kind scopes of the attribution reads reach the statement
// as predicates with their bindings.
func TestAttributionReads_ApplyRepoAndKindPredicates(t *testing.T) {
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	repo := "11111111-1111-1111-1111-111111111111"
	kinds := []model.AIAttributionBucketInput{model.AIAttributionBucketInputAiReview}
	client := &fixtureClientB{fixtureClient: &fixtureClient{c: oracleCase{}}}
	if _, err := AttributionOverview(context.Background(), client, "org-1", dr, &model.AIAttributionScopeInput{RepoID: &repo, Buckets: kinds}, 10, 0); err != nil {
		t.Fatal(err)
	}
	reads := 0
	for i, st := range client.fixtureClient.statements {
		if !strings.Contains(st, "FROM ai_attribution_resolved") {
			continue
		}
		reads++
		for _, p := range []string{"repo_id = {repo_id:UUID}", "kind IN {kinds:Array(String)}"} {
			if !strings.Contains(st, p) {
				t.Errorf("attribution read lacks %q:\n%s", p, st)
			}
		}
		if v, _ := binding(client.fixtureClient.bindings[i], "kinds"); len(v.([]string)) != 1 || v.([]string)[0] != "ai_review" {
			t.Errorf("kinds binding %v", v)
		}
	}
	if reads != 2 {
		t.Fatalf("attribution reads = %d, want 2", reads)
	}
	none := &fixtureClientB{fixtureClient: &fixtureClient{c: oracleCase{}}}
	if _, err := AttributionOverview(context.Background(), none, "org-1", dr, nil, 10, 0); err != nil {
		t.Fatal(err)
	}
	for _, st := range none.fixtureClient.statements {
		if strings.Contains(st, "{kinds:") || strings.Contains(st, "{repo_id:") {
			t.Errorf("an unscoped read carries a scope predicate:\n%s", st)
		}
	}
}
