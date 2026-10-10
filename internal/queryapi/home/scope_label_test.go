package home

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func TestPrimaryScopeLabelIsANameNeverTheID(t *testing.T) {
	named := map[string]string{"t1": "Ops", "t2": "Dev"}
	cases := []struct {
		name  string
		scope ScopeFilter
		want  string
	}{
		{"org", ScopeFilter{Level: "org"}, "org"},
		{"one named team", ScopeFilter{Level: "team", IDs: []string{"t1"}, names: named}, "Ops"},
		{"two named teams", ScopeFilter{Level: "team", IDs: []string{"t1", "t2"}, names: named}, "Ops, Dev"},
		{"a named and an unnamed team: the unnamed one is counted, not hidden", ScopeFilter{Level: "team", IDs: []string{"t1", "t9"}, names: named}, "Ops and 1 other team"},
		{"mixed repositories", ScopeFilter{Level: "repo", IDs: []string{"r1", "r8", "r9"}, names: map[string]string{"r1": "alpha"}}, "alpha and 2 other repositories"},
		{"unnamed service", ScopeFilter{Level: "service", IDs: []string{"s9"}}, "the selected service"},
		{"unnamed developers", ScopeFilter{Level: "developer", IDs: []string{"d8", "d9"}}, "the selected developers"},
		{"a duplicate id is one scope", ScopeFilter{Level: "team", IDs: []string{"t1", "t1"}, names: named}, "Ops"},
		{"a duplicate unnamed id is one scope", ScopeFilter{Level: "team", IDs: []string{"t9", "t9"}}, "the selected team"},
		{"unnamed team", ScopeFilter{Level: "team", IDs: []string{"t9"}}, "the selected team"},
		{"unnamed teams", ScopeFilter{Level: "team", IDs: []string{"t8", "t9"}}, "the selected teams"},
		{"unnamed repository", ScopeFilter{Level: "repo", IDs: []string{"r9"}}, "the selected repository"},
		{"unnamed repositories", ScopeFilter{Level: "repo", IDs: []string{"r8", "r9"}}, "the selected repositories"},
	}
	for _, tc := range cases {
		if got := primaryScopeLabel(Filters{Scope: tc.scope}); got != tc.want {
			t.Errorf("%s: label = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestRecommendationSignalNamesItsTeamOrSaysATeam(t *testing.T) {
	row := RecommendationRow{TeamID: "github:acme/ops", RuleID: "wip-saturation", LatestTitle: "WIP"}
	if s, _ := RecommendationSignal(row, Filters{}, DataConfidence{}); s.AffectedScope != recommendationUnnamedTeam {
		t.Errorf("no team name: affected_scope = %q, want a team", s.AffectedScope)
	}
	row.TeamName = "Ops"
	s, _ := RecommendationSignal(row, Filters{}, DataConfidence{})
	if s.AffectedScope != "Ops" {
		t.Errorf("affected_scope = %q, want Ops", s.AffectedScope)
	}
	if s.ID != "recommendation:wip-saturation:github:acme/ops" {
		t.Errorf("the structured id must stay the raw id, got %q", s.ID)
	}
}

// Two ids that carry one name are one name in the prose (vet p3f).
func TestPrimaryScopeLabelNamesOneNameOnceForTwoIDs(t *testing.T) {
	scope := ScopeFilter{Level: "team", IDs: []string{"t1", "t2"}, names: map[string]string{"t1": "Alpha", "t2": "Alpha"}}
	if got := primaryScopeLabel(Filters{Scope: scope}); got != "Alpha" {
		t.Errorf("label = %q, want Alpha once", got)
	}
}

// The risk label lookup keys each name by its id, for repositories and teams (vet p12).
func TestResolveScopeLabelsKeysEachNameByItsID(t *testing.T) {
	score := 0.5
	rows := []RiskRow{{Scope: "repo", ScopeID: "11111111-1111-4111-8111-111111111111", Score: &score}, {Scope: "team", ScopeID: "t1", Score: &score}}
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM repos") {
			return &fixtureRowScanner{rows: [][]any{{"11111111-1111-4111-8111-111111111111", "acme/checkout"}}}, nil
		}
		return &fixtureRowScanner{rows: [][]any{{"t1", "Ops"}}}, nil
	}}
	got := resolveScopeLabels(context.Background(), client, "org-1", rows)
	if got["11111111-1111-4111-8111-111111111111"] != "acme/checkout" || got["t1"] != "Ops" || len(got) != 2 {
		t.Errorf("labels = %v, want each name under its own id", got)
	}
}
