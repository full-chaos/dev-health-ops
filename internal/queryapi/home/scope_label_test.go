package home

import "testing"

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
