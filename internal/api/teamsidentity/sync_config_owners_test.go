package teamsidentity

import "testing"

func TestDedupeDiscoveredTeamsKeepsFirstOccurrence(t *testing.T) {
	teams := []discoveredTeam{
		{ProviderTeamID: "a", Name: "first-a"},
		{ProviderTeamID: "b", Name: "first-b"},
		{ProviderTeamID: "a", Name: "second-a"},
	}
	got := dedupeDiscoveredTeams(teams)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(got), got)
	}
	if got[0].Name != "first-a" || got[1].Name != "first-b" {
		t.Fatalf("dedupe did not keep first occurrence in order: %+v", got)
	}
}

func TestDedupeDiscoveredTeamsEmptyInput(t *testing.T) {
	got := dedupeDiscoveredTeams(nil)
	if len(got) != 0 {
		t.Fatalf("len = %d, want 0", len(got))
	}
}
