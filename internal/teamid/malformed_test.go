package teamid

import (
	"slices"
	"testing"
)

func TestMalformedNamesNoTeamOfAnyProvider(t *testing.T) {
	for id, want := range map[string]bool{
		"": true, "  ": true, "gh:": true, " linear: ": true, "atlassian:": true, "custom:": true,
		"linear:gh:": true, "jira: atlassian: ": true, "gh:gl:": true,
		"ENG": false, "linear:ENG": false, "gh:org/team": false, "atlassian:x": false, "a:b": false, "linear:gh:x": false,
	} {
		if got := Malformed(id); got != want {
			t.Errorf("Malformed(%q) = %v, want %v", id, got, want)
		}
	}
}

func TestCandidatesAreEveryPrefixOfABareID(t *testing.T) {
	got := Candidates(" ENG ")
	want := []string{"gh:ENG", "gl:ENG", "linear:ENG", "jira:ENG", "pagerduty:ENG", "custom:ENG", "ms-teams:ENG"}
	if !slices.Equal(got, want) {
		t.Errorf("Candidates = %v, want %v", got, want)
	}
	for _, id := range []string{"linear:ENG", "atlassian:x", "gh:", "", "linear:gh:"} {
		if got := Candidates(id); got != nil {
			t.Errorf("Candidates(%q) = %v, want none", id, got)
		}
	}
	for _, form := range PrefixOnlyForms() {
		if !Malformed(form) {
			t.Errorf("prefix-only form %q is not Malformed", form)
		}
	}
}

// A custom team, pushed or written by the admin, is stored with no provider;
// every other integration with its own name.
func TestStoredProviderIsEmptyOnlyForACustomTeam(t *testing.T) {
	for integration, want := range map[string]string{"custom": "", " custom ": "", "linear": "linear", "jira": "jira", "pagerduty": "pagerduty", "github": "github"} {
		if got := StoredProvider(integration); got != want {
			t.Errorf("StoredProvider(%q) = %q, want %q", integration, got, want)
		}
	}
}
