package providersync

import "testing"

// The linear_team_key arm maps a Linear work item's native_team_key to the id
// of the known Linear team with that native key: "linear:<key>" for a team
// the catalog wrote since CHAOS-8939. A bare-id row of the same key (written
// before the carry) never wins over the prefixed one, in either order.
func TestLinearTeamKeyArmResolvesToThePrefixedTeamID(t *testing.T) {
	workItems := []TeamRepoOwnershipWorkItem{{
		WorkItemID: "linear:CHAOS-1", Provider: "linear", RepoID: "repo-a", Type: "pr",
		ProjectID: "11111111-1111-4111-8111-111111111111", NativeTeamKey: "CHAOS",
	}}
	prefixed := TeamRepoOwnershipKnownTeam{Provider: "linear", ID: "linear:CHAOS", NativeTeamKey: "CHAOS"}
	bare := TeamRepoOwnershipKnownTeam{Provider: "linear", ID: "CHAOS"}
	for name, known := range map[string][]TeamRepoOwnershipKnownTeam{
		"prefixed only":          {prefixed},
		"bare row first":         {bare, prefixed},
		"prefixed row first":     {prefixed, bare},
		"another provider first": {{Provider: "jira", ID: "jira:CHAOS", NativeTeamKey: "CHAOS"}, prefixed},
		// A prefixed row with no native_team_key is keyed by its id without
		// the prefix.
		"prefixed, no native key": {{Provider: "linear", ID: "linear:CHAOS"}},
	} {
		t.Run(name, func(t *testing.T) {
			if !hasResolvableLinearNativeTeamKey(workItems, known) {
				t.Fatal("the readiness guard sees no Linear-native signal")
			}
			got := deriveTeamRepoOwnership("org-1", nil, workItems, nil, nil, known).Rows
			if len(got) != 1 || got[0].TeamID != "linear:CHAOS" || got[0].RepoID != "repo-a" ||
				got[0].ResolutionArm != TeamRepoOwnershipResolutionArmLinearTeamKey {
				t.Fatalf("got %+v, want repo-a -> linear:CHAOS through the linear_team_key arm", got)
			}
		})
	}
	if got := deriveTeamRepoOwnership("org-1", nil, workItems, nil, nil,
		[]TeamRepoOwnershipKnownTeam{{Provider: "linear", ID: "linear:OTHER", NativeTeamKey: "OTHER"}}).Rows; len(got) != 0 {
		t.Fatalf("an unknown native key resolved: %+v", got)
	}
}

// A team row with provider linear whose id holds another provider's key is
// not a Linear team: it takes no native key, so no Linear work item resolves
// to it, by the id without a prefix nor by its stored native key.
func TestLinearTeamKeyArmIgnoresAForeignPrefixedTeamID(t *testing.T) {
	for _, foreign := range []string{"gh:ENG", "gl:ENG", "jira:ENG", "custom:ENG"} {
		for _, itemKey := range []string{foreign, "ENG"} {
			workItems := []TeamRepoOwnershipWorkItem{{
				WorkItemID: "linear:ENG-1", Provider: "linear", RepoID: "repo-a", Type: "pr",
				ProjectID: "11111111-1111-4111-8111-111111111111", NativeTeamKey: itemKey,
			}}
			for name, known := range map[string][]TeamRepoOwnershipKnownTeam{
				"no native key": {{Provider: "linear", ID: foreign}},
				"native key":    {{Provider: "linear", ID: foreign, NativeTeamKey: itemKey}},
			} {
				if got := deriveTeamRepoOwnership("org-1", nil, workItems, nil, nil, known).Rows; len(got) != 0 {
					t.Errorf("%s, item key %q, %s: derived %+v, want no row", foreign, itemKey, name, got)
				}
			}
		}
		workItems := []TeamRepoOwnershipWorkItem{{
			WorkItemID: "linear:ENG-1", Provider: "linear", RepoID: "repo-a", Type: "pr",
			ProjectID: "11111111-1111-4111-8111-111111111111", NativeTeamKey: "ENG",
		}}
		// A row of another provider is not a Linear team, whatever its id.
		for _, other := range []TeamRepoOwnershipKnownTeam{
			{Provider: "jira", ID: "ENG", NativeTeamKey: "ENG"},
			{Provider: "github", ID: "linear:ENG", NativeTeamKey: "ENG"},
			{Provider: "gitlab", ID: "linear:ENG"},
		} {
			if got := deriveTeamRepoOwnership("org-1", nil, workItems, nil, nil, []TeamRepoOwnershipKnownTeam{other}).Rows; len(got) != 0 {
				t.Errorf("%s row %s: derived %+v, want no row", other.Provider, other.ID, got)
			}
		}
		known := []TeamRepoOwnershipKnownTeam{
			{Provider: "linear", ID: foreign, NativeTeamKey: "ENG"},
			{Provider: "linear", ID: "linear:ENG", NativeTeamKey: "ENG"},
		}
		got := deriveTeamRepoOwnership("org-1", nil, workItems, nil, nil, known).Rows
		if len(got) != 1 || got[0].TeamID != "linear:ENG" {
			t.Errorf("%s beside linear:ENG: derived %+v, want repo-a -> linear:ENG", foreign, got)
		}
	}
}
