package teamattribution

import (
	"fmt"
	"testing"
)

// A linked item copies its donor's own row, never a resolution of the donor
// in the linked item's context (CHAOS-9460): the donor row's team wins over
// what this context would give the donor, a donor with no row gives nothing,
// and only a row the donor holds on its own (native_team, issue_project,
// project_ownership, repo_ownership) gives its team. Every provider.
func TestALinkedItemCopiesItsDonorsRowOnly(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			repoD := "repo-d"
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Repos: []GithubWorkItemDerivationRepoFact{{Provider: provider, TeamID: "team-context", RepoID: &repoD, IsPrimary: 1, Specificity: 100}},
			})
			subjects := map[string]GithubWorkItemDerivationSubject{
				"pr":    {WorkItemID: "pr", Provider: provider, OrgID: "org"},
				"donor": {WorkItemID: "donor", Provider: provider, RepoID: &repoD, OrgID: "org"},
			}
			edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "pr", TargetWorkItemID: "donor", RelationshipType: "relates_to"}}
			for _, tc := range []struct {
				name string
				rows map[string]GithubWorkItemDerivationDonorRow
				want string
			}{
				{"the donor's row wins over its resolution here", map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-row", Source: "repo_ownership"}}, "team-row"},
				{"no donor row gives nothing", map[string]GithubWorkItemDerivationDonorRow{}, ""},
				{"an inherited row gives nothing", map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-row", Source: "linked_issue"}}, ""},
				{"a membership row gives nothing", map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-row", Source: "assignee_membership"}}, ""},
				{"a fallback row gives nothing", map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-row", Source: "manual_fallback"}}, ""},
				{"a native team row gives its team", map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-row", Source: "native_team"}}, "team-row"},
				{"a row with no team gives nothing", map[string]GithubWorkItemDerivationDonorRow{"donor": {Source: "repo_ownership"}}, ""},
			} {
				index, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows(provider, subjects, tc.rows, edges, nil)
				inherited, ok := index["pr"]
				if ok != (tc.want != "") || inherited[0] != tc.want {
					t.Errorf("%s: the pull request inherits %q (present %v), want %q (index %v)", tc.name, inherited[0], ok, tc.want, index)
				}
				if ok {
					want := "linked_issue=donor;dependency_type=relates_to;donor_provider=" + provider + ";donor_source=" + tc.rows["donor"].Source
					if evidence["pr"] != want {
						t.Errorf("%s: evidence %q, want %q", tc.name, evidence["pr"], want)
					}
					derived.LinkedIssue, derived.LinkedIssueEvidence = index, evidence
					for _, candidate := range func() []GithubWorkItemDerivationCandidate { _, _, c := derived.Resolve(subjects["pr"]); return c }() {
						if candidate.Source == "linked_issue" && candidate.Evidence != want {
							t.Errorf("%s: the linked_issue row's evidence %q, want %q", tc.name, candidate.Evidence, want)
						}
					}
					derived.LinkedIssue, derived.LinkedIssueEvidence = nil, nil
				}
			}
			index, _, _ := derived.BuildLinkedIssueIndex(provider, subjects, edges, nil)
			if got := fmt.Sprint(index["pr"][0]); got != "team-context" {
				t.Errorf("the reference shape resolves the donor in this context: %q, want team-context", got)
			}
		})
	}
}

// Two edges from one item that reach the same donor (an issue-key edge and a
// direct edge to the same Jira issue) give one linked_issue row; its evidence
// names the relationship that sorts first, never the one the edges happen to
// be read in.
func TestTwoEdgesToOneDonorNameOneRelationship(t *testing.T) {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
	subjects := map[string]GithubWorkItemDerivationSubject{
		"pr":         {WorkItemID: "pr", Provider: "github", OrgID: "org"},
		"jira:ENG-1": {WorkItemID: "jira:ENG-1", Provider: "jira", OrgID: "org"},
	}
	rows := map[string]GithubWorkItemDerivationDonorRow{"jira:ENG-1": {TeamID: "team-row", Source: "project_ownership"}}
	edges := []GithubWorkItemDerivationDependencyEdge{
		{SourceWorkItemID: "pr", TargetWorkItemID: "jira:ENG-1", RelationshipType: "relates_to"},
		{SourceWorkItemID: "pr", TargetWorkItemID: "extkey:ENG-1", RelationshipType: "external_issue_key"},
	}
	for run := 0; run < 20; run++ {
		_, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows("github", subjects, rows, edges, nil)
		if want := "linked_issue=jira:ENG-1;dependency_type=external_issue_key;donor_provider=jira;donor_source=project_ownership"; evidence["pr"] != want {
			t.Fatalf("run %d: evidence %q, want %q", run, evidence["pr"], want)
		}
	}
}

// A stored donor row does not record its team's provider. The team is the one
// the id names for an item of the donor's provider, so an inactive team of
// another provider with the same id never drops it, and the team of the
// donor's provider, when inactive, does.
func TestAStoredDonorRowBindsTheTeamOfTheDonorsProvider(t *testing.T) {
	for _, tc := range []struct {
		name         string
		jiraInactive bool
		want         string
	}{{"another provider's inactive team with the id", false, "shared-id"}, {"the donor provider's team is inactive", true, ""}} {
		t.Run(tc.name, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: []GithubWorkItemDerivationTeamFact{
					{Provider: "github", TeamID: "shared-id", TeamName: "GitHub Shared", Inactive: true},
					{Provider: "jira", TeamID: "shared-id", TeamName: "Jira Shared", Inactive: tc.jiraInactive},
				},
			})
			subjects := map[string]GithubWorkItemDerivationSubject{
				"ghpr:acme/api#7": {WorkItemID: "ghpr:acme/api#7", Provider: "github", Type: "pr", OrgID: "org"},
				"jira:ENG-1":      {WorkItemID: "jira:ENG-1", Provider: "jira", OrgID: "org"},
			}
			edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "ghpr:acme/api#7", TargetWorkItemID: "jira:ENG-1", RelationshipType: "relates_to"}}
			rows := map[string]GithubWorkItemDerivationDonorRow{"jira:ENG-1": {TeamID: "shared-id", Source: "project_ownership"}}
			derived.LinkedIssue, derived.LinkedIssueEvidence, _, _ = derived.BuildLinkedIssueIndexFromRows("github", subjects, rows, edges, nil)
			teamID, _, candidates := derived.Resolve(subjects["ghpr:acme/api#7"])
			if got := GithubWorkItemDerivationStringValue(teamID); got != tc.want {
				t.Fatalf("team %q, want %q (candidates %+v)", got, tc.want, candidates)
			}
			if tc.want != "" {
				for _, candidate := range candidates {
					if candidate.Source == "linked_issue" && (candidate.TeamProvider != "jira" || GithubWorkItemDerivationStringValue(candidate.TeamName) != "Jira Shared") {
						t.Errorf("linked_issue candidate bound to %q %q, want the jira team", candidate.TeamProvider, GithubWorkItemDerivationStringValue(candidate.TeamName))
					}
				}
			}
		})
	}
}

// A donor whose team is inactive is no donor: it is left out before the
// winner among the linked donors is picked, so the item takes the team of its
// other donor, never unassigned. A pull request of GitHub or GitLab, with
// donors of every provider; the inactive donor sorts first.
func TestAnInactiveDonorNeverHidesAnotherLinkedDonor(t *testing.T) {
	for _, itemProvider := range []string{"github", "gitlab"} {
		for _, donorProvider := range []string{"github", "gitlab", "jira", "linear"} {
			t.Run(itemProvider+"_"+donorProvider, func(t *testing.T) {
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
					Teams: []GithubWorkItemDerivationTeamFact{
						{Provider: donorProvider, TeamID: "team-gone", TeamName: "Gone", Inactive: true},
						{Provider: donorProvider, TeamID: "team-active", TeamName: "Active"},
					},
				})
				subjects := map[string]GithubWorkItemDerivationSubject{
					"pr":      {WorkItemID: "pr", Provider: itemProvider, Type: "pr", OrgID: "org"},
					"a-donor": {WorkItemID: "a-donor", Provider: donorProvider, OrgID: "org"},
					"b-donor": {WorkItemID: "b-donor", Provider: donorProvider, OrgID: "org"},
				}
				edges := []GithubWorkItemDerivationDependencyEdge{
					{SourceWorkItemID: "pr", TargetWorkItemID: "a-donor", RelationshipType: "relates_to"},
					{SourceWorkItemID: "pr", TargetWorkItemID: "b-donor", RelationshipType: "relates_to"},
				}
				rows := map[string]GithubWorkItemDerivationDonorRow{
					"a-donor": {TeamID: "team-gone", Source: "project_ownership"},
					"b-donor": {TeamID: "team-active", Source: "project_ownership"},
				}
				derived.LinkedIssue, derived.LinkedIssueEvidence, _, _ = derived.BuildLinkedIssueIndexFromRows(itemProvider, subjects, rows, edges, nil)
				teamID, _, candidates := derived.Resolve(subjects["pr"])
				if got := GithubWorkItemDerivationStringValue(teamID); got != "team-active" {
					t.Fatalf("team %q, want team-active from the active donor (candidates %+v)", got, candidates)
				}
				if got := derived.LinkedIssueEvidence["pr"]; got != "linked_issue=b-donor;dependency_type=relates_to;donor_provider="+donorProvider+";donor_source=project_ownership" {
					t.Errorf("evidence %q, want the active donor's", got)
				}
			})
		}
	}
}

// A donor's row of this run keeps the team as its own resolution bound it: an
// admin team (no provider) is never bound again to a provider team that has the
// same id, so the linked item names the donor's team, not another one. Every
// donor provider.
func TestADonorsAdminTeamKeepsItsIdentity(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			key := "ENG"
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: []GithubWorkItemDerivationTeamFact{
					{Provider: "", TeamID: "shared", TeamName: "Admin donor team", ProjectKeys: []string{key}},
					{Provider: "github", TeamID: "shared", TeamName: "Unrelated provider team"},
					{Provider: provider, TeamID: "shared", TeamName: "Unrelated provider team"},
				},
			})
			subjects := map[string]GithubWorkItemDerivationSubject{
				"ghpr:acme/api#7": {WorkItemID: "ghpr:acme/api#7", Provider: "github", Type: "pr", OrgID: "org"},
				"donor":           {WorkItemID: "donor", Provider: provider, ProjectKey: &key, OrgID: "org"},
			}
			own := derived.OwnDonorRows(subjects, map[string]struct{}{"donor": {}})
			if own["donor"].TeamName != "Admin donor team" || own["donor"].TeamProvider != "" {
				t.Fatalf("the donor's own row here is %+v, not the admin team this case needs", own["donor"])
			}
			edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "ghpr:acme/api#7", TargetWorkItemID: "donor", RelationshipType: "relates_to"}}
			derived.LinkedIssue, derived.LinkedIssueEvidence, _, _ = derived.BuildLinkedIssueIndexFromRows("github", subjects, own, edges, nil)
			_, _, candidates := derived.Resolve(subjects["ghpr:acme/api#7"])
			found := false
			for _, candidate := range candidates {
				if candidate.Source != "linked_issue" {
					continue
				}
				found = true
				if got := GithubWorkItemDerivationStringValue(candidate.TeamName); got != "Admin donor team" || candidate.TeamProvider != "" {
					t.Errorf("linked_issue team %q of provider %q, want the donor's admin team", got, candidate.TeamProvider)
				}
			}
			if !found {
				t.Fatalf("no linked_issue candidate: %+v", candidates)
			}
		})
	}
}

// A stored donor row records its team's id and name, not its provider. The
// team is the catalog team with that id and that name, so an admin team keeps
// its identity when a provider team has the same id; when that team is
// inactive the donor gives nothing (the provider team of the same id does not
// stand in for it); a name no catalog team has falls back to the binding of
// an ownership fact of the donor. Every donor provider.
func TestAStoredAdminDonorRowKeepsItsIdentity(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		for _, tc := range []struct {
			name          string
			adminInactive bool
			storedName    string
			wantName      string
			wantProvider  string
		}{
			{"the admin team of the row", false, "Admin donor team", "Admin donor team", ""},
			{"the admin team of the row is inactive", true, "Admin donor team", "", ""},
			{"a name no catalog team has", false, "Renamed long ago", "Renamed long ago", provider},
		} {
			t.Run(provider+"/"+tc.name, func(t *testing.T) {
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
					Teams: []GithubWorkItemDerivationTeamFact{
						{Provider: "", TeamID: "shared", TeamName: "Admin donor team", Inactive: tc.adminInactive},
						{Provider: provider, TeamID: "shared", TeamName: "Unrelated provider team"},
					},
				})
				subjects := map[string]GithubWorkItemDerivationSubject{
					"ghpr:acme/api#7": {WorkItemID: "ghpr:acme/api#7", Provider: "github", Type: "pr", OrgID: "org"},
					"donor":           {WorkItemID: "donor", Provider: provider, OrgID: "org"},
				}
				edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "ghpr:acme/api#7", TargetWorkItemID: "donor", RelationshipType: "relates_to"}}
				rows := map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "shared", TeamName: tc.storedName, Source: "issue_project"}}
				derived.LinkedIssue, derived.LinkedIssueEvidence, _, _ = derived.BuildLinkedIssueIndexFromRows("github", subjects, rows, edges, nil)
				_, _, candidates := derived.Resolve(subjects["ghpr:acme/api#7"])
				var linked *GithubWorkItemDerivationCandidate
				for i := range candidates {
					if candidates[i].Source == "linked_issue" {
						linked = &candidates[i]
					}
				}
				if tc.wantName == "" {
					if linked != nil {
						t.Fatalf("linked_issue candidate %+v from an inactive donor team, want none", *linked)
					}
					return
				}
				if linked == nil {
					t.Fatalf("no linked_issue candidate: %+v", candidates)
				}
				if got := GithubWorkItemDerivationStringValue(linked.TeamName); got != tc.wantName || linked.TeamProvider != tc.wantProvider {
					t.Errorf("linked_issue team %q of provider %q, want %q of %q", got, linked.TeamProvider, tc.wantName, tc.wantProvider)
				}
			})
		}
	}
}

// Two catalog teams with the stored row's id and name: an active one comes
// before an inactive one, and of two active ones, the one of the donor's
// provider.
func TestAStoredDonorRowPrefersAnActiveTeamOfTheDonorsProvider(t *testing.T) {
	for _, tc := range []struct {
		name         string
		teams        []GithubWorkItemDerivationTeamFact
		wantProvider string
	}{
		{"an active team before an inactive one", []GithubWorkItemDerivationTeamFact{
			{Provider: "jira", TeamID: "shared", TeamName: "Platform", Inactive: true},
			{Provider: "", TeamID: "shared", TeamName: "Platform"},
		}, ""},
		{"the donor's provider of two active ones", []GithubWorkItemDerivationTeamFact{
			{Provider: "github", TeamID: "shared", TeamName: "Platform"},
			{Provider: "jira", TeamID: "shared", TeamName: "Platform"},
		}, "jira"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: tc.teams})
			subjects := map[string]GithubWorkItemDerivationSubject{
				"ghpr:acme/api#7": {WorkItemID: "ghpr:acme/api#7", Provider: "github", Type: "pr", OrgID: "org"},
				"donor":           {WorkItemID: "donor", Provider: "jira", OrgID: "org"},
			}
			edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "ghpr:acme/api#7", TargetWorkItemID: "donor", RelationshipType: "relates_to"}}
			rows := map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "shared", TeamName: "Platform", Source: "project_ownership"}}
			index, _, _, _ := derived.BuildLinkedIssueIndexFromRows("github", subjects, rows, edges, nil)
			got, ok := index["ghpr:acme/api#7"]
			if !ok || got[0] != "shared" || got[2] != tc.wantProvider {
				t.Fatalf("inherited %v (present %v), want team shared of provider %q", got, ok, tc.wantProvider)
			}
		})
	}
}
