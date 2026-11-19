package teamattribution

import (
	"testing"
	"time"
)

// A pull request has a native relates_to link to issue #11 and its body says
// "Blocks #11": one sync writes both edges with the same last_synced. On that
// tie the inheritable edge stays, so the pull request keeps #11 as its donor.
// A relationship that is newer than an inheritable one still replaces it.
func TestATieOnLastSyncedKeepsTheInheritableEdge(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	native := GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: "ghpr:acme/api#7", TargetWorkItemID: "gh:acme/api#11", RelationshipType: "relates_to", LastSynced: at}
	text := GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: "ghpr:acme/api#7", TargetWorkItemID: "gh:acme/api#11", RelationshipType: "blocks", LastSynced: at}
	key := struct{ source, target string }{"ghpr:acme/api#7", "gh:acme/api#11"}
	for name, edges := range map[string][]GithubWorkItemDerivationDependencyEdge{
		"native first": {native, text},
		"text first":   {text, native},
	} {
		if got := LatestGitHubWorkItemDerivationDependencies(edges)[key].RelationshipType; got != "relates_to" {
			t.Errorf("%s: the tie keeps %q, want relates_to", name, got)
		}
	}
	newer := text
	newer.LastSynced = at.Add(time.Hour)
	if got := LatestGitHubWorkItemDerivationDependencies([]GithubWorkItemDerivationDependencyEdge{native, newer})[key].RelationshipType; got != "blocks" {
		t.Errorf("a newer blocks edge keeps %q, want blocks", got)
	}
}

func TestALinkedPullRequestKeepsItsNativeDonorOnATie(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
	derived.LinkTiers = map[LinkPair]string{{PullRequest: "ghpr:acme/api#7", Issue: "gh:acme/api#11"}: "native", {PullRequest: "ghpr:acme/api#7", Issue: "gh:acme/api#22"}: "native"}
	subjects := map[string]GithubWorkItemDerivationSubject{
		"ghpr:acme/api#7": {WorkItemID: "ghpr:acme/api#7", Provider: "github", Type: "pr", OrgID: "org"},
		"gh:acme/api#11":  {WorkItemID: "gh:acme/api#11", Provider: "github", Type: "issue", OrgID: "org"},
		"gh:acme/api#22":  {WorkItemID: "gh:acme/api#22", Provider: "github", Type: "issue", OrgID: "org"},
	}
	rows := map[string]GithubWorkItemDerivationDonorRow{
		"gh:acme/api#11": {TeamID: "team-11", Source: "project_ownership"},
		"gh:acme/api#22": {TeamID: "team-22", Source: "project_ownership"},
	}
	edge := func(target, relationship string) GithubWorkItemDerivationDependencyEdge {
		return GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: "ghpr:acme/api#7", TargetWorkItemID: target, RelationshipType: relationship, LastSynced: at}
	}
	for _, tc := range []struct {
		name  string
		edges []GithubWorkItemDerivationDependencyEdge
	}{
		{"#11 is the only donor", []GithubWorkItemDerivationDependencyEdge{edge("gh:acme/api#11", "relates_to"), edge("gh:acme/api#11", "blocks")}},
		{"#22 is a second donor", []GithubWorkItemDerivationDependencyEdge{edge("gh:acme/api#11", "relates_to"), edge("gh:acme/api#11", "blocks"), edge("gh:acme/api#22", "relates_to")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows("github", subjects, rows, tc.edges, nil)
			if got := index["ghpr:acme/api#7"][0]; got != "team-11" {
				t.Errorf("the pull request inherits %q, want team-11 (evidence %q)", got, evidence["ghpr:acme/api#7"])
			}
			if want := "linked_issue=gh:acme/api#11;dependency_type=relates_to;donor_provider=github;donor_source=project_ownership;link_tier=native"; evidence["ghpr:acme/api#7"] != want {
				t.Errorf("evidence %q, want %q", evidence["ghpr:acme/api#7"], want)
			}
		})
	}
}
