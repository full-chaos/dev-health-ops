package remaining

import (
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// CHAOS-14013 in the backstop: the donor a linked item would copy must be an issue, by the same predicate as
// the cascade (teamattribution.LinkedIssueDonorRefusal). A refused donor owes the item nothing, is counted
// by reason, and an item whose stored row was copied from a donor that is not an issue is owed a rederive.
func TestDonorChangeRefusesADonorThatIsNotAnIssue(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	before, after := base, base.Add(time.Hour)
	edges := []teamattribution.GithubWorkItemDerivationDependencyEdge{
		{SourceWorkItemID: "P", TargetWorkItemID: "D", RelationshipType: "relates_to", LastSynced: base},
	}
	itemUnassigned := workItemAttributionNewestPrimary{Source: "unassigned", ComputedAt: before}
	itemLinked := workItemAttributionNewestPrimary{TeamID: "team-a", Source: "linked_issue", ComputedAt: before,
		Evidence: "linked_issue=D;dependency_type=relates_to;donor_provider=github;donor_source=repo_ownership"}
	donorRow := workItemAttributionNewestPrimary{TeamID: "team-b", Source: "repo_ownership", ComputedAt: after}
	kind := func(provider, itemType string) map[string]teamattribution.GithubWorkItemDerivationSubject {
		return map[string]teamattribution.GithubWorkItemDerivationSubject{
			"P": {WorkItemID: "P", Provider: provider, Type: "pr"},
			"D": {WorkItemID: "D", Provider: provider, Type: itemType},
		}
	}
	for _, tc := range []struct {
		name    string
		item    workItemAttributionNewestPrimary
		kinds   map[string]teamattribution.GithubWorkItemDerivationSubject
		want    []string
		refused map[string]int
	}{
		{"github issue donor is a donor", itemUnassigned, kind("github", "issue"), []string{"P"}, map[string]int{}},
		{"github bug donor is a donor", itemUnassigned, kind("github", "bug"), []string{"P"}, map[string]int{}},
		{"jira task donor is a donor", itemUnassigned, kind("jira", "task"), []string{"P"}, map[string]int{}},
		{"linear task donor is a donor", itemUnassigned, kind("linear", "task"), []string{"P"}, map[string]int{}},
		{"github pr donor is refused", itemUnassigned, kind("github", "pr"), nil, map[string]int{"pr_kind": 1}},
		{"gitlab merge request donor is refused", itemUnassigned, kind("gitlab", "merge_request"), nil, map[string]int{"pr_kind": 1}},
		{"empty type donor is refused (github)", itemUnassigned, kind("github", ""), nil, map[string]int{"empty_type": 1}},
		{"empty type donor is refused (jira)", itemUnassigned, kind("jira", ""), nil, map[string]int{"empty_type": 1}},
		{"a donor with no work_items row is refused", itemUnassigned, map[string]teamattribution.GithubWorkItemDerivationSubject{}, nil, map[string]int{"unknown": 1}},
		{"a stored row copied from a pr donor is owed a rederive", itemLinked, kind("github", "pr"), []string{"P"}, map[string]int{"pr_kind": 1}},
		{"a stored row copied from an issue donor of the same team is not owed", func() workItemAttributionNewestPrimary {
			row := itemLinked
			row.TeamID = "team-b"
			return row
		}(), kind("github", "issue"), nil, map[string]int{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refused := map[string]int{}
			rows := map[string]workItemAttributionNewestPrimary{"P": tc.item, "D": donorRow}
			got := donorChangedDependents(edges, nil, rows, tc.kinds, nil, refused)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("owed %v, want %v", got, tc.want)
			}
			if fmt.Sprint(refused) != fmt.Sprint(tc.refused) {
				t.Errorf("refused %v, want %v", refused, tc.refused)
			}
		})
	}
}

// A refused donor with the lower id never hides the issue donor that follows it.
func TestDonorChangeAPullRequestDonorDoesNotShadowAnIssueDonor(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	edges := []teamattribution.GithubWorkItemDerivationDependencyEdge{
		{SourceWorkItemID: "P", TargetWorkItemID: "A", RelationshipType: "relates_to", LastSynced: base},
		{SourceWorkItemID: "P", TargetWorkItemID: "B", RelationshipType: "relates_to", LastSynced: base},
	}
	rows := map[string]workItemAttributionNewestPrimary{
		"P": {TeamID: "team-b", Source: "linked_issue", ComputedAt: base,
			Evidence: "linked_issue=B;dependency_type=relates_to;donor_provider=github;donor_source=repo_ownership"},
		"A": {TeamID: "team-a", Source: "repo_ownership", ComputedAt: base.Add(time.Hour)},
		"B": {TeamID: "team-b", Source: "repo_ownership", ComputedAt: base},
	}
	kinds := map[string]teamattribution.GithubWorkItemDerivationSubject{
		"A": {WorkItemID: "A", Provider: "github", Type: "pr"},
		"B": {WorkItemID: "B", Provider: "github", Type: "issue"},
	}
	if got := donorChangedDependents(edges, nil, rows, kinds, nil, nil); len(got) != 0 {
		t.Errorf("owed %v: the newer pull request donor A changed nothing the item copies (B stays the winner)", got)
	}
}
