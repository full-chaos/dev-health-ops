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

// Round-1 classes of the review: a stored link to a donor that is no longer an issue is repaired whatever the
// timestamps, the item's own primary source and the current edges; a refused donor never advances the
// staleness signal; the counts are of distinct evaluated donors.
func TestDonorChangeRepairsAStoredLinkToARefusedDonor(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	edges := []teamattribution.GithubWorkItemDerivationDependencyEdge{
		{SourceWorkItemID: "P", TargetWorkItemID: "D", RelationshipType: "relates_to", LastSynced: base},
	}
	recorded := "linked_issue=D;dependency_type=relates_to;donor_provider=github;donor_source=repo_ownership"
	primaryLinked := workItemAttributionNewestPrimary{TeamID: "team-b", Source: "linked_issue", ComputedAt: base, Evidence: recorded}
	ownedWithProvenance := workItemAttributionNewestPrimary{TeamID: "team-o", Source: "project_ownership", ComputedAt: base,
		LinkedTeamID: "team-b", LinkedEvidence: recorded, LinkedComputedAt: base}
	issueKinds := map[string]teamattribution.GithubWorkItemDerivationSubject{"D": {WorkItemID: "D", Provider: "github", Type: "issue"}}
	prKinds := map[string]teamattribution.GithubWorkItemDerivationSubject{"D": {WorkItemID: "D", Provider: "github", Type: "pr"}}
	for _, tc := range []struct {
		name  string
		item  workItemAttributionNewestPrimary
		donor workItemAttributionNewestPrimary
		kinds map[string]teamattribution.GithubWorkItemDerivationSubject
		want  []string
	}{
		{"donor older than the item", primaryLinked, workItemAttributionNewestPrimary{TeamID: "team-b", Source: "repo_ownership", ComputedAt: base.Add(-time.Hour)}, prKinds, []string{"P"}},
		{"donor as old as the item", primaryLinked, workItemAttributionNewestPrimary{TeamID: "team-b", Source: "repo_ownership", ComputedAt: base}, prKinds, []string{"P"}},
		{"donor newer than the item", primaryLinked, workItemAttributionNewestPrimary{TeamID: "team-b", Source: "repo_ownership", ComputedAt: base.Add(time.Hour)}, prKinds, []string{"P"}},
		{"provenance under an own primary", ownedWithProvenance, workItemAttributionNewestPrimary{TeamID: "team-b", Source: "repo_ownership", ComputedAt: base.Add(-time.Hour)}, prKinds, []string{"P"}},
		{"recorded donor has no work item", primaryLinked, workItemAttributionNewestPrimary{TeamID: "team-b", Source: "repo_ownership", ComputedAt: base.Add(-time.Hour)}, map[string]teamattribution.GithubWorkItemDerivationSubject{}, []string{"P"}},
		{"control: an issue donor older than the item", primaryLinked, workItemAttributionNewestPrimary{TeamID: "team-b", Source: "repo_ownership", ComputedAt: base.Add(-time.Hour)}, issueKinds, nil},
		{"control: provenance of an issue donor under an own primary", ownedWithProvenance, workItemAttributionNewestPrimary{TeamID: "team-b", Source: "repo_ownership", ComputedAt: base.Add(-time.Hour)}, issueKinds, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := map[string]workItemAttributionNewestPrimary{"P": tc.item, "D": tc.donor}
			if got := donorChangedDependents(edges, nil, rows, tc.kinds, nil, nil); fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("owed %v, want %v", got, tc.want)
			}
		})
	}
}

// A refused donor alone newer than the item is no trigger, even when another, valid donor is linked whose row
// is older than the item's snapshot.
func TestDonorChangeARefusedDonorDoesNotAdvanceTheStalenessSignal(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	edges := []teamattribution.GithubWorkItemDerivationDependencyEdge{
		{SourceWorkItemID: "P", TargetWorkItemID: "A-pr", RelationshipType: "relates_to", LastSynced: base},
		{SourceWorkItemID: "P", TargetWorkItemID: "B-issue", RelationshipType: "relates_to", LastSynced: base},
	}
	rows := map[string]workItemAttributionNewestPrimary{
		"P":       {Source: "unassigned", ComputedAt: base},
		"A-pr":    {TeamID: "team-a", Source: "repo_ownership", ComputedAt: base.Add(time.Hour)},
		"B-issue": {TeamID: "team-b", Source: "project_ownership", ComputedAt: base.Add(-time.Hour)},
	}
	kinds := map[string]teamattribution.GithubWorkItemDerivationSubject{
		"A-pr":    {WorkItemID: "A-pr", Provider: "github", Type: "pr"},
		"B-issue": {WorkItemID: "B-issue", Provider: "github", Type: "issue"},
	}
	if got := donorChangedDependents(edges, nil, rows, kinds, nil, nil); len(got) != 0 {
		t.Errorf("owed %v: only the refused donor is newer", got)
	}
}

// The counts are of distinct evaluated donors: a donor shared by two items counts once, a key no item resolves
// to counts as unknown, and an item with no edge counts nothing.
func TestDonorChangeRefusalCountsAreOfDistinctEvaluatedDonors(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	edge := func(source, target string) teamattribution.GithubWorkItemDerivationDependencyEdge {
		return teamattribution.GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: source, TargetWorkItemID: target, RelationshipType: "relates_to", LastSynced: base}
	}
	edges := []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P1", "empty"), edge("P2", "empty"), edge("P3", "extkey:NOPE-1")}
	unassigned := workItemAttributionNewestPrimary{Source: "unassigned", ComputedAt: base}
	rows := map[string]workItemAttributionNewestPrimary{
		"P1": unassigned, "P2": unassigned, "P3": unassigned, "owned-pr": {TeamID: "team-o", Source: "repo_ownership", ComputedAt: base},
		"empty": {TeamID: "team-e", Source: "project_ownership", ComputedAt: base},
	}
	kinds := map[string]teamattribution.GithubWorkItemDerivationSubject{
		"empty":    {WorkItemID: "empty", Provider: "jira", Type: ""},
		"owned-pr": {WorkItemID: "owned-pr", Provider: "github", Type: "pr"},
	}
	refused := map[string]int{}
	donorChangedDependents(edges, map[string]string{}, rows, kinds, nil, refused)
	if fmt.Sprint(refused) != "map[empty_type:1 unknown:1]" {
		t.Errorf("refused = %v, want map[empty_type:1 unknown:1]", refused)
	}
}

// The counts are of the current edges: a recorded donor that the item no longer links is repaired but not
// counted, and the current candidates are still counted after a repair is scheduled; a direct id and a key
// that name the same donor count once.
func TestDonorChangeRefusalCountsAreOfCurrentEdgesOnly(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	edge := func(source, target string) teamattribution.GithubWorkItemDerivationDependencyEdge {
		return teamattribution.GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: source, TargetWorkItemID: target, RelationshipType: "relates_to", LastSynced: base}
	}
	recorded := "linked_issue=OLD;dependency_type=relates_to;donor_provider=github;donor_source=repo_ownership"
	itemLinkedToOld := workItemAttributionNewestPrimary{TeamID: "team-b", Source: "linked_issue", ComputedAt: base, Evidence: recorded}
	kind := func(id, provider, itemType string) teamattribution.GithubWorkItemDerivationSubject {
		return teamattribution.GithubWorkItemDerivationSubject{WorkItemID: id, Provider: provider, Type: itemType}
	}
	for _, tc := range []struct {
		name  string
		edges []teamattribution.GithubWorkItemDerivationDependencyEdge
		keys  map[string]string
		kinds map[string]teamattribution.GithubWorkItemDerivationSubject
		want  string
		owed  string
	}{
		{"the old donor is replaced by a valid issue: repaired, not counted",
			[]teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "jira:OK-1")}, nil,
			map[string]teamattribution.GithubWorkItemDerivationSubject{"OLD": kind("OLD", "github", "pr"), "jira:OK-1": kind("jira:OK-1", "jira", "task")},
			"map[]", "[P]"},
		{"repair scheduled and a current empty-type and a missing donor: both counted",
			[]teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "jira:EMPTY-1"), edge("P", "gone")}, nil,
			map[string]teamattribution.GithubWorkItemDerivationSubject{"OLD": kind("OLD", "github", "pr"), "jira:EMPTY-1": kind("jira:EMPTY-1", "jira", "")},
			"map[empty_type:1 unknown:1]", "[P]"},
		{"a direct id and a key naming the same empty-type donor count once",
			[]teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "jira:EMPTY-1"), edge("P", "extkey:EMPTY-1")},
			map[string]string{"EMPTY-1": "jira:EMPTY-1"},
			map[string]teamattribution.GithubWorkItemDerivationSubject{"OLD": kind("OLD", "github", "issue"), "jira:EMPTY-1": kind("jira:EMPTY-1", "jira", "")},
			"map[empty_type:1]", "[]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := map[string]workItemAttributionNewestPrimary{"P": itemLinkedToOld}
			refused := map[string]int{}
			got := donorChangedDependents(tc.edges, tc.keys, rows, tc.kinds, nil, refused)
			if fmt.Sprint(refused) != tc.want {
				t.Errorf("refused = %v, want %s", refused, tc.want)
			}
			if fmt.Sprint(got) != tc.owed {
				t.Errorf("owed = %v, want %s", got, tc.owed)
			}
		})
	}
}
