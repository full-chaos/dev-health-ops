package teamattribution

import "testing"

// CHAOS-14013: the donor of a linked_issue row must be an issue. A pull request (or merge request) linked
// to another pull request is not an edge of the Repository <> Pull request <> Issue <> Project tree, so it
// gives the item no team. A PR to issue edge is kept. Provider x entity: github (pr), gitlab
// (merge_request); jira and linear have no pull request type, so their donors are always issues.
func TestLinkedIssueDonorMustBeAnIssue(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		provider, prType, issue string
		donorType               string
		wantInherits            bool
	}{
		{"github PR to PR", "github", "pr", "", "pr", false},
		{"github PR to issue", "github", "pr", "", "issue", true},
		{"github PR to bug", "github", "pr", "", "bug", true},
		{"gitlab MR to MR", "gitlab", "merge_request", "", "merge_request", false},
		{"gitlab MR to issue", "gitlab", "merge_request", "", "issue", true},
		{"github PR to empty type", "github", "pr", "", "", false},
		{"gitlab MR to empty type", "gitlab", "merge_request", "", "", false},
		{"github PR to padded pr", "github", "pr", "", " pr ", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
			subjects := map[string]GithubWorkItemDerivationSubject{
				"item":  {WorkItemID: "item", Provider: tc.provider, Type: tc.prType, OrgID: "org"},
				"donor": {WorkItemID: "donor", Provider: tc.provider, Type: tc.donorType, OrgID: "org"},
			}
			rows := map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-donor", Source: "repo_ownership"}}
			edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "item", TargetWorkItemID: "donor", RelationshipType: "relates_to"}}
			index, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows(tc.provider, subjects, rows, edges, nil)
			team, inherits := index["item"]
			if inherits != tc.wantInherits {
				t.Fatalf("item inherits = %v (team %q, evidence %q), want %v", inherits, team[0], evidence["item"], tc.wantInherits)
			}
			derived.LinkedIssue, derived.LinkedIssueEvidence = index, evidence
			_, _, candidates := derived.Resolve(subjects["item"])
			got := false
			for _, candidate := range candidates {
				if candidate.Source == "linked_issue" {
					got = true
				}
			}
			if got != tc.wantInherits {
				t.Fatalf("Resolve has a linked_issue candidate = %v, want %v (candidates %+v)", got, tc.wantInherits, candidates)
			}
		})
	}
}

// Jira and linear donors are issues by their own types; an empty Type fails closed for them too: an empty Type
// is a writer gap and is never read as an issue (CHAOS-14013). The item is a PR of github or gitlab.
func TestLinkedIssueDonorJiraAndLinearTypes(t *testing.T) {
	for _, tc := range []struct {
		donorProvider, donorType string
		want                     bool
	}{
		{"jira", "task", true}, {"jira", "bug", true}, {"jira", "", false},
		{"linear", "task", true}, {"linear", "issue", true}, {"linear", "", false},
	} {
		for _, itemProvider := range []string{"github", "gitlab"} {
			t.Run(itemProvider+" item, "+tc.donorProvider+" donor type "+tc.donorType, func(t *testing.T) {
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
				itemType := map[string]string{"github": "pr", "gitlab": "merge_request"}[itemProvider]
				subjects := map[string]GithubWorkItemDerivationSubject{
					"item":  {WorkItemID: "item", Provider: itemProvider, Type: itemType, OrgID: "org"},
					"donor": {WorkItemID: "donor", Provider: tc.donorProvider, Type: tc.donorType, OrgID: "org"},
				}
				rows := map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-donor", Source: "project_ownership"}}
				edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "item", TargetWorkItemID: "donor", RelationshipType: "relates_to"}}
				index, _, _, _ := derived.BuildLinkedIssueIndexFromRows(itemProvider, subjects, rows, edges, nil)
				if _, inherits := index["item"]; inherits != tc.want {
					t.Fatalf("item inherits = %v, want %v", inherits, tc.want)
				}
			})
		}
	}
}

// A refused donor never shadows an issue donor: the PR donor has the lower target id and is ignored, the
// issue donor gives its team; with only the PR donor, nothing is inherited and a donor that is not among
// the subjects is refused too.
func TestLinkedIssueRefusedDonorDoesNotShadowAnIssueDonor(t *testing.T) {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
	subjects := map[string]GithubWorkItemDerivationSubject{
		"item":    {WorkItemID: "item", Provider: "github", Type: "pr", OrgID: "org"},
		"a-pr":    {WorkItemID: "a-pr", Provider: "github", Type: "pr", OrgID: "org"},
		"b-issue": {WorkItemID: "b-issue", Provider: "github", Type: "issue", OrgID: "org"},
	}
	rows := map[string]GithubWorkItemDerivationDonorRow{
		"a-pr":    {TeamID: "team-pr", Source: "repo_ownership"},
		"b-issue": {TeamID: "team-issue", Source: "repo_ownership"},
		"c-ghost": {TeamID: "team-ghost", Source: "repo_ownership"},
	}
	edges := []GithubWorkItemDerivationDependencyEdge{
		{SourceWorkItemID: "item", TargetWorkItemID: "a-pr", RelationshipType: "relates_to"},
		{SourceWorkItemID: "item", TargetWorkItemID: "b-issue", RelationshipType: "relates_to"},
		{SourceWorkItemID: "item", TargetWorkItemID: "c-ghost", RelationshipType: "relates_to"},
	}
	index, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows("github", subjects, rows, edges, nil)
	if got := index["item"][0]; got != "team-issue" {
		t.Fatalf("item team = %q (evidence %q), want team-issue", got, evidence["item"])
	}
	refused := derived.LinkedIssueDonorRefusalCounts(subjects, edges)
	if refused[LinkedIssueDonorRefusedPullKind] != 1 || refused[LinkedIssueDonorRefusedUnknown] != 1 || len(refused) != 2 {
		t.Errorf("refusals = %v, want one pr_kind (a-pr) and one unknown (c-ghost, an edge target that is not a known work item)", refused)
	}
}

// The refusal counts come from the edges the index evaluates: an item with no edge counts nothing (a repo-owned
// pull request is not a refusal), a donor shared by two items counts once, a key no item resolves to counts as
// unknown, an item with a native team of its own is not evaluated, and a valid issue donor counts nothing.
func TestLinkedIssueDonorRefusalCountsFollowTheEvaluatedEdges(t *testing.T) {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
	subjects := map[string]GithubWorkItemDerivationSubject{
		"owned-pr":     {WorkItemID: "owned-pr", Provider: "github", Type: "pr", OrgID: "org"},
		"item-1":       {WorkItemID: "item-1", Provider: "github", Type: "pr", OrgID: "org"},
		"item-2":       {WorkItemID: "item-2", Provider: "github", Type: "pr", OrgID: "org"},
		"empty":        {WorkItemID: "empty", Provider: "jira", Type: "", OrgID: "org"},
		"jira:OK-1":    {WorkItemID: "jira:OK-1", Provider: "jira", Type: "task", OrgID: "org"},
		"jira:EMPTY-1": {WorkItemID: "jira:EMPTY-1", Provider: "jira", Type: "", OrgID: "org"},
		"native-pr":    {WorkItemID: "native-pr", Provider: "github", Type: "pr", OrgID: "org"},
		"native-pr2":   {WorkItemID: "native-pr2", Provider: "github", Type: "pr", OrgID: "org"},
	}
	edge := func(source, target string) GithubWorkItemDerivationDependencyEdge {
		return GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: source, TargetWorkItemID: target, RelationshipType: "relates_to"}
	}
	for _, tc := range []struct {
		name  string
		edges []GithubWorkItemDerivationDependencyEdge
		want  map[string]int
	}{
		{"no edge", nil, nil},
		{"a shared empty-type donor counts once", []GithubWorkItemDerivationDependencyEdge{edge("item-1", "empty"), edge("item-2", "empty")}, map[string]int{"empty_type": 1}},
		{"a pull request donor", []GithubWorkItemDerivationDependencyEdge{edge("item-1", "item-2")}, map[string]int{"pr_kind": 1}},
		{"a target that is not a known work item", []GithubWorkItemDerivationDependencyEdge{edge("item-1", "gone")}, map[string]int{"unknown": 1}},
		{"a key no item resolves to", []GithubWorkItemDerivationDependencyEdge{edge("item-1", "extkey:NOPE-1")}, map[string]int{"unknown": 1}},
		{"a key that resolves to a valid issue", []GithubWorkItemDerivationDependencyEdge{edge("item-1", "extkey:OK-1")}, nil},
		{"a valid issue donor", []GithubWorkItemDerivationDependencyEdge{edge("item-1", "jira:OK-1")}, nil},
		{"a direct id and a key that name the same empty-type donor count once", []GithubWorkItemDerivationDependencyEdge{edge("item-1", "jira:EMPTY-1"), edge("item-1", "extkey:EMPTY-1")}, map[string]int{"empty_type": 1}},
		{"the same, split across two items", []GithubWorkItemDerivationDependencyEdge{edge("item-1", "jira:EMPTY-1"), edge("item-2", "extkey:empty-1")}, map[string]int{"empty_type": 1}},
		{"two keys no item resolves to count apart", []GithubWorkItemDerivationDependencyEdge{edge("item-1", "extkey:NOPE-1"), edge("item-2", "extkey:NOPE-2")}, map[string]int{"unknown": 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := derived.LinkedIssueDonorRefusalCounts(subjects, tc.edges)
			if len(got) != len(tc.want) {
				t.Fatalf("counts = %v, want %v", got, tc.want)
			}
			for reason, n := range tc.want {
				if got[reason] != n {
					t.Fatalf("counts = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestLinkedIssueDonorRefusalReasons(t *testing.T) {
	for _, tc := range []struct {
		name    string
		subject GithubWorkItemDerivationSubject
		known   bool
		want    string
	}{
		{"github pr", GithubWorkItemDerivationSubject{Provider: "github", Type: "pr"}, true, LinkedIssueDonorRefusedPullKind},
		{"gitlab mr", GithubWorkItemDerivationSubject{Provider: "gitlab", Type: "merge_request"}, true, LinkedIssueDonorRefusedPullKind},
		{"empty type", GithubWorkItemDerivationSubject{Provider: "jira", Type: "  "}, true, LinkedIssueDonorRefusedEmptyType},
		{"not known", GithubWorkItemDerivationSubject{}, false, LinkedIssueDonorRefusedUnknown},
		{"github issue", GithubWorkItemDerivationSubject{Provider: "github", Type: "issue"}, true, ""},
		{"a pr type of another provider is not a pull request", GithubWorkItemDerivationSubject{Provider: "jira", Type: "pr"}, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := LinkedIssueDonorRefusal(tc.subject, tc.known); got != tc.want {
				t.Errorf("reason = %q, want %q", got, tc.want)
			}
		})
	}
}

// An item with a native team of its own evaluates no linked donor: the cascade counts nothing for it, as the
// donor-change backstop does for an item whose own primary is native_team (see
// TestDonorChangeANativeTeamItemCountsNoRefusal in the remaining package).
func TestLinkedIssueDonorRefusalCountsAnItemWithANativeTeamCountsNone(t *testing.T) {
	native := "ENG"
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Teams: []GithubWorkItemDerivationTeamFact{{Provider: "github", TeamID: "team-eng", NativeTeamKey: native}},
	})
	subjects := map[string]GithubWorkItemDerivationSubject{
		"native-item": {WorkItemID: "native-item", Provider: "github", Type: "pr", NativeTeamKey: &native, OrgID: "org"},
		"pr-donor":    {WorkItemID: "pr-donor", Provider: "github", Type: "pr", OrgID: "org"},
		"plain-item":  {WorkItemID: "plain-item", Provider: "github", Type: "pr", OrgID: "org"},
	}
	edge := func(source, target string) GithubWorkItemDerivationDependencyEdge {
		return GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: source, TargetWorkItemID: target, RelationshipType: "relates_to"}
	}
	if got := derived.LinkedIssueDonorRefusalCounts(subjects, []GithubWorkItemDerivationDependencyEdge{edge("native-item", "pr-donor")}); len(got) != 0 {
		t.Errorf("a native-team item counts %v, want none", got)
	}
	if got := derived.LinkedIssueDonorRefusalCounts(subjects, []GithubWorkItemDerivationDependencyEdge{edge("plain-item", "pr-donor")}); got["pr_kind"] != 1 {
		t.Errorf("control: an item with no native team counts %v, want pr_kind 1", got)
	}
}
