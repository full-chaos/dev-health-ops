package teamattribution

import "testing"

const tierPR = "ghpr:acme/api#7"

func tierFixture(rows map[string]GithubWorkItemDerivationDonorRow, edges []GithubWorkItemDerivationDependencyEdge, tiers map[LinkPair]string) (map[string][3]string, map[string]string) {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
	derived.LinkTiers = tiers
	subjects := map[string]GithubWorkItemDerivationSubject{
		tierPR:       {WorkItemID: tierPR, Provider: "github", Type: "pr", OrgID: "org"},
		"jira:AAA-1": {WorkItemID: "jira:AAA-1", Provider: "jira", OrgID: "org"},
		"jira:ZZZ-9": {WorkItemID: "jira:ZZZ-9", Provider: "jira", OrgID: "org"},
	}
	index, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows("github", subjects, rows, edges, nil)
	return index, evidence
}

func tierEdges(targets ...string) []GithubWorkItemDerivationDependencyEdge {
	var edges []GithubWorkItemDerivationDependencyEdge
	for _, target := range targets {
		edges = append(edges, GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: tierPR, TargetWorkItemID: target, RelationshipType: "relates_to"})
	}
	return edges
}

func ownRow(team string) GithubWorkItemDerivationDonorRow {
	return GithubWorkItemDerivationDonorRow{TeamID: team, Source: "project_ownership"}
}

// R1: a native link outranks an explicit_text link whatever the target ids.
func TestLinkedIssueDonorNativeBeforeExplicitText(t *testing.T) {
	rows := map[string]GithubWorkItemDerivationDonorRow{"jira:AAA-1": ownRow("team-text"), "jira:ZZZ-9": ownRow("team-native")}
	tiers := map[LinkPair]string{{tierPR, "jira:AAA-1"}: "explicit_text", {tierPR, "jira:ZZZ-9"}: "native"}
	index, _ := tierFixture(rows, tierEdges("extkey:AAA-1", "jira:ZZZ-9"), tiers)
	if got := index[tierPR][0]; got != "team-native" {
		t.Fatalf("team %q, want team-native", got)
	}
}

// R2: a native donor with no usable team gives no row; the text donor is out.
func TestUnusableNativeDonorGivesNoTextDonorRow(t *testing.T) {
	rows := map[string]GithubWorkItemDerivationDonorRow{"jira:AAA-1": ownRow("team-text"), "jira:ZZZ-9": {Source: "manual_fallback"}}
	tiers := map[LinkPair]string{{tierPR, "jira:AAA-1"}: "explicit_text", {tierPR, "jira:ZZZ-9"}: "native"}
	index, _ := tierFixture(rows, tierEdges("extkey:AAA-1", "jira:ZZZ-9"), tiers)
	if got, ok := index[tierPR]; ok {
		t.Fatalf("linked_issue row from %q, want none", got[0])
	}
}

// R3: a recorded explicit_text edge beats an unrecorded edge to a lower id.
func TestRecordedTextEdgeBeatsUnrecordedEdge(t *testing.T) {
	rows := map[string]GithubWorkItemDerivationDonorRow{"jira:AAA-1": ownRow("team-a"), "jira:ZZZ-9": ownRow("team-b")}
	tiers := map[LinkPair]string{{tierPR, "jira:ZZZ-9"}: "explicit_text"}
	index, _ := tierFixture(rows, tierEdges("jira:AAA-1", "jira:ZZZ-9"), tiers)
	if got := index[tierPR][0]; got != "team-b" {
		t.Fatalf("team %q, want team-b", got)
	}
}

// R4: an edge with no link of record is never a donor, even alone.
func TestUnrecordedEdgeIsNeverADonor(t *testing.T) {
	rows := map[string]GithubWorkItemDerivationDonorRow{"jira:AAA-1": ownRow("team-a")}
	for _, target := range []string{"extkey:AAA-1", "jira:AAA-1"} {
		t.Run(target, func(t *testing.T) {
			index, _ := tierFixture(rows, tierEdges(target), map[LinkPair]string{})
			if got, ok := index[tierPR]; ok {
				t.Fatalf("linked_issue row from %q, want none", got[0])
			}
		})
	}
}

// R4b: an extkey edge takes the tier of its resolved pair.
func TestExtkeyEdgeTakesThePairTier(t *testing.T) {
	rows := map[string]GithubWorkItemDerivationDonorRow{"jira:AAA-1": ownRow("team-a"), "jira:ZZZ-9": ownRow("team-b")}
	tiers := map[LinkPair]string{{tierPR, "jira:AAA-1"}: "heuristic", {tierPR, "jira:ZZZ-9"}: "native"}
	index, _ := tierFixture(rows, tierEdges("jira:AAA-1", "extkey:ZZZ-9"), tiers)
	if got := index[tierPR][0]; got != "team-b" {
		t.Fatalf("team %q, want team-b", got)
	}
}

// R5: the linked_issue evidence names the tier of the link of record.
func TestLinkedIssueEvidenceNamesTheTier(t *testing.T) {
	rows := map[string]GithubWorkItemDerivationDonorRow{"jira:AAA-1": ownRow("team-a")}
	tiers := map[LinkPair]string{{tierPR, "jira:AAA-1"}: "explicit_text"}
	_, evidence := tierFixture(rows, tierEdges("jira:AAA-1"), tiers)
	want := "linked_issue=jira:AAA-1;dependency_type=relates_to;donor_provider=jira;donor_source=project_ownership;link_tier=explicit_text"
	if evidence[tierPR] != want {
		t.Fatalf("evidence %q, want %q", evidence[tierPR], want)
	}
}

// A source that is not a pull or merge request has no link of record: its
// edges keep the id order and name no tier.
func TestNonPullRequestSourceKeepsIdOrder(t *testing.T) {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
	subjects := map[string]GithubWorkItemDerivationSubject{
		"jira:ENG-5": {WorkItemID: "jira:ENG-5", Provider: "jira", OrgID: "org"},
		"jira:AAA-1": {WorkItemID: "jira:AAA-1", Provider: "jira", OrgID: "org"},
		"jira:ZZZ-9": {WorkItemID: "jira:ZZZ-9", Provider: "jira", OrgID: "org"},
	}
	rows := map[string]GithubWorkItemDerivationDonorRow{"jira:AAA-1": ownRow("team-a"), "jira:ZZZ-9": ownRow("team-b")}
	edges := []GithubWorkItemDerivationDependencyEdge{
		{SourceWorkItemID: "jira:ENG-5", TargetWorkItemID: "jira:ZZZ-9", RelationshipType: "relates_to"},
		{SourceWorkItemID: "jira:ENG-5", TargetWorkItemID: "jira:AAA-1", RelationshipType: "relates_to"},
	}
	index, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows("jira", subjects, rows, edges, nil)
	if got := index["jira:ENG-5"][0]; got != "team-a" {
		t.Fatalf("team %q, want team-a", got)
	}
	if want := "linked_issue=jira:AAA-1;dependency_type=relates_to;donor_provider=jira;donor_source=project_ownership"; evidence["jira:ENG-5"] != want {
		t.Fatalf("evidence %q, want %q", evidence["jira:ENG-5"], want)
	}
}

func TestOnlyPullAndMergeRequestsHaveALinkOfRecord(t *testing.T) {
	for id, want := range map[string]bool{
		"ghpr:acme/api#7": true, "gitlab:acme/app!7": true, "gitlab:group/sub/app!12": true,
		"gitlab:acme/app#7": false, "gh:acme/api#7": false, "ghpr:acme/api!7": false, "gitlab:acme/app!0": false,
		"ghpr:acme/api#": false, "ghpr:#7": false, "jira:ENG-1": false,
	} {
		if got := IsPullRequestSource(id); got != want {
			t.Errorf("IsPullRequestSource(%q) = %v, want %v", id, got, want)
		}
	}
}

// A GitLab issue (gitlab:path#n) has no link of record: its donors keep id
// order and never wait on a merge request of the same number.
func TestAGitLabIssueKeepsIdOrder(t *testing.T) {
	const issue = "gitlab:acme/app#7"
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
	derived.LinkTiers = map[LinkPair]string{{PullRequest: issue, Issue: "jira:ZZZ-9"}: "native"}
	subjects := map[string]GithubWorkItemDerivationSubject{
		issue:        {WorkItemID: issue, Provider: "gitlab", Type: "issue", OrgID: "org"},
		"jira:AAA-1": {WorkItemID: "jira:AAA-1", Provider: "jira", OrgID: "org"},
		"jira:ZZZ-9": {WorkItemID: "jira:ZZZ-9", Provider: "jira", OrgID: "org"},
	}
	rows := map[string]GithubWorkItemDerivationDonorRow{"jira:AAA-1": ownRow("team-a"), "jira:ZZZ-9": ownRow("team-b")}
	edges := []GithubWorkItemDerivationDependencyEdge{
		{SourceWorkItemID: issue, TargetWorkItemID: "jira:AAA-1", RelationshipType: "relates_to"},
		{SourceWorkItemID: issue, TargetWorkItemID: "jira:ZZZ-9", RelationshipType: "relates_to"},
	}
	index, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows("gitlab", subjects, rows, edges, nil)
	if got := index[issue][0]; got != "team-a" {
		t.Fatalf("team %q, want team-a (id order, no link tier)", got)
	}
	if want := "linked_issue=jira:AAA-1;dependency_type=relates_to;donor_provider=jira;donor_source=project_ownership"; evidence[issue] != want {
		t.Fatalf("evidence %q, want %q", evidence[issue], want)
	}
}
