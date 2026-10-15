package providersync

import "testing"

// TestTheTextKeyCountersCountWhatTheRunDid: one text-key candidate and one
// text key skipped for a native link, for the run's log line and stats.
func TestTheTextKeyCountersCountWhatTheRunDid(t *testing.T) {
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "jira:WEB-5", Provider: "jira", ProjectID: "web-proj"},
		{WorkItemID: "linear:CHAOS-1", Provider: "linear", ProjectID: "owner-proj"},
		{WorkItemID: "ghpr:full-chaos/deploy#427", Provider: "github", Type: "pr", RepoID: "repo-1"},
		{WorkItemID: "gitlab:full-chaos/deploy!704", Provider: "gitlab", Type: "merge_request", RepoID: "repo-1"},
	}
	edges := []TeamRepoOwnershipDependencyEdge{
		{SourceWorkItemID: "ghpr:full-chaos/deploy#427", TargetWorkItemID: "extkey:WEB-5", RelationshipType: "external_issue_key"},
		{SourceWorkItemID: "gitlab:full-chaos/deploy!704", TargetWorkItemID: "extkey:WEB-5", RelationshipType: "external_issue_key"},
	}
	links := []TeamRepoOwnershipIssuePRLink{{WorkItemID: "linear:CHAOS-1", RepoID: "repo-1", PRNumber: 704, Provenance: "native"}}
	got := deriveTeamRepoOwnership("org-1", ownerAndWebLinks("linear", "jira"), workItems, edges, links, nil)
	if got.TextKeyCandidates != 1 || got.TextKeysSkippedForNative != 1 {
		t.Fatalf("TextKeyCandidates=%d TextKeysSkippedForNative=%d, want 1 and 1", got.TextKeyCandidates, got.TextKeysSkippedForNative)
	}
}
