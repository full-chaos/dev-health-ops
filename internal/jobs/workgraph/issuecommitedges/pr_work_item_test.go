package issuecommitedges_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuecommitedges"
)

func deriveGitHub(t *testing.T, message string, items ...issuecommitedges.WorkItemRow) issuecommitedges.Result {
	t.Helper()
	repo := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	for i := range items {
		items[i].RepoID = repo
		items[i].Provider = "github"
	}
	return issuecommitedges.Derive(issuecommitedges.Inputs{
		Commits:   []issuecommitedges.CommitRow{{RepoID: repo, Hash: "abc123", Message: message}},
		WorkItems: items,
	}, time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC))
}

func TestPullRequestWorkItemIsNeverCommitIssueTarget(t *testing.T) {
	got := deriveGitHub(t, "Fixes #7", issuecommitedges.WorkItemRow{WorkItemID: "ghpr:o/r#7"})
	for _, edge := range got.Edges {
		t.Errorf("unexpected commit->issue edge to %q", edge.TargetID)
	}
}

func TestRealIssueStillLinksBesidePullRequestWorkItem(t *testing.T) {
	got := deriveGitHub(t, "Fixes #7",
		issuecommitedges.WorkItemRow{WorkItemID: "gh:o/r#7"},
		issuecommitedges.WorkItemRow{WorkItemID: "ghpr:o/r#7"},
	)
	if len(got.Edges) != 1 || got.Edges[0].TargetID != "gh:o/r#7" {
		t.Fatalf("want one edge to gh:o/r#7, got %+v", got.Edges)
	}
}

func TestRealIssueLinks(t *testing.T) {
	got := deriveGitHub(t, "Fixes #9", issuecommitedges.WorkItemRow{WorkItemID: "gh:o/r#9"})
	if len(got.Edges) != 1 || got.Edges[0].TargetID != "gh:o/r#9" {
		t.Fatalf("want one edge to gh:o/r#9, got %+v", got.Edges)
	}
}
