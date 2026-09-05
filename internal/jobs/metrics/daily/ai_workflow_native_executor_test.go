package daily

import (
	"sort"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiworkflow"
)

// TestAIPositiveWithoutIssueLinkageNamesThePRs pins codex round chaos-5220
// r1's P3 fix: the helper must return the actual PR identities (artifact_id,
// "repoID:number" form) of every unlinked AI-positive PR, not merely a
// count -- an aggregate count alone names nothing an operator can act on.
func TestAIPositiveWithoutIssueLinkageNamesThePRs(t *testing.T) {
	org := uuid.New()
	artifactEdges := []aiworkflow.ArtifactEdge{
		{RunID: "run-linked", ArtifactID: "repo:1", OrgID: org},
		{RunID: "run-unlinked-a", ArtifactID: "repo:2", OrgID: org},
		{RunID: "run-unlinked-b", ArtifactID: "repo:3", OrgID: org},
	}
	issueEdges := []aiworkflow.IssueEdge{
		{RunID: "run-linked", IssueID: "jira:ABC-1", OrgID: org},
	}

	got := aiPositiveWithoutIssueLinkage(artifactEdges, issueEdges)
	sort.Strings(got)
	want := []string{"repo:2", "repo:3"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got %v, want %v", got, want)
			break
		}
	}
}

func TestAIPositiveWithoutIssueLinkageEmptyWhenAllLinked(t *testing.T) {
	org := uuid.New()
	artifactEdges := []aiworkflow.ArtifactEdge{{RunID: "run-1", ArtifactID: "repo:1", OrgID: org}}
	issueEdges := []aiworkflow.IssueEdge{{RunID: "run-1", IssueID: "jira:ABC-1", OrgID: org}}

	if got := aiPositiveWithoutIssueLinkage(artifactEdges, issueEdges); len(got) != 0 {
		t.Errorf("expected no unlinked PRs, got %v", got)
	}
}
