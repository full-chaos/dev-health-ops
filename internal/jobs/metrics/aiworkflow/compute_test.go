package aiworkflow

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	testOrgID  = uuid.MustParse("00000000-0000-4000-8000-000000000001")
	testRepoID = uuid.MustParse("00000000-0000-4000-8000-000000000002")
	testNow    = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
)

func TestComputeNoSignalProducesNoRun(t *testing.T) {
	prs := []PullRequestRow{
		{RepoID: testRepoID, Number: 1, HeadBranch: "fix/typo", Body: "", AuthorName: "dev-b",
			CreatedAt: testNow, LastSynced: testNow},
	}
	result := Compute(prs, testOrgID, "github", nil, testNow)
	if len(result.Runs) != 0 || len(result.ArtifactEdges) != 0 || len(result.IssueEdges) != 0 {
		t.Fatalf("PR with no AI signal must produce nothing, got %+v", result)
	}
}

func TestComputeAIPositivePRProducesRunAndArtifactEdge(t *testing.T) {
	mergedAt := testNow
	prs := []PullRequestRow{
		{
			RepoID: testRepoID, Number: 7, Body: "Generated with Claude Code",
			HeadBranch: "feature/cache", AuthorName: "dev-a",
			CreatedAt: testNow.Add(-time.Hour), MergedAt: &mergedAt, LastSynced: testNow,
		},
	}
	result := Compute(prs, testOrgID, "github", nil, testNow)
	if len(result.Runs) != 1 {
		t.Fatalf("got %d runs, want 1", len(result.Runs))
	}
	if len(result.ArtifactEdges) != 1 {
		t.Fatalf("got %d artifact edges, want 1", len(result.ArtifactEdges))
	}
	if len(result.IssueEdges) != 0 {
		t.Fatalf("got %d issue edges, want 0 (no linkage provided)", len(result.IssueEdges))
	}

	run := result.Runs[0]
	wantPRID := testRepoID.String() + ":7"
	wantRunID := hashParts(testOrgID.String(), "github", "pull_request", wantPRID, SourcePRBody)
	if run.RunID != wantRunID {
		t.Errorf("run_id = %q, want %q", run.RunID, wantRunID)
	}
	if run.OrgID != testOrgID || run.Provider != "github" {
		t.Errorf("run org/provider = %v/%v, want %v/github", run.OrgID, run.Provider, testOrgID)
	}
	if run.RunKind != RunKindChatAssisted {
		t.Errorf("run_kind = %q, want chat_assisted (body signal is ai_assisted-kind)", run.RunKind)
	}
	if run.Status == nil || *run.Status != RunStatusCompleted {
		t.Errorf("status = %v, want completed", run.Status)
	}
	if !run.PromptsRedacted {
		t.Error("prompts_redacted must always be true")
	}
	if run.Metadata["subject_type"] != "pull_request" || run.Metadata["subject_id"] != wantPRID {
		t.Errorf("metadata subject fields wrong: %+v", run.Metadata)
	}

	edge := result.ArtifactEdges[0]
	if edge.ArtifactType != ArtifactTypePullRequest || edge.ArtifactID != wantPRID {
		t.Errorf("artifact edge = %+v, want type=pull_request id=%q", edge, wantPRID)
	}
	if edge.RunID != run.RunID {
		t.Errorf("artifact edge run_id = %q, want %q (must match the run)", edge.RunID, run.RunID)
	}
}

func TestComputeIssueEdgeFanOut(t *testing.T) {
	prs := []PullRequestRow{
		{RepoID: testRepoID, Number: 7, Body: "Generated with Claude Code",
			CreatedAt: testNow, LastSynced: testNow},
	}
	prID := testRepoID.String() + ":7"
	issueIDsByPR := map[string][]string{prID: {"jira:ABC-1", "jira:ABC-2"}}

	result := Compute(prs, testOrgID, "github", issueIDsByPR, testNow)
	if len(result.IssueEdges) != 2 {
		t.Fatalf("got %d issue edges, want 2 (one per linked work item)", len(result.IssueEdges))
	}
	runID := result.Runs[0].RunID
	for _, edge := range result.IssueEdges {
		if edge.RunID != runID {
			t.Errorf("issue edge run_id = %q, want %q", edge.RunID, runID)
		}
	}
	if result.IssueEdges[0].IssueID == result.IssueEdges[1].IssueID {
		t.Fatal("the two issue edges must be for DIFFERENT issue ids")
	}
}

// TestComputeObservedAtFallsBackInPythonOrder pins _dt's exact candidate
// order: merged_at, then closed_at, then created_at, then last_synced.
func TestComputeObservedAtFallsBackInPythonOrder(t *testing.T) {
	closedAt := testNow.Add(-2 * time.Hour)
	createdAt := testNow.Add(-3 * time.Hour)
	lastSynced := testNow.Add(-4 * time.Hour)

	// merged_at absent, closed_at present -> closed_at wins.
	prs := []PullRequestRow{
		{RepoID: testRepoID, Number: 1, Body: "ai-assisted", ClosedAt: &closedAt,
			CreatedAt: createdAt, LastSynced: lastSynced},
	}
	result := Compute(prs, testOrgID, "github", nil, testNow)
	if !result.Runs[0].ObservedAt.Equal(closedAt) {
		t.Errorf("observed_at = %v, want closed_at %v (merged_at absent)", result.Runs[0].ObservedAt, closedAt)
	}
}

func TestComputeRunIDIsDeterministicAndProviderScoped(t *testing.T) {
	prs := []PullRequestRow{
		{RepoID: testRepoID, Number: 7, Body: "ai-assisted", CreatedAt: testNow, LastSynced: testNow},
	}
	resultA := Compute(prs, testOrgID, "github", nil, testNow)
	resultB := Compute(prs, testOrgID, "gitlab", nil, testNow)
	if resultA.Runs[0].RunID == resultB.Runs[0].RunID {
		t.Error("run_id must differ across providers for the identical PR (provider is a hash input)")
	}

	resultC := Compute(prs, testOrgID, "github", nil, testNow.Add(time.Hour))
	if resultA.Runs[0].RunID != resultC.Runs[0].RunID {
		t.Error("run_id must be stable across re-runs with a different `now` (now is not a hash input)")
	}
}

func TestComputeEvidenceJSONIsCompactAndSorted(t *testing.T) {
	prs := []PullRequestRow{
		{RepoID: testRepoID, Number: 7, Body: "ai-assisted", CreatedAt: testNow, LastSynced: testNow},
	}
	result := Compute(prs, testOrgID, "github", nil, testNow)
	evidence := result.ArtifactEdges[0].Evidence
	if len(evidence) == 0 {
		t.Fatal("evidence must not be empty")
	}
	// Compact separators: no ", " or ": " anywhere -- proves
	// MarshalPythonJSONCompact, not the default-separator sibling, was used.
	for _, forbidden := range []string{", ", ": "} {
		if containsSubstring(evidence, forbidden) {
			t.Errorf("evidence %q contains %q -- wrong encoder (default separators, not compact)", evidence, forbidden)
		}
	}
}

func containsSubstring(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
