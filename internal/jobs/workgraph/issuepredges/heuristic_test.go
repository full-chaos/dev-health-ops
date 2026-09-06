package issuepredges

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

func TestDeriveHeuristicEdgesMatchesNearestPRWithinWindow(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000010")
	buildTime := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	updatedAt := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)

	inputs := HeuristicInputs{
		WorkItems: []HeuristicWorkItemRow{
			{RepoID: repoID, WorkItemID: "jira:HEUR-1", UpdatedAt: updatedAt},
		},
		PullRequests: []HeuristicPullRequestRow{
			// Far outside the window.
			{RepoID: repoID, Number: 1, CreatedAt: updatedAt.AddDate(0, -1, 0)},
			// Nearest within window (2 hours away).
			{RepoID: repoID, Number: 2, CreatedAt: updatedAt.Add(2 * time.Hour)},
			// Also within window but further away (1 day).
			{RepoID: repoID, Number: 3, CreatedAt: updatedAt.Add(24 * time.Hour)},
		},
		HeuristicDaysWindow: 7,
		HeuristicConfidence: 0.3,
	}

	result := DeriveHeuristicEdges(inputs, buildTime)
	if len(result.Edges) != 1 {
		t.Fatalf("got %d edges, want 1", len(result.Edges))
	}
	wantPRID := edges.GeneratePRID(repoID, 2)
	if result.Edges[0].SourceID != wantPRID {
		t.Errorf("SourceID = %q, want %q (the nearest PR, not the closest by number)", result.Edges[0].SourceID, wantPRID)
	}
	if result.Edges[0].EdgeType != edges.EdgeTypeRelates {
		t.Errorf("EdgeType = %q, want %q", result.Edges[0].EdgeType, edges.EdgeTypeRelates)
	}
	if result.Edges[0].Provenance != edges.ProvenanceHeuristic {
		t.Errorf("Provenance = %q, want %q", result.Edges[0].Provenance, edges.ProvenanceHeuristic)
	}
	if result.Edges[0].Evidence != "time_window_7d" {
		t.Errorf("Evidence = %q, want time_window_7d", result.Edges[0].Evidence)
	}
	if len(result.Links) != 1 || result.Links[0].PRNumber != 2 {
		t.Fatalf("Links = %+v, want exactly one link to PR 2", result.Links)
	}
}

func TestDeriveHeuristicEdgesExcludesAnyWorkItemWithAnExplicitLink(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000011")
	updatedAt := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)

	inputs := HeuristicInputs{
		WorkItems: []HeuristicWorkItemRow{
			{RepoID: repoID, WorkItemID: "jira:HEUR-2", UpdatedAt: updatedAt},
		},
		PullRequests: []HeuristicPullRequestRow{
			{RepoID: repoID, Number: 99, CreatedAt: updatedAt.Add(time.Hour)},
		},
		// The work item has an explicit link to a DIFFERENT PR number (5),
		// not 99 -- but Python's exclusion is item-level, not pair-level, so
		// this must still exclude the work item entirely from heuristic
		// matching, even against the unrelated PR 99.
		ExplicitLinks:       []ExplicitLink{{WorkItemID: "jira:HEUR-2", PRNumber: 5}},
		HeuristicDaysWindow: 7,
		HeuristicConfidence: 0.3,
	}

	result := DeriveHeuristicEdges(inputs, time.Now())
	if len(result.Edges) != 0 {
		t.Fatalf("a work item with ANY explicit link must be excluded entirely, got %d edges", len(result.Edges))
	}
}

func TestDeriveHeuristicEdgesZeroWindowShortCircuits(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000012")
	inputs := HeuristicInputs{
		WorkItems:           []HeuristicWorkItemRow{{RepoID: repoID, WorkItemID: "jira:HEUR-3", UpdatedAt: time.Now()}},
		PullRequests:        []HeuristicPullRequestRow{{RepoID: repoID, Number: 1, CreatedAt: time.Now()}},
		HeuristicDaysWindow: 0,
	}
	result := DeriveHeuristicEdges(inputs, time.Now())
	if len(result.Edges) != 0 || len(result.Links) != 0 {
		t.Fatalf("HeuristicDaysWindow=0 must short-circuit to an empty result, got edges=%d links=%d", len(result.Edges), len(result.Links))
	}
}

// TestDeriveHeuristicEdgesOutsideWindowFindsNoMatch pins the binary-search
// bound semantics: a PR strictly outside [updated_at-window, updated_at+window]
// must never match, even if it is the only PR for that repo.
func TestDeriveHeuristicEdgesOutsideWindowFindsNoMatch(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000013")
	updatedAt := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	inputs := HeuristicInputs{
		WorkItems: []HeuristicWorkItemRow{{RepoID: repoID, WorkItemID: "jira:HEUR-4", UpdatedAt: updatedAt}},
		PullRequests: []HeuristicPullRequestRow{
			{RepoID: repoID, Number: 1, CreatedAt: updatedAt.AddDate(0, 0, -8)}, // 8 days before, window is 7
		},
		HeuristicDaysWindow: 7,
		HeuristicConfidence: 0.3,
	}
	result := DeriveHeuristicEdges(inputs, time.Now())
	if len(result.Edges) != 0 {
		t.Fatalf("a PR 8 days outside a 7-day window must not match, got %d edges", len(result.Edges))
	}
}

// TestDeriveHeuristicEdgesWindowBoundaryIsInclusive pins that a PR exactly
// AT the window boundary (window_seconds away) DOES match -- Python's
// bisect_right on the upper bound includes values equal to it.
func TestDeriveHeuristicEdgesWindowBoundaryIsInclusive(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000014")
	updatedAt := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	inputs := HeuristicInputs{
		WorkItems: []HeuristicWorkItemRow{{RepoID: repoID, WorkItemID: "jira:HEUR-5", UpdatedAt: updatedAt}},
		PullRequests: []HeuristicPullRequestRow{
			{RepoID: repoID, Number: 1, CreatedAt: updatedAt.AddDate(0, 0, 7)}, // exactly at the 7-day boundary
		},
		HeuristicDaysWindow: 7,
		HeuristicConfidence: 0.3,
	}
	result := DeriveHeuristicEdges(inputs, time.Now())
	if len(result.Edges) != 1 {
		t.Fatalf("a PR exactly at the window boundary must match (inclusive bound), got %d edges", len(result.Edges))
	}
}

func TestDeriveHeuristicEdgesSkipsWorkItemWithNoUpdatedAt(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000015")
	inputs := HeuristicInputs{
		WorkItems:           []HeuristicWorkItemRow{{RepoID: repoID, WorkItemID: "jira:HEUR-6"}}, // zero UpdatedAt
		PullRequests:        []HeuristicPullRequestRow{{RepoID: repoID, Number: 1, CreatedAt: time.Now()}},
		HeuristicDaysWindow: 7,
		HeuristicConfidence: 0.3,
	}
	result := DeriveHeuristicEdges(inputs, time.Now())
	if len(result.Edges) != 0 {
		t.Fatalf("a work item with no updated_at must be skipped, got %d edges", len(result.Edges))
	}
}

func TestDeriveHeuristicEdgesSkipsRepoWithNoPullRequests(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000016")
	inputs := HeuristicInputs{
		WorkItems:           []HeuristicWorkItemRow{{RepoID: repoID, WorkItemID: "jira:HEUR-7", UpdatedAt: time.Now()}},
		PullRequests:        nil,
		HeuristicDaysWindow: 7,
		HeuristicConfidence: 0.3,
	}
	result := DeriveHeuristicEdges(inputs, time.Now())
	if len(result.Edges) != 0 {
		t.Fatalf("a repo with no PRs must yield no matches, got %d edges", len(result.Edges))
	}
}
