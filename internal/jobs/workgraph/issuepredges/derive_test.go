package issuepredges

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

func TestDeriveFastPathEdgesBuildsAnImplementsEdge(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	buildTime := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	prCreated := time.Date(2026, 7, 15, 9, 0, 0, 0, time.UTC)

	rows := []FastPathRow{{
		RepoID:      repoID,
		WorkItemID:  "jira:ABC-123",
		PRNumber:    42,
		Confidence:  1.0,
		Provenance:  "native",
		Evidence:    "closing_reference",
		PRCreatedAt: prCreated,
	}}

	out := DeriveFastPathEdges(rows, buildTime)
	if len(out) != 1 {
		t.Fatalf("got %d edges, want 1", len(out))
	}
	edge := out[0]
	wantPRID := edges.GeneratePRID(repoID, 42)
	if edge.SourceID != wantPRID {
		t.Errorf("SourceID = %q, want %q", edge.SourceID, wantPRID)
	}
	if edge.TargetID != "jira:ABC-123" {
		t.Errorf("TargetID = %q, want jira:ABC-123", edge.TargetID)
	}
	if edge.EdgeType != edges.EdgeTypeImplements {
		t.Errorf("EdgeType = %q, want %q", edge.EdgeType, edges.EdgeTypeImplements)
	}
	if edge.Provenance != "native" {
		t.Errorf("Provenance = %q, want native", edge.Provenance)
	}
	if !edge.EventTs.Equal(prCreated) {
		t.Errorf("EventTs = %v, want %v", edge.EventTs, prCreated)
	}
	if !edge.DiscoveredAt.Equal(buildTime) || !edge.LastSynced.Equal(buildTime) {
		t.Errorf("DiscoveredAt/LastSynced should both be buildTime, got %v/%v", edge.DiscoveredAt, edge.LastSynced)
	}
}

// TestDeriveFastPathEdgesConfidenceZeroQuirk pins Python's
// `float(row.get("confidence") or 1.0)` falsy-or behavior: a genuinely
// zero-confidence row is silently promoted to 1.0, not written as 0.0.
func TestDeriveFastPathEdgesConfidenceZeroQuirk(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	buildTime := time.Now().UTC()

	rows := []FastPathRow{{
		RepoID:     repoID,
		WorkItemID: "jira:ABC-1",
		PRNumber:   1,
		Confidence: 0,
		Provenance: "native",
		Evidence:   "",
	}}

	out := DeriveFastPathEdges(rows, buildTime)
	if len(out) != 1 {
		t.Fatalf("got %d edges, want 1", len(out))
	}
	if out[0].Confidence != 1.0 {
		t.Errorf("Confidence = %v, want 1.0 (falsy-or promotion)", out[0].Confidence)
	}
	if out[0].Evidence != "issue_pr_fast_path" {
		t.Errorf("Evidence = %q, want issue_pr_fast_path (falsy-or promotion)", out[0].Evidence)
	}
}

// TestDeriveFastPathEdgesEventTsFallsBackToBuildTime pins the zero-PRCreatedAt
// fallback (builder.py's `if not event_ts: event_ts = self._now`).
func TestDeriveFastPathEdgesEventTsFallsBackToBuildTime(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	buildTime := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)

	rows := []FastPathRow{{RepoID: repoID, WorkItemID: "jira:ABC-1", PRNumber: 1, Provenance: "native"}}
	out := DeriveFastPathEdges(rows, buildTime)
	if !out[0].EventTs.Equal(buildTime) {
		t.Errorf("EventTs = %v, want buildTime %v", out[0].EventTs, buildTime)
	}
}

func TestDeriveTextParseEdgesMatchesJiraReferenceAgainstLookup(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000002")
	buildTime := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	prCreated := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)

	prs := []PullRequestRow{{
		RepoID:    repoID,
		Number:    7,
		Title:     "Fix ABC-123 crash",
		CreatedAt: prCreated,
	}}
	workItems := []WorkItemRow{{RepoID: repoID, WorkItemID: "jira:ABC-123", Provider: "jira"}}

	result := DeriveTextParseEdges(prs, workItems, buildTime)
	if result.JiraRefsFound != 1 {
		t.Fatalf("JiraRefsFound = %d, want 1", result.JiraRefsFound)
	}
	if len(result.Edges) != 1 {
		t.Fatalf("got %d edges, want 1", len(result.Edges))
	}
	edge := result.Edges[0]
	if edge.TargetID != "jira:ABC-123" {
		t.Errorf("TargetID = %q, want jira:ABC-123", edge.TargetID)
	}
	if edge.Provenance != edges.ProvenanceExplicitText {
		t.Errorf("Provenance = %q, want %q", edge.Provenance, edges.ProvenanceExplicitText)
	}
	if edge.Confidence != 0.9 {
		t.Errorf("Confidence = %v, want 0.9", edge.Confidence)
	}
	if edge.Provider == nil || *edge.Provider != "jira" {
		t.Errorf("Provider = %v, want jira", edge.Provider)
	}
	if len(result.Links) != 1 {
		t.Fatalf("got %d links, want 1", len(result.Links))
	}
	if result.Links[0].WorkItemID != "jira:ABC-123" || result.Links[0].PRNumber != 7 {
		t.Errorf("Link = %+v, unexpected", result.Links[0])
	}
}

func TestDeriveTextParseEdgesGitHubCloseKeywordImplementsEdge(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000003")
	buildTime := time.Now().UTC()

	prs := []PullRequestRow{{
		RepoID: repoID,
		Number: 10,
		Body:   "Closes #55",
	}}
	workItems := []WorkItemRow{{RepoID: repoID, WorkItemID: "gh:owner/repo#55", Provider: "github"}}

	result := DeriveTextParseEdges(prs, workItems, buildTime)
	if len(result.Edges) != 1 {
		t.Fatalf("got %d edges, want 1", len(result.Edges))
	}
	if result.Edges[0].EdgeType != edges.EdgeTypeImplements {
		t.Errorf("EdgeType = %q, want %q for a closing keyword", result.Edges[0].EdgeType, edges.EdgeTypeImplements)
	}
}

func TestDeriveTextParseEdgesSkipsPRWithNoText(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000004")
	prs := []PullRequestRow{{RepoID: repoID, Number: 1}}
	workItems := []WorkItemRow{{RepoID: repoID, WorkItemID: "jira:ABC-1", Provider: "jira"}}

	result := DeriveTextParseEdges(prs, workItems, time.Now())
	if len(result.Edges) != 0 || len(result.Links) != 0 {
		t.Fatalf("a PR with no title/body/head_branch must be skipped, got edges=%d links=%d", len(result.Edges), len(result.Links))
	}
}

// TestDeriveTextParseEdgesLinearProviderIsNeverTextMatched pins builder.py's
// own comment: Linear (and any provider outside jira/github/gitlab) never
// enters the lookup, so a matching-looking reference in PR text can never
// resolve for it through this path.
func TestDeriveTextParseEdgesLinearProviderIsNeverTextMatched(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000005")
	prs := []PullRequestRow{{RepoID: repoID, Number: 1, Title: "Fix ABC-123"}}
	workItems := []WorkItemRow{{RepoID: repoID, WorkItemID: "linear:ABC-123", Provider: "linear"}}

	result := DeriveTextParseEdges(prs, workItems, time.Now())
	if len(result.Edges) != 0 {
		t.Fatalf("a linear work item must never resolve through text-parse lookups, got %d edges", len(result.Edges))
	}
	// The ref is still found (extraction is provider-agnostic); it just has
	// nothing to resolve against.
	if result.JiraRefsFound != 1 {
		t.Errorf("JiraRefsFound = %d, want 1 (extraction runs regardless of resolution)", result.JiraRefsFound)
	}
}
