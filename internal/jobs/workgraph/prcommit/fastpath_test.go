package prcommit_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/prcommit"
)

func TestBuildFastPathEdgesProducesDeterministicContainsEdge(t *testing.T) {
	repo := uuid.New()
	buildTime := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	authorWhen := time.Date(2026, 9, 1, 8, 30, 0, 0, time.UTC)

	rows := []prcommit.FastPathRow{
		{
			RepoID: repo, PRNumber: 42, CommitHash: "abc123",
			Confidence: 0.9, Provenance: "explicit_text", Evidence: "commit_message_pr_ref",
			AuthorWhen: authorWhen,
		},
	}

	got := prcommit.BuildFastPathEdges("org-a", rows, buildTime)
	if len(got) != 1 {
		t.Fatalf("got %d edges, want 1", len(got))
	}
	edge := got[0]

	wantPRID := edges.GeneratePRID(repo, 42)
	wantCommitID := edges.GenerateCommitID(repo, "abc123")
	wantEdgeID := edges.EdgeID(edges.NodeTypePR, wantPRID, edges.EdgeTypeContains, edges.NodeTypeCommit, wantCommitID)

	if edge.EdgeID != wantEdgeID {
		t.Fatalf("edge id mismatch: got %s want %s", edge.EdgeID, wantEdgeID)
	}
	if edge.SourceType != edges.NodeTypePR || edge.SourceID != wantPRID {
		t.Fatalf("unexpected source: %+v", edge)
	}
	if edge.TargetType != edges.NodeTypeCommit || edge.TargetID != wantCommitID {
		t.Fatalf("unexpected target: %+v", edge)
	}
	if edge.EdgeType != edges.EdgeTypeContains {
		t.Fatalf("unexpected edge type: %s", edge.EdgeType)
	}
	if edge.EventTs != authorWhen {
		t.Fatalf("event_ts should be the row's author_when, got %v want %v", edge.EventTs, authorWhen)
	}
	if !edge.Day.Equal(edges.DayFor(authorWhen)) {
		t.Fatalf("day mismatch: got %v want %v", edge.Day, edges.DayFor(authorWhen))
	}
	if edge.DiscoveredAt != buildTime || edge.LastSynced != buildTime {
		t.Fatalf("discovered_at/last_synced should be the build clock, got %+v", edge)
	}
	if edge.RepoID == nil || *edge.RepoID != repo {
		t.Fatalf("repo_id should be set, got %+v", edge.RepoID)
	}
}

func TestBuildFastPathEdgesConfidenceZeroPromotesToOne(t *testing.T) {
	// Preserves builder.py:1949's `float(row.get("confidence") or 1.0)`
	// truthiness quirk: a stored 0.0 reads back as 1.0.
	repo := uuid.New()
	rows := []prcommit.FastPathRow{
		{RepoID: repo, PRNumber: 1, CommitHash: "zero-confidence", Confidence: 0, Provenance: "heuristic"},
	}

	got := prcommit.BuildFastPathEdges("org-a", rows, time.Now().UTC())
	if len(got) != 1 {
		t.Fatalf("got %d edges, want 1", len(got))
	}
	if got[0].Confidence != 1.0 {
		t.Fatalf("zero confidence must be promoted to 1.0, got %v", got[0].Confidence)
	}
}

func TestBuildFastPathEdgesEventTsFallsBackToBuildTimeWhenAuthorWhenIsZero(t *testing.T) {
	repo := uuid.New()
	buildTime := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	rows := []prcommit.FastPathRow{
		{RepoID: repo, PRNumber: 1, CommitHash: "no-author-when", Confidence: 0.6, Provenance: "heuristic"},
	}

	got := prcommit.BuildFastPathEdges("org-a", rows, buildTime)
	if len(got) != 1 {
		t.Fatalf("got %d edges, want 1", len(got))
	}
	if got[0].EventTs != buildTime {
		t.Fatalf("event_ts should fall back to build time, got %v want %v", got[0].EventTs, buildTime)
	}
}

func TestBuildFastPathEdgesUnrecognizedProvenanceDefaultsToNative(t *testing.T) {
	// Mirrors _parse_provenance's unconditional NATIVE fallback for any value
	// outside the three known literals.
	repo := uuid.New()
	rows := []prcommit.FastPathRow{
		{RepoID: repo, PRNumber: 1, CommitHash: "weird-provenance", Confidence: 0.5, Provenance: "something-else"},
		{RepoID: repo, PRNumber: 2, CommitHash: "empty-provenance", Confidence: 0.5, Provenance: ""},
	}

	got := prcommit.BuildFastPathEdges("org-a", rows, time.Now().UTC())
	if len(got) != 2 {
		t.Fatalf("got %d edges, want 2", len(got))
	}
	for _, edge := range got {
		if edge.Provenance != edges.ProvenanceNative {
			t.Fatalf("unrecognized/empty provenance must default to native, got %s", edge.Provenance)
		}
	}
}

func TestBuildFastPathEdgesEmptyEvidenceDefaultsToFastPathTag(t *testing.T) {
	repo := uuid.New()
	rows := []prcommit.FastPathRow{
		{RepoID: repo, PRNumber: 1, CommitHash: "no-evidence", Confidence: 0.6, Provenance: "heuristic", Evidence: ""},
	}

	got := prcommit.BuildFastPathEdges("org-a", rows, time.Now().UTC())
	if len(got) != 1 {
		t.Fatalf("got %d edges, want 1", len(got))
	}
	if got[0].Evidence != "pr_commit_fast_path" {
		t.Fatalf("empty evidence should default to pr_commit_fast_path, got %q", got[0].Evidence)
	}
}
