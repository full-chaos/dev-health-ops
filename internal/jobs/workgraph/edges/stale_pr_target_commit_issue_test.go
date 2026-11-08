package edges

import (
	"testing"
	"time"
)

func commitIssueEdge(commit, edgeType, target string, lastSynced time.Time) Row {
	return Row{
		EdgeID:     EdgeID(NodeTypeCommit, commit, edgeType, NodeTypeIssue, target),
		SourceType: NodeTypeCommit, SourceID: commit, EdgeType: edgeType,
		TargetType: NodeTypeIssue, TargetID: target,
		Provenance: ProvenanceExplicitText, Confidence: 0.85, Evidence: "#7",
		DiscoveredAt: lastSynced, LastSynced: lastSynced, EventTs: lastSynced,
	}
}

func liveVersion(row Row) EdgeVersion {
	return EdgeVersion{Latest: row, MaxLastSynced: row.LastSynced}
}

func TestPlanStalePRTargetCommitIssueTombstonesRetiresOnlyPRShapedTargets(t *testing.T) {
	stored := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	clock := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)

	ghpr := commitIssueEdge("c1", EdgeTypeReferences, "ghpr:o/r#7", stored)
	ghprClose := commitIssueEdge("c2", EdgeTypeImplements, "ghpr:o/r#8", stored)
	gitlabMR := commitIssueEdge("c3", EdgeTypeReferences, "gitlab:g/p!9", stored)
	keep := []Row{
		commitIssueEdge("c4", EdgeTypeReferences, "gh:o/r#7", stored),
		commitIssueEdge("c5", EdgeTypeReferences, "linear:ACME-1", stored),
		commitIssueEdge("c6", EdgeTypeReferences, "jira:ACME-2", stored),
		commitIssueEdge("c7", EdgeTypeImplements, "gitlab:g/p#10", stored),
	}
	otherSource := Row{
		EdgeID: "pr-source", SourceType: NodeTypePR, SourceID: "r#pr1", EdgeType: EdgeTypeImplements,
		TargetType: NodeTypeIssue, TargetID: "ghpr:o/r#1", LastSynced: stored,
	}
	deleted := liveVersion(commitIssueEdge("c8", EdgeTypeReferences, "ghpr:o/r#11", stored))
	deleted.IsDeleted = true

	entries := []EdgeVersion{liveVersion(ghpr), liveVersion(ghprClose), liveVersion(gitlabMR), liveVersion(otherSource), deleted}
	for _, row := range keep {
		entries = append(entries, liveVersion(row))
	}
	versions := versionsOf(entries...)

	got := PlanStalePRTargetCommitIssueTombstones(versions, clock)
	if len(got) != 3 {
		t.Fatalf("got %d tombstones, want 3: %+v", len(got), got)
	}
	want := map[string]bool{ghpr.EdgeID: true, ghprClose.EdgeID: true, gitlabMR.EdgeID: true}
	for _, tombstone := range got {
		if !want[tombstone.EdgeID] {
			t.Errorf("unexpected tombstone for %s -> %s", tombstone.SourceID, tombstone.TargetID)
		}
	}

	second := EdgeVersions{}
	for _, entry := range versions {
		for _, tombstone := range got {
			if tombstone.EdgeID == entry.Latest.EdgeID {
				entry = EdgeVersion{Latest: tombstone, IsDeleted: true, MaxLastSynced: tombstone.LastSynced}
			}
		}
		second.Put(entry)
	}
	if again := PlanStalePRTargetCommitIssueTombstones(second, clock.Add(time.Hour)); len(again) != 0 {
		t.Fatalf("second plan wrote %d tombstones, want 0 (idempotent)", len(again))
	}
}
