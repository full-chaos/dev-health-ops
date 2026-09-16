package edges

import (
	"testing"
	"time"
)

// TestUnreachableIsBlockedByMapEntryIsRemoved pins a mapping-table invariant: the
// blocker family is forced to BLOCKS before dependencyTypeMap is ever
// consulted, so an "is_blocked_by" entry in that map could never be reached.
// The map must not carry it.
func TestUnreachableIsBlockedByMapEntryIsRemoved(t *testing.T) {
	if edgeType, exists := dependencyTypeMap["is_blocked_by"]; exists {
		t.Fatalf("dependencyTypeMap still carries the unreachable is_blocked_by entry (-> %q)", edgeType)
	}
}

// TestRelationshipSemanticsVersionIsCaseFolded pins a case-folding invariant: a row
// carrying a mixed-case semantics version must take the SAME canonical branch
// as its lowercase equivalent, rather than falling through to the legacy
// orientation heuristics.
//
// "blocked_by" is deliberately used rather than "blocks": for a "blocks" row,
// the canonical branch and the legacy fallthrough happen to agree (both keep
// source/target), so that relationship cannot distinguish the two paths. For
// a gh:-prefixed "blocked_by" row with raw == relationship, the canonical
// branch swaps the endpoints (trusting the stored orientation) while the
// legacy fallthrough's GitHub-body-parsing branch does NOT -- a real
// direction flip, not just an internal-state difference.
func TestRelationshipSemanticsVersionIsCaseFolded(t *testing.T) {
	row := DependencyRow{
		SourceWorkItemID: "gh:acme/app#1",
		TargetWorkItemID: "gh:acme/app#2",
		RelationshipType: "blocked_by",
		RelationshipRaw:  "blocked_by",
		SemanticsVersion: "Canonical-Blocks.V2",
	}
	source, target, edgeType := CanonicalDependency(row)
	if edgeType != EdgeTypeBlocks {
		t.Fatalf("edge type = %q, want %q", edgeType, EdgeTypeBlocks)
	}
	// Canonical branch 2 trusts the stored orientation: relationship is
	// "blocked_by" (not "blocks"), so the endpoints are swapped.
	if source != row.TargetWorkItemID || target != row.SourceWorkItemID {
		t.Fatalf("got %s -> %s, want the canonical (swapped) orientation %s -> %s "+
			"-- a mixed-case semantics version must not fall through to the legacy heuristics",
			source, target, row.TargetWorkItemID, row.SourceWorkItemID)
	}
}

// TestEventTimestampIsExactlyTheRowsNoNowFallback pins a timestamp invariant: a
// row with no last_synced gets a zero-valued event_ts, not the build clock.
// Python's `or self._now` fallback on this field can only fire on a literal
// None, which never reaches this port (the read scans a time.Time, not an
// optional) -- so there is no case left in which a substituted "now" is
// correct, and a Go "if zero-value, use now" translation would be a new
// behaviour Python never had.
func TestEventTimestampIsExactlyTheRowsNoNowFallback(t *testing.T) {
	buildClock := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	row := DependencyRow{
		SourceWorkItemID: "gh:acme/app#3", TargetWorkItemID: "gh:acme/app#4",
		RelationshipType: "relates", RelationshipRaw: "relates",
		// LastSynced left at its zero value: no timestamp on the row.
	}
	result, err := DeriveIssueIssueEdges([]DependencyRow{row}, buildClock)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if len(result.Edges) != 1 {
		t.Fatalf("expected exactly one derived edge, got %d", len(result.Edges))
	}
	edge := result.Edges[0]
	if !edge.EventTs.IsZero() {
		t.Fatalf("event_ts = %v, want the zero value (exactly the row's) -- "+
			"a build-clock substitution here is a divergence Python never had",
			edge.EventTs)
	}
	if edge.EventTs.Equal(buildClock) {
		t.Fatal("event_ts equals the build clock; the zero-value-means-now fallback is still present")
	}
	if result.MissingTimestamps != 1 {
		t.Fatalf("MissingTimestamps = %d, want 1", result.MissingTimestamps)
	}
	// discovered_at and last_synced ARE the build clock, by design (see Row's
	// doc comment) -- only event_ts must carry the row's own (zero) value.
	if !edge.DiscoveredAt.Equal(buildClock) || !edge.LastSynced.Equal(buildClock) {
		t.Fatalf("discovered_at/last_synced = %v/%v, want the build clock %v",
			edge.DiscoveredAt, edge.LastSynced, buildClock)
	}
}
