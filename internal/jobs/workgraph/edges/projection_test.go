package edges

import (
	"testing"
	"time"
)

// The projection path was the least-tested surface in this port, and two of the
// last three defects landed in it: a null-guard written but never wired, and
// BuildBlockerProjection having no test at all while its doc comment made a
// specific behavioural claim about the empty case.
//
// The cause was cardinality. Every other test here uses either the frozen
// 6,531-row golden or exactly one row — never zero — and Python has an
// explicitly asymmetric empty-rows branch (builder.py:854-860).

// TestAnEmptyRunStillStampsAWatermark pins the claim BuildBlockerProjection's
// doc comment makes and nothing verified: a run producing no blocker edges
// takes the build clock rather than leaving the watermark null, because
// Python's `max(..., default=self._now)` does.
//
// A null watermark reads as "no progress"; a build-clock watermark reads as
// "ran, found nothing". Those are different facts and the projection is how
// anyone downstream tells them apart.
func TestAnEmptyRunStillStampsAWatermark(t *testing.T) {
	buildClock := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	run := BuildBlockerProjection("org", "", nil, buildClock)

	if run.InputWatermark.IsZero() {
		t.Fatal("empty run left the watermark zero; Python's max(..., default=self._now) " +
			"stamps the build clock, and a null watermark reads as progress that did not happen")
	}
	if !run.InputWatermark.Equal(buildClock) {
		t.Errorf("empty run stamped %v, want the build clock %v", run.InputWatermark, buildClock)
	}
}

// TestTheWatermarkIsTheMaximumEventTsNotTheLast. `max()` is order-independent;
// a loop that assigns rather than compares is not, and the frozen golden cannot
// tell them apart because its rows arrive in a fixed order.
func TestTheWatermarkIsTheMaximumEventTsNotTheLast(t *testing.T) {
	buildClock := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	early := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	ascending := BuildBlockerProjection("org", "", []Row{
		{EdgeType: EdgeTypeBlocks, EventTs: early},
		{EdgeType: EdgeTypeBlocks, EventTs: late},
	}, buildClock)
	descending := BuildBlockerProjection("org", "", []Row{
		{EdgeType: EdgeTypeBlocks, EventTs: late},
		{EdgeType: EdgeTypeBlocks, EventTs: early},
	}, buildClock)

	if !ascending.InputWatermark.Equal(late) {
		t.Errorf("ascending order gave %v, want the maximum %v", ascending.InputWatermark, late)
	}
	if !descending.InputWatermark.Equal(late) {
		t.Errorf("descending order gave %v, want the maximum %v — a loop that ASSIGNS rather "+
			"than compares would return the earlier value here and would still pass on the "+
			"golden, whose rows arrive in one fixed order",
			descending.InputWatermark, late)
	}
}

// TestOnlyBlockersCountTowardTheProjection. Audit gate 30: the projection
// filters on EdgeType BLOCKS, which is also why IS_BLOCKED_BY is dead as an
// output. A non-blocker edge must not move the watermark or the count.
func TestOnlyBlockersCountTowardTheProjection(t *testing.T) {
	buildClock := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	future := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

	run := BuildBlockerProjection("org", "", []Row{
		{EdgeType: "relates", EventTs: future},
	}, buildClock)

	if run.InputWatermark.Equal(future) {
		t.Error("a `relates` edge moved the blocker watermark; the projection filters on BLOCKS")
	}
	if !run.InputWatermark.Equal(buildClock) {
		t.Errorf("with no blocker edges the watermark should be the build clock, got %v",
			run.InputWatermark)
	}
}

// TestCleanupPlanAtEveryCardinality. BuildCleanupPlan was called exactly once
// in this package, with all 6,531 golden rows, inside a determinism test.
// Zero, one, and a page boundary were all untested.
func TestCleanupPlanAtEveryCardinality(t *testing.T) {
	blocker := DependencyRow{
		SourceWorkItemID: "gh:o/r#1", TargetWorkItemID: "gh:o/r#2",
		RelationshipType: "blocks", RelationshipRaw: "blocks",
	}

	if plan := BuildCleanupPlan(nil, nil); len(plan.Pages) != 0 {
		t.Errorf("zero rows produced %d pages; nothing to delete means no statement",
			len(plan.Pages))
	}
	// Six ids per blocker row, as the audit records.
	one := BuildCleanupPlan([]DependencyRow{blocker}, nil)
	if len(one.Pages) != 1 {
		t.Fatalf("one blocker row produced %d pages, want 1", len(one.Pages))
	}
	if got := len(one.Pages[0]); got != 6 {
		t.Errorf("one blocker row produced %d ids, want 6", got)
	}
	// Ordering is load-bearing: the plan is paged, so an unstable order
	// redistributes ids across pages between runs.
	again := BuildCleanupPlan([]DependencyRow{blocker}, nil)
	for index, id := range one.Pages[0] {
		if again.Pages[0][index] != id {
			t.Fatalf("two identical inputs produced different id order at %d", index)
		}
	}
}
