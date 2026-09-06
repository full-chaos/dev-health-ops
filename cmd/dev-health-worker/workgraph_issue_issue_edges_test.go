package main

import "testing"

// TestBuildPostStepOrderIsPinned mirrors TestBuildPreStepOrderIsPinned: the
// declared order is asserted separately from the construction, so a test can
// read it without a ClickHouse connection and so appending is a deliberate act.
//
// Empty since CHAOS-4924 moved `issue_issue_edges` (the last remaining
// post-step) into buildPreStepOrder: its Python producer
// (`_build_issue_issue_edges`) was DELETED, so there is nothing left for a
// post-step to overwrite. A step belongs in buildPostStepOrder only if the
// Python stage that still runs would OVERWRITE its rows -- that condition no
// longer holds for anything in this build.
func TestBuildPostStepOrderIsPinned(t *testing.T) {
	want := []string{}
	got := buildPostStepOrder()

	if len(got) != len(want) {
		t.Fatalf(
			"build post-step order is %v, want %v (empty).\n"+
				"A step belongs here only if the Python stage that still runs would OVERWRITE "+
				"its rows. If you are adding one back, confirm that condition genuinely holds "+
				"-- issueIssueEdgesPreStep's own doc comment explains why it does not any more.",
			got, want,
		)
	}
}

// TestThePostStepIsNotAlsoAPreStep. The two declared orders must stay disjoint:
// a name in both would run the step twice per build and would collide in the
// ledger evidence, where fragments are keyed by step name.
func TestThePostStepIsNotAlsoAPreStep(t *testing.T) {
	pre := map[string]struct{}{}
	for _, name := range buildPreStepOrder() {
		pre[name] = struct{}{}
	}
	for _, name := range buildPostStepOrder() {
		if _, clash := pre[name]; clash {
			t.Errorf("%q is declared in BOTH orders: it would run twice and its two evidence "+
				"fragments would collide under one key", name)
		}
	}
}
