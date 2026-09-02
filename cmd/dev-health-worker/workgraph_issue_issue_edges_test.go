package main

import "testing"

// TestBuildPostStepOrderIsPinned mirrors TestBuildPreStepOrderIsPinned: the
// declared order is asserted separately from the construction, so a test can
// read it without a ClickHouse connection and so appending is a deliberate act.
//
// A step belongs in THIS list rather than the pre-step one when Python's build
// would OVERWRITE what it writes. That is a narrower condition than the
// pre-step ordering invariant and is easy to get backwards: the pre-step rule
// is about what a later stage READS, this one is about what a later stage
// WRITES over.
func TestBuildPostStepOrderIsPinned(t *testing.T) {
	want := []string{"issue_issue_edges"}
	got := buildPostStepOrder()

	if len(got) != len(want) {
		t.Fatalf(
			"build post-step order is %v, want %v.\n"+
				"A step belongs here only if the Python stage that still runs would OVERWRITE "+
				"its rows — `issue_issue_edges` is here because builder.py:905 writes "+
				"confidence=1.0 over variant-C's 0.9 and work_graph_edges is "+
				"ReplacingMergeTree(last_synced). If the Python stage has retired, the step "+
				"should MOVE to buildPreStepOrder rather than stay here: being last writer is a "+
				"property this arrangement depends on, not one worth keeping.",
			got, want,
		)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("build post-step order is %v, want %v", got, want)
		}
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
