package investment

import (
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// TestRepoHierarchyCascadeChainsThroughAChildResolvedParent is the CHAOS-5459
// regression test and it FAILS on main.
//
// The shape matters, and my first attempt at this test was VACUOUS: a
// grandparent with an own signal is already reachable, because ancestorCascade
// walks the parent chain to depth 10 in a single pass. That case has been
// covered since CHAOS-5359 (TestRepoHierarchyCascadeDepthTwo) and proves
// nothing about chaining.
//
// The case that genuinely blocks is a parent resolved DOWNWARD, from its other
// child:
//
//	S (own signal) --parent--> P <--parent-- C
//
// P has no signal and no parent, so nothing above it exists to walk to. P can
// only be resolved by childrenCascade, from its sibling-of-C child S. C's own
// upward walk reaches P and stops -- pre-fix P is nil in ownRepoByComponent,
// so C sees nothing and stays unassigned forever, no matter how deep the walk
// goes. Post-fix, pass 1 resolves P from S and pass 2 resolves C from P.
//
// This is the majority shape in production: of the 178 unresolved sub-issues
// whose parent DOES have a repo, 81 have a parent resolved via `children`.
func TestRepoHierarchyCascadeChainsThroughAChildResolvedParent(t *testing.T) {
	repoA := "11111111-1111-4111-8111-111111111111"
	components := []units.Component{
		issueComponentOf("S"), // 0: own signal, child of P
		issueComponentOf("P"), // 1: no signal, no parent -- resolvable only from S
		issueComponentOf("C"), // 2: child of P -- only reachable once P is resolved
	}
	ownRepoByComponent := map[int]*uuid.UUID{0: mustRepoID(t, repoA), 1: nil, 2: nil}
	workItems := map[string]chquery.WorkItem{
		"S": {WorkItemID: "S", ParentID: "P"},
		"P": {WorkItemID: "P"},
		"C": {WorkItemID: "C", ParentID: "P"},
	}

	got := computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, buildIssueComponentIndex(components))

	parent, ok := got[1]
	if !ok {
		t.Fatal("component 1 (P) got no cascade result; the children tier is pre-existing behaviour and must still work")
	}
	if parent.Source != RepoSourceChildren {
		t.Errorf("P repo_source = %q, want %q", parent.Source, RepoSourceChildren)
	}
	if parent.Hops != 1 {
		t.Errorf("P resolved at %d hops, want 1", parent.Hops)
	}

	child, ok := got[2]
	if !ok {
		t.Fatal("component 2 (C) got no cascade result -- the cascade did not chain through a " +
			"child-resolved parent, which is exactly the CHAOS-5459 defect")
	}
	if child.RepoID == nil || child.RepoID.String() != mustRepoID(t, repoA).String() {
		t.Errorf("C repo = %v, want %s", child.RepoID, repoA)
	}
	if child.Hops != 2 {
		t.Errorf("C resolved at %d hops, want 2", child.Hops)
	}
	// Provenance names the TRANSITIVE ROOT (S, whose own signal supplied the
	// repo), not P, which has no repo of its own to point an auditor at.
	if child.Source != AncestorSource("S") {
		t.Errorf("C repo_source = %q, want %q (the transitive root, not the direct parent)",
			child.Source, AncestorSource("S"))
	}
	if child.RootIssueID != "S" {
		t.Errorf("C root = %q, want \"S\"", child.RootIssueID)
	}
}

// TestRepoHierarchyCascadeReachesADeepChain proves the fixed point is a real
// loop and not a hard-coded second pass.
//
// The chain is built DOWNWARD on purpose -- L0 (own signal) is the child of
// L1, which is the child of L2, and so on -- because childrenCascade looks
// exactly one level down with no recursion of its own. Hop N is therefore
// only reachable by running the tiers N times. An upward chain would be
// resolved in one pass by ancestorCascade's depth-10 walk and would prove
// nothing.
func TestRepoHierarchyCascadeReachesADeepChain(t *testing.T) {
	repoA := "11111111-1111-4111-8111-111111111111"
	ids := []string{"L0", "L1", "L2", "L3", "L4", "L5"}
	components := make([]units.Component, len(ids))
	ownRepoByComponent := map[int]*uuid.UUID{}
	workItems := map[string]chquery.WorkItem{}
	for i, id := range ids {
		components[i] = issueComponentOf(id)
		ownRepoByComponent[i] = nil
		item := chquery.WorkItem{WorkItemID: id}
		if i < len(ids)-1 {
			item.ParentID = ids[i+1] // L0's parent is L1, L1's is L2, ...
		}
		workItems[id] = item
	}
	ownRepoByComponent[0] = mustRepoID(t, repoA)

	got := computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, buildIssueComponentIndex(components))

	for i := 1; i < len(ids); i++ {
		cascade, ok := got[i]
		if !ok {
			t.Fatalf("component %d (%s) unresolved; the chain broke at hop %d", i, ids[i], i)
		}
		if cascade.Hops != i {
			t.Errorf("%s resolved at %d hops, want %d", ids[i], cascade.Hops, i)
		}
		if cascade.RootIssueID != "L0" {
			t.Errorf("%s root = %q, want \"L0\" -- the root must not drift along the chain", ids[i], cascade.RootIssueID)
		}
	}
}

// TestRepoHierarchyCascadeTerminatesOnAParentCycle: two components whose
// issues name each other as parent, neither with a signal. The component-level
// guard (a resolved component is never revisited) plus the per-chain `visited`
// set must both hold, and the call must RETURN rather than spin.
func TestRepoHierarchyCascadeTerminatesOnAParentCycle(t *testing.T) {
	components := []units.Component{issueComponentOf("A"), issueComponentOf("B")}
	ownRepoByComponent := map[int]*uuid.UUID{0: nil, 1: nil}
	workItems := map[string]chquery.WorkItem{
		"A": {WorkItemID: "A", ParentID: "B"},
		"B": {WorkItemID: "B", ParentID: "A"},
	}

	done := make(chan map[int]repoCascade, 1)
	go func() {
		done <- computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, buildIssueComponentIndex(components))
	}()
	select {
	case got := <-done:
		if len(got) != 0 {
			t.Errorf("a cycle with no signal anywhere resolved %d components: %v", len(got), got)
		}
	case <-t.Context().Done():
		t.Fatal("computeRepoHierarchyCascade did not terminate on a parent cycle")
	}
}

// TestRepoHierarchyCascadeCycleWithASignalResolvesOnceAndStops: the same cycle,
// but one member has an own signal. The other must resolve exactly once, and
// the resolved member must NOT be re-resolved or upgraded on a later pass.
func TestRepoHierarchyCascadeCycleWithASignalResolvesOnceAndStops(t *testing.T) {
	repoA := "11111111-1111-4111-8111-111111111111"
	components := []units.Component{issueComponentOf("A"), issueComponentOf("B")}
	ownRepoByComponent := map[int]*uuid.UUID{0: mustRepoID(t, repoA), 1: nil}
	workItems := map[string]chquery.WorkItem{
		"A": {WorkItemID: "A", ParentID: "B"},
		"B": {WorkItemID: "B", ParentID: "A"},
	}

	got := computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, buildIssueComponentIndex(components))

	if _, resolved := got[0]; resolved {
		t.Error("component 0 has its OWN signal and must never appear as a cascade result")
	}
	cascade, ok := got[1]
	if !ok {
		t.Fatal("component 1 (B) should inherit from A across the cycle")
	}
	if cascade.Hops != 1 {
		t.Errorf("B resolved at %d hops, want 1", cascade.Hops)
	}
}

// TestRepoHierarchyCascadeIsDeterministicAcrossRuns guards the atomic
// pass-commit. If a pass applied its results as it discovered them, a
// component's answer would depend on Go's randomised map iteration order, and
// two runs over identical input could disagree. Repeating the call must be
// bit-for-bit identical.
func TestRepoHierarchyCascadeIsDeterministicAcrossRuns(t *testing.T) {
	repoA := "11111111-1111-4111-8111-111111111111"
	repoB := "22222222-2222-4222-8222-222222222222"
	components := []units.Component{
		issueComponentOf("R1"), issueComponentOf("R2"),
		issueComponentOf("M1"), issueComponentOf("M2"), issueComponentOf("M3"),
	}
	ownRepoByComponent := map[int]*uuid.UUID{
		0: mustRepoID(t, repoA), 1: mustRepoID(t, repoB), 2: nil, 3: nil, 4: nil,
	}
	workItems := map[string]chquery.WorkItem{
		"R1": {WorkItemID: "R1"},
		"R2": {WorkItemID: "R2"},
		"M1": {WorkItemID: "M1", ParentID: "R1"},
		"M2": {WorkItemID: "M2", ParentID: "M1"},
		"M3": {WorkItemID: "M3", ParentID: "M2"},
	}
	index := buildIssueComponentIndex(components)

	first := computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, index)
	for run := 0; run < 20; run++ {
		again := computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, index)
		if len(again) != len(first) {
			t.Fatalf("run %d resolved %d components, first run resolved %d", run, len(again), len(first))
		}
		for idx, want := range first {
			got, ok := again[idx]
			if !ok {
				t.Fatalf("run %d lost component %d", run, idx)
			}
			if got.Source != want.Source || got.Hops != want.Hops || got.RootIssueID != want.RootIssueID ||
				fmt.Sprint(got.RepoID) != fmt.Sprint(want.RepoID) {
				t.Fatalf("run %d component %d = %+v, first run = %+v", run, idx, got, want)
			}
		}
	}
}

// TestChurnRepoForComponentSeedsAPRLinkedComponent is the second half of
// CHAOS-5459: a component with no repo-bearing EDGE but with its own PR churn
// is an own signal, so a PR-linked parent can be inherited from. Pre-fix only
// edge repo_ids seeded ownRepoByComponent, which is why `own_edges` appeared
// on none of the 198 unresolved sub-issues' parents.
func TestChurnRepoForComponentSeedsAPRLinkedComponent(t *testing.T) {
	repoA := mustRepoID(t, "11111111-1111-4111-8111-111111111111")
	prID := fmt.Sprintf("%s#pr7", repoA)
	component := units.Component{Nodes: []units.NodeKey{
		{Type: "issue", ID: "I1"},
		{Type: "pr", ID: prID},
	}}
	entities := entitySet{PRChurn: map[string]float64{prID: 42}, CommitChurn: map[string]float64{}}

	got := churnRepoForComponent(component, entities)
	if got == nil || got.String() != repoA.String() {
		t.Fatalf("churnRepoForComponent = %v, want %s", got, repoA)
	}
}

// TestChurnRepoForComponentRefusesAmbiguityAndSilence is the NEGATIVE control
// for the test above: without it, a function that returned the first repo it
// saw -- or any repo at all -- would still pass. Each case must yield nil.
func TestChurnRepoForComponentRefusesAmbiguityAndSilence(t *testing.T) {
	repoA := mustRepoID(t, "11111111-1111-4111-8111-111111111111")
	repoB := mustRepoID(t, "22222222-2222-4222-8222-222222222222")
	prA := fmt.Sprintf("%s#pr1", repoA)
	prB := fmt.Sprintf("%s#pr2", repoB)

	cases := []struct {
		name      string
		component units.Component
		entities  entitySet
	}{
		{
			name: "two repos is no single answer",
			component: units.Component{Nodes: []units.NodeKey{
				{Type: "pr", ID: prA}, {Type: "pr", ID: prB},
			}},
			entities: entitySet{PRChurn: map[string]float64{prA: 10, prB: 10}},
		},
		{
			name: "a PR with zero churn is not a signal",
			component: units.Component{Nodes: []units.NodeKey{
				{Type: "pr", ID: prA},
			}},
			entities: entitySet{PRChurn: map[string]float64{prA: 0}},
		},
		{
			name:      "an issue-only component has nothing to read",
			component: issueComponentOf("I1"),
			entities:  entitySet{PRChurn: map[string]float64{}, CommitChurn: map[string]float64{}},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := churnRepoForComponent(testCase.component, testCase.entities); got != nil {
				t.Errorf("churnRepoForComponent = %v, want nil", got)
			}
		})
	}
}
