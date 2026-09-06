package investment

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

func mustRepoID(t *testing.T, raw string) *uuid.UUID {
	t.Helper()
	repoID := units.ParseRepoID(raw)
	if repoID == nil {
		t.Fatalf("test fixture repo id %q failed to parse", raw)
	}
	return repoID
}

func issueComponentOf(nodes ...string) units.Component {
	keys := make([]units.NodeKey, len(nodes))
	for i, id := range nodes {
		keys[i] = units.NodeKey{Type: "issue", ID: id}
	}
	return units.Component{Nodes: keys}
}

// TestRepoHierarchyCascadeDepthOne pins the depth-1 ancestor case (R22): a
// component with no own repo edge inherits from its PARENT issue's own
// component, when that parent resolved to exactly one repo.
func TestRepoHierarchyCascadeDepthOne(t *testing.T) {
	repoA := "11111111-1111-4111-8111-111111111111"
	components := []units.Component{
		issueComponentOf("P"),         // idx 0: resolved on its own (PR edge, stubbed via ownRepoByComponent)
		issueComponentOf("C1", "C1b"), // idx 1: pure-issue, unresolved on its own
	}
	ownRepoByComponent := map[int]*uuid.UUID{
		0: mustRepoID(t, repoA),
		1: nil,
	}
	workItems := map[string]chquery.WorkItem{
		"P":   {WorkItemID: "P"},
		"C1":  {WorkItemID: "C1", ParentID: "P"},
		"C1b": {WorkItemID: "C1b"},
	}
	issueComponent := buildIssueComponentIndex(components)

	got := computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, issueComponent)

	cascade, ok := got[1]
	if !ok {
		t.Fatal("component 1 (C1/C1b) got no cascade result; expected a depth-1 ancestor inheritance from P")
	}
	if cascade.Source != AncestorSource("P") {
		t.Errorf("source = %q, want %q", cascade.Source, AncestorSource("P"))
	}
	if cascade.RepoID == nil || cascade.RepoID.String() != mustRepoID(t, repoA).String() {
		t.Errorf("repo = %v, want %s", cascade.RepoID, repoA)
	}
	if _, resolved := got[0]; resolved {
		t.Error("component 0 already resolved on its own edges; it must not appear in the cascade map")
	}
}

// TestRepoHierarchyCascadeDepthTwo pins the multi-hop case: the immediate
// parent (MID) is itself unresolved, so the walk must continue past it to the
// grandparent (GP) rather than stopping at the first unresolved hop.
func TestRepoHierarchyCascadeDepthTwo(t *testing.T) {
	repoB := "22222222-2222-4222-8222-222222222222"
	components := []units.Component{
		issueComponentOf("GP"),          // idx 0: resolved
		issueComponentOf("MID", "MIDb"), // idx 1: unresolved (the intermediate hop)
		issueComponentOf("GC", "GCb"),   // idx 2: unresolved, two hops from GP
	}
	ownRepoByComponent := map[int]*uuid.UUID{
		0: mustRepoID(t, repoB),
		1: nil,
		2: nil,
	}
	workItems := map[string]chquery.WorkItem{
		"GP":   {WorkItemID: "GP"},
		"MID":  {WorkItemID: "MID", ParentID: "GP"},
		"MIDb": {WorkItemID: "MIDb"},
		"GC":   {WorkItemID: "GC", ParentID: "MID"},
		"GCb":  {WorkItemID: "GCb"},
	}
	issueComponent := buildIssueComponentIndex(components)

	got := computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, issueComponent)

	// MID's OWN component also cascades -- GP is MID's direct (depth-1)
	// parent, so {MID, MIDb} independently inherits from GP too. That is
	// correct and expected; it is GC's chain (below) that is the actual
	// depth-2 proof, since GC's parent MID has no OWN-edges resolution (the
	// cascade never chains through another component's CASCADED value, only
	// through OWN edges) and the walk must continue past it to GP.
	if midCascade, resolved := got[1]; !resolved || midCascade.Source != AncestorSource("GP") {
		t.Errorf("MID/MIDb = %+v, ok=%v; want a depth-1 ancestor inheritance from GP", midCascade, resolved)
	}
	cascade, ok := got[2]
	if !ok {
		t.Fatal("component 2 (GC/GCb) got no cascade result; expected a depth-2 ancestor inheritance from GP, walking through the unresolved MID hop")
	}
	if cascade.Source != AncestorSource("GP") {
		t.Errorf("source = %q, want %q (nearest RESOLVED ancestor, not the immediate parent MID)", cascade.Source, AncestorSource("GP"))
	}
	if cascade.RepoID == nil || cascade.RepoID.String() != mustRepoID(t, repoB).String() {
		t.Errorf("repo = %v, want %s", cascade.RepoID, repoB)
	}
}

// TestRepoHierarchyCascadeCycleStaysUnassigned pins cycle-safety: X and Y
// name each other as parent. The walk must terminate (via the visited set,
// not merely the depth bound) and produce no resolution, rather than hang or
// fabricate an answer.
func TestRepoHierarchyCascadeCycleStaysUnassigned(t *testing.T) {
	components := []units.Component{
		issueComponentOf("X", "Y"), // idx 0: a two-issue cycle, no PR/commit, no valid ancestor
	}
	ownRepoByComponent := map[int]*uuid.UUID{0: nil}
	workItems := map[string]chquery.WorkItem{
		"X": {WorkItemID: "X", ParentID: "Y"},
		"Y": {WorkItemID: "Y", ParentID: "X"},
	}
	issueComponent := buildIssueComponentIndex(components)

	done := make(chan map[int]repoCascade, 1)
	go func() {
		done <- computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, issueComponent)
	}()
	var got map[int]repoCascade
	select {
	case got = <-done:
	case <-time.After(time.Second):
		t.Fatal("computeRepoHierarchyCascade did not terminate on a parent_id cycle")
	}

	if cascade, ok := got[0]; ok {
		t.Errorf("a pure X<->Y parent cycle produced a cascade result %+v; it must stay unassigned", cascade)
	}
}

// TestRepoHierarchyCascadeAmbiguousAncestorStaysUnassigned pins the conflict
// case: two member issues' ancestor chains resolve to two DIFFERENT repos.
// Neither wins -- the component stays unassigned rather than picking one
// arbitrarily.
func TestRepoHierarchyCascadeAmbiguousAncestorStaysUnassigned(t *testing.T) {
	repoA := "11111111-1111-4111-8111-111111111111"
	repoB := "22222222-2222-4222-8222-222222222222"
	components := []units.Component{
		issueComponentOf("P"),        // idx 0: resolved to repoA
		issueComponentOf("GP"),       // idx 1: resolved to repoB
		issueComponentOf("Q1", "Q2"), // idx 2: unresolved; Q1's ancestor is P (repoA), Q2's is GP (repoB)
	}
	ownRepoByComponent := map[int]*uuid.UUID{
		0: mustRepoID(t, repoA),
		1: mustRepoID(t, repoB),
		2: nil,
	}
	workItems := map[string]chquery.WorkItem{
		"P":  {WorkItemID: "P"},
		"GP": {WorkItemID: "GP"},
		"Q1": {WorkItemID: "Q1", ParentID: "P"},
		"Q2": {WorkItemID: "Q2", ParentID: "GP"},
	}
	issueComponent := buildIssueComponentIndex(components)

	got := computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, issueComponent)

	if cascade, ok := got[2]; ok {
		t.Errorf("ambiguous multi-repo ancestors (repoA via P, repoB via GP) produced a cascade result %+v; it must stay unassigned, not pick one", cascade)
	}
}

// TestRepoHierarchyCascadeUnanimousChildren pins the fallback path: a
// component with no resolving ancestor of its own inherits from its
// children instead, when every child that resolves agrees on one repo.
func TestRepoHierarchyCascadeUnanimousChildren(t *testing.T) {
	repoA := "11111111-1111-4111-8111-111111111111"
	components := []units.Component{
		issueComponentOf("Parent2", "Parent2b"), // idx 0: unresolved, no ancestor either
		issueComponentOf("Kid1"),                // idx 1: resolved to repoA
		issueComponentOf("Kid2"),                // idx 2: resolved to repoA (agrees with Kid1)
	}
	ownRepoByComponent := map[int]*uuid.UUID{
		0: nil,
		1: mustRepoID(t, repoA),
		2: mustRepoID(t, repoA),
	}
	workItems := map[string]chquery.WorkItem{
		"Parent2":  {WorkItemID: "Parent2"},
		"Parent2b": {WorkItemID: "Parent2b"},
		"Kid1":     {WorkItemID: "Kid1", ParentID: "Parent2"},
		"Kid2":     {WorkItemID: "Kid2", ParentID: "Parent2"},
	}
	issueComponent := buildIssueComponentIndex(components)

	got := computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, issueComponent)

	cascade, ok := got[0]
	if !ok {
		t.Fatal("Parent2/Parent2b got no cascade result; expected unanimous-children inheritance from Kid1 and Kid2")
	}
	if cascade.Source != RepoSourceChildren {
		t.Errorf("source = %q, want %q", cascade.Source, RepoSourceChildren)
	}
	if cascade.RepoID == nil || cascade.RepoID.String() != mustRepoID(t, repoA).String() {
		t.Errorf("repo = %v, want %s", cascade.RepoID, repoA)
	}
}

// TestRepoHierarchyCascadeDisagreeingChildrenStayUnassigned is the negative
// counterpart of the unanimous case: children that resolve to DIFFERENT
// repos must not produce a cascade result.
func TestRepoHierarchyCascadeDisagreeingChildrenStayUnassigned(t *testing.T) {
	repoA := "11111111-1111-4111-8111-111111111111"
	repoB := "22222222-2222-4222-8222-222222222222"
	components := []units.Component{
		issueComponentOf("Parent3"),
		issueComponentOf("KidA"),
		issueComponentOf("KidB"),
	}
	ownRepoByComponent := map[int]*uuid.UUID{
		0: nil,
		1: mustRepoID(t, repoA),
		2: mustRepoID(t, repoB),
	}
	workItems := map[string]chquery.WorkItem{
		"Parent3": {WorkItemID: "Parent3"},
		"KidA":    {WorkItemID: "KidA", ParentID: "Parent3"},
		"KidB":    {WorkItemID: "KidB", ParentID: "Parent3"},
	}
	issueComponent := buildIssueComponentIndex(components)

	got := computeRepoHierarchyCascade(components, ownRepoByComponent, workItems, issueComponent)

	if cascade, ok := got[0]; ok {
		t.Errorf("KidA (repoA) and KidB (repoB) disagree; Parent3 must stay unassigned, got %+v", cascade)
	}
}
