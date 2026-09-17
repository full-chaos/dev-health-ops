package goapiproof

import "testing"

// This file exercises SankeyRepoFanoutShape's own OPTIONAL NodeValuePath
// (rule 4, sankeyrepofanout.go): investment/flow and investment/flow/
// repo-team reuse the SAME sankey.Response wire type as GET/POST
// /api/v1/sankey, but UNLIKE that route their own node builders
// (investmentflow/builders.go's nodeRunningTotal/nodePresence) populate a
// real Node.Value, so a genuine repo fan-out there also moves an
// anchor's own node value, not only its incident edges.
//
// NO REAL CAPTURE SHOWS THIS MECHANISM FIRING: every one of the three
// available production deployed-vs-deployed prove runs (_records/
// deploy-71/r78/step73, deploy-70/r77/step72, deploy-69/r76/step71) shows
// investment/flow and investment/flow/repo-team as a clean match on
// every admissible request -- the repos-join fan-out has
// not yet been observed live on these two routes. This rule is proven
// here only by a deliberately injected fixture fault, exactly as
// sankeyInvestmentParity's own argMax-null-skip/ConservationShape entry already
// is (restcorpus.go).

func sankeyNodeVal(name, group string, value float64) string {
	return `{"name":"` + name + `","group":"` + group + `","value":` + jsonFloat(value) + `}`
}

// sankeyRepoFanoutOptionsWithNodeValue is sankeyRepoFanoutOptions' own
// twin with NodeValuePath declared, modelling investment/flow's own
// wiring (Paths naming both data.nodes and data.links, since here --
// unlike plain sankey -- data.nodes DOES carry a numeric leaf).
func sankeyRepoFanoutOptionsWithNodeValue(nodeGroups, fallback []string) Options {
	return Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.nodes", KeyFields: []string{"name"}, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
			{Path: "data.links", KeyFields: []string{"source", "target"}, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
		},
		BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-TEST-FANOUT", Reason: "test fixture",
			Paths:        []string{"data.nodes", "data.links"},
			Intermittent: true, IntermittentReason: "test fixture",
			SankeyRepoFanoutShape: &SankeyRepoFanoutShape{
				NodesListPath:       "data.nodes",
				LinksListPath:       "data.links",
				LinkValuePath:       "data.links.value",
				NodeValuePath:       "data.nodes.value",
				RepoNodeGroups:      nodeGroups,
				FallbackAnchorNames: fallback,
			},
		}},
	}
}

// An anchor's own node value, fanned out by the SAME uniform k its
// direct edges already establish, is admitted alongside the edge --
// rule 4 checked directly against the anchor's own verified k, not
// re-summed through the graph.
func TestSankeyRepoFanoutShape_NodeValueAdmittedWhenMatchesAnchorK(t *testing.T) {
	nodes := sankeyNodeVal("repoA", "repo", 100) + "," + sankeyNodeVal("repoA / dir", "directory", 100)
	candLinks := sankeyLink("repoA", "repoA / dir", 100)
	baseLinks := sankeyLink("repoA", "repoA / dir", 200)
	baseNodes := sankeyNodeVal("repoA", "repo", 200) + "," + sankeyNodeVal("repoA / dir", "directory", 100)

	baseline := snapshotFromJSON(t, sankeyBody(baseNodes, baseLinks))
	candidate := snapshotFromJSON(t, sankeyBody(nodes, candLinks))
	opts := sankeyRepoFanoutOptionsWithNodeValue([]string{"repo"}, nil)

	result := Compare(baseline, candidate, opts)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 (anchor's own node value fans out by the same verified k as its edge): findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-FANOUT"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-FANOUT]", result.BaselineDefectsMatched)
	}
}

// Backward compatibility: when NodeValuePath is left unset (every
// EXISTING caller -- sankeyRepoDedupParity, sankeyInvestmentParity,
// investmentFlowRepoDedupParity's own repos-join-fan-out entry before this
// change), a node value difference is NEVER admitted even when it
// numerically fits the SAME verified k -- rule 4 never runs, so this
// change is a strict addition with no altered behaviour for an unset
// field.
func TestSankeyRepoFanoutShape_NodeValueUnsetLeavesNodeDifferenceUncovered(t *testing.T) {
	nodes := sankeyNodeVal("repoA", "repo", 100) + "," + sankeyNodeVal("repoA / dir", "directory", 100)
	candLinks := sankeyLink("repoA", "repoA / dir", 100)
	baseLinks := sankeyLink("repoA", "repoA / dir", 200)
	baseNodes := sankeyNodeVal("repoA", "repo", 200) + "," + sankeyNodeVal("repoA / dir", "directory", 100)

	baseline := snapshotFromJSON(t, sankeyBody(baseNodes, baseLinks))
	candidate := snapshotFromJSON(t, sankeyBody(nodes, candLinks))
	// sankeyRepoFanoutOptions (no NodeValuePath) -- the pre-existing
	// helper, Paths widened to include data.nodes so the node-value
	// finding is even IN this defect's own citation at all (otherwise it
	// would trivially stay outside for an unrelated reason).
	opts := sankeyRepoFanoutOptions([]string{"repo"}, nil)
	opts.BaselineDefects[0].Paths = []string{"data.nodes", "data.links"}

	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- the node value must stay uncovered with NodeValuePath unset, only the edge is admitted: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// Blind-spot pin: a NON-anchor node (Group not in RepoNodeGroups) whose
// value happens to differ by the SAME integer ratio as a real anchor
// elsewhere in the response is never admitted by rule 4 -- an aggregate
// node's own value needs a separate re-summation/conservation argument
// this shape does not attempt (see the type doc comment's rule 4).
func TestSankeyRepoFanoutShape_NonAnchorNodeValueNeverAdmitted(t *testing.T) {
	nodes := sankeyNodeVal("repoA", "repo", 100) + "," + sankeyNodeVal("teamX", "team", 100)
	candLinks := sankeyLink("repoA", "teamX", 100)
	baseLinks := sankeyLink("repoA", "teamX", 200)
	// teamX's own node value ALSO happens to be exactly 2x -- a
	// coincidence this shape must not treat as proof for a non-anchor.
	baseNodes := sankeyNodeVal("repoA", "repo", 200) + "," + sankeyNodeVal("teamX", "team", 200)

	baseline := snapshotFromJSON(t, sankeyBody(baseNodes, baseLinks))
	candidate := snapshotFromJSON(t, sankeyBody(nodes, candLinks))
	opts := sankeyRepoFanoutOptionsWithNodeValue([]string{"repo"}, nil)

	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- teamX is not an anchor (Group=\"team\" not in RepoNodeGroups) and its own value must stay uncovered even though it numerically matches repoA's k: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}
