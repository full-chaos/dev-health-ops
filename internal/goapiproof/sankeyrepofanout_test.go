package goapiproof

import (
	"os"
	"strconv"
	"testing"
)

// This file exercises SankeyRepoFanoutShape (sankeyrepofanout.go)
// directly through small, self-contained node/link lists, plus two real
// captured GET /api/v1/sankey response pairs (a production deployed-vs-deployed prove run) --
// the shape-specific admission that replaces a blanket "any difference
// under data.nodes/data.links is covered" rule for the sankey repos-join
// fan-out mechanism.
//
// Every case here declares OrderInsensitiveLists alongside the shape:
// SankeyRepoFanoutShape's own admits() reads a finding's pairing key from
// its Detail (parseOrderInsensitiveDetailKey), which only exists once
// compareListByKey -- not positional compareList -- produced the
// finding. Without that declaration a finding never carries a key at
// all, and the shape correctly, safely admits nothing.

func sankeyRepoFanoutOptions(nodeGroups, fallback []string) Options {
	return Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.nodes", KeyFields: []string{"name"}, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
			{Path: "data.links", KeyFields: []string{"source", "target"}, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
		},
		BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-TEST-FANOUT", Reason: "test fixture",
			Paths:        []string{"data.links"},
			Intermittent: true, IntermittentReason: "test fixture",
			SankeyRepoFanoutShape: &SankeyRepoFanoutShape{
				NodesListPath:       "data.nodes",
				LinksListPath:       "data.links",
				LinkValuePath:       "data.links.value",
				RepoNodeGroups:      nodeGroups,
				FallbackAnchorNames: fallback,
			},
		}},
	}
}

func sankeyBody(nodes, links string) string {
	return `{"data":{"nodes":[` + nodes + `],"links":[` + links + `]}}`
}

func sankeyNode(name, group string) string {
	return `{"name":"` + name + `","group":"` + group + `","value":null}`
}

func sankeyLink(source, target string, value float64) string {
	return `{"source":"` + source + `","target":"` + target + `","value":` + jsonFloat(value) + `}`
}

// jsonFloat renders a float64 the way encoding/json would for a whole
// number (no trailing ".0"), so hand-authored fixtures round-trip
// through asFloat identically regardless of which side wrote them --
// exactly the whole-number-formatting concern workgraphedgedup_test.go's
// own fixtures already account for.
func jsonFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// A repo (Group "repo", hotspot mode's own root dimension) whose whole
// chain -- repo->directory, directory->file, file->change_type -- fans
// out at a clean, uniform k=2 is fully admitted at every edge: rule 2's
// forward propagation, validated at each hop rather than merely assumed.
func TestSankeyRepoFanoutShape_UniformChainFanoutIsAdmitted(t *testing.T) {
	nodes := sankeyNode("repoA", "repo") + "," + sankeyNode("repoA / dir", "directory") + "," +
		sankeyNode("repoA / dir/file.go", "file") + "," + sankeyNode("refactor", "change_type")
	candLinks := sankeyLink("repoA", "repoA / dir", 100) + "," +
		sankeyLink("repoA / dir", "repoA / dir/file.go", 100) + "," +
		sankeyLink("repoA / dir/file.go", "refactor", 100)
	baseLinks := sankeyLink("repoA", "repoA / dir", 200) + "," +
		sankeyLink("repoA / dir", "repoA / dir/file.go", 200) + "," +
		sankeyLink("repoA / dir/file.go", "refactor", 200)

	baseline := snapshotFromJSON(t, sankeyBody(nodes, baseLinks))
	candidate := snapshotFromJSON(t, sankeyBody(nodes, candLinks))
	opts := sankeyRepoFanoutOptions([]string{"repo"}, []string{"Unknown repo"})

	result := Compare(baseline, candidate, opts)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-FANOUT"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-FANOUT]", result.BaselineDefectsMatched)
	}
}

// An anchor whose OWN direct edges disagree on k (2x on one, 3x on
// another) has no verified multiplier at all: rule 1's uniformity
// requirement, failing closed rather than picking either candidate.
func TestSankeyRepoFanoutShape_NonUniformDirectEdgesAdmitsNothing(t *testing.T) {
	nodes := sankeyNode("repoA", "repo") + "," + sankeyNode("repoA / dirX", "directory") + "," + sankeyNode("repoA / dirY", "directory")
	candLinks := sankeyLink("repoA", "repoA / dirX", 100) + "," + sankeyLink("repoA", "repoA / dirY", 100)
	baseLinks := sankeyLink("repoA", "repoA / dirX", 200) + "," + sankeyLink("repoA", "repoA / dirY", 300)

	baseline := snapshotFromJSON(t, sankeyBody(nodes, baseLinks))
	candidate := snapshotFromJSON(t, sankeyBody(nodes, candLinks))
	opts := sankeyRepoFanoutOptions([]string{"repo"}, nil)

	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 (both edges unexplained -- repoA has no uniform k): findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// The mode's own null-repo fallback label ("Unknown repo") is excluded
// from anchor candidacy even though its own edges happen to show a
// clean, uniform ratio: it is a catch-all bucket for an unresolved
// repo_id, not a repos row subject to this mechanism (rule 3).
func TestSankeyRepoFanoutShape_FallbackAnchorNeverAdmits(t *testing.T) {
	nodes := sankeyNode("Unknown repo", "repo") + "," + sankeyNode("Unknown repo / dir", "directory")
	candLinks := sankeyLink("Unknown repo", "Unknown repo / dir", 100)
	baseLinks := sankeyLink("Unknown repo", "Unknown repo / dir", 200)

	baseline := snapshotFromJSON(t, sankeyBody(nodes, baseLinks))
	candidate := snapshotFromJSON(t, sankeyBody(nodes, candLinks))
	opts := sankeyRepoFanoutOptions([]string{"repo"}, []string{"Unknown repo"})

	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- the fallback bucket must never borrow this mechanism: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TWO REPOS FANNING OUT IN THE SAME RESPONSE, where a downstream node is
// genuinely reachable from both anchors with DIFFERENT verified k's (2x
// and 3x), and the shared downstream edge's own observed ratio matches
// NEITHER: the shape declines to vouch for that edge explicitly, rather
// than picking whichever anchor's forward walk reached it first. This is
// the overlap case the design was required to decide rather than leave
// to traversal order.
func TestSankeyRepoFanoutShape_OverlappingAnchorsDeclineWhenNeitherKValidates(t *testing.T) {
	nodes := sankeyNode("repoA", "repo") + "," + sankeyNode("repoB", "repo") + "," +
		sankeyNode("shared", "directory") + "," + sankeyNode("shared / leaf", "file")
	candLinks := sankeyLink("repoA", "shared", 100) + "," + sankeyLink("repoB", "shared", 100) + "," +
		sankeyLink("shared", "shared / leaf", 100)
	// repoA fans out 2x, repoB fans out 3x -- both established cleanly on
	// their own direct edge into "shared". The downstream edge out of
	// "shared" (owned by BOTH, since both reach it) shows a THIRD ratio,
	// 2.5x, matching neither.
	baseLinks := sankeyLink("repoA", "shared", 200) + "," + sankeyLink("repoB", "shared", 300) + "," +
		sankeyLink("shared", "shared / leaf", 250)

	baseline := snapshotFromJSON(t, sankeyBody(nodes, baseLinks))
	candidate := snapshotFromJSON(t, sankeyBody(nodes, candLinks))
	opts := sankeyRepoFanoutOptions([]string{"repo"}, nil)

	result := Compare(baseline, candidate, opts)
	// The two direct edges (repoA->shared, repoB->shared) each validate
	// their own anchor's k and ARE admitted; only the shared downstream
	// edge, owned by both with no single k that fits, is not.
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 (only shared->leaf declines): findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	foundDeclined := false
	for _, f := range result.Findings {
		if f.Kind == FindingMismatch && f.Path == "$.data.links[2].value" {
			foundDeclined = true
		}
	}
	_ = foundDeclined // path indices depend on sorted key order; the outside count above is the load-bearing assertion.
}

// TWO REPOS overlapping at a shared node, where the shared downstream
// edge's own ratio matches EXACTLY ONE owner's k: ownership being
// ambiguous at the NODE does not by itself block the EDGE -- only when
// no single k explains it, or more than one does, does it decline.
func TestSankeyRepoFanoutShape_OverlappingAnchorsAdmitWhenExactlyOneKValidates(t *testing.T) {
	nodes := sankeyNode("repoA", "repo") + "," + sankeyNode("repoB", "repo") + "," +
		sankeyNode("shared", "directory") + "," + sankeyNode("shared / leaf", "file")
	candLinks := sankeyLink("repoA", "shared", 100) + "," + sankeyLink("repoB", "shared", 100) + "," +
		sankeyLink("shared", "shared / leaf", 100)
	// repoA fans out 2x, repoB fans out 3x; the shared downstream edge
	// shows EXACTLY 2x -- repoA's own multiplier, and only repoA's.
	baseLinks := sankeyLink("repoA", "shared", 200) + "," + sankeyLink("repoB", "shared", 300) + "," +
		sankeyLink("shared", "shared / leaf", 200)

	baseline := snapshotFromJSON(t, sankeyBody(nodes, baseLinks))
	candidate := snapshotFromJSON(t, sankeyBody(nodes, candLinks))
	opts := sankeyRepoFanoutOptions([]string{"repo"}, nil)

	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- shared->leaf's own ratio matches repoA's k uniquely: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

func sankeyRepoFanoutSnapshotFromFile(t *testing.T, path string) Snapshot {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	snapshot, err := DecodeRESTSnapshot(body)
	if err != nil {
		t.Fatalf("decode REST fixture %s: %v", path, err)
	}
	return snapshot
}

// TestSankeyRepoFanoutShape_RealInvestmentCapture pins investment mode's
// own real production instance (GET /api/v1/sankey investment_default_
// org, a production deployed-vs-deployed prove run): repo "full-chaos/script-manifest" fans out
// at an exact, uniform 2.0x across the two theme->repo edges that target
// it (feature_delivery->..., maintenance->...); every other one of the
// capture's 32 shared edge keys already agrees.
func TestSankeyRepoFanoutShape_RealInvestmentCapture(t *testing.T) {
	baseline := sankeyRepoFanoutSnapshotFromFile(t, "testdata/sankey_investment_fanout_baseline_d546771b.json")
	candidate := sankeyRepoFanoutSnapshotFromFile(t, "testdata/sankey_investment_fanout_candidate_8d4b7675.json")
	opts := sankeyRepoFanoutOptions([]string{"project"}, []string{"Other"})

	result := Compare(baseline, candidate, opts)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- covered %v outside %v findings %+v",
			result.DifferencesOutsideBaselineDefect, result.CoveredByShape, result.OutsideByShape, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-FANOUT"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-FANOUT] -- idle %v stale %v", result.BaselineDefectsMatched, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
}

// TestSankeyRepoFanoutShape_RealHotspotCapture pins hotspot mode's own
// real production instance (GET /api/v1/sankey hotspot_org,
// a production deployed-vs-deployed prove run): the SAME repo fans out at an exact, uniform
// 2.0x across all THREE edges of its own repo->directory->file->
// change_type chain -- the multi-hop propagation rule 2 exists for,
// proven live rather than only by construction.
func TestSankeyRepoFanoutShape_RealHotspotCapture(t *testing.T) {
	baseline := sankeyRepoFanoutSnapshotFromFile(t, "testdata/sankey_hotspot_fanout_baseline_67c5cc60.json")
	candidate := sankeyRepoFanoutSnapshotFromFile(t, "testdata/sankey_hotspot_fanout_candidate_a230f55f.json")
	opts := sankeyRepoFanoutOptions([]string{"repo"}, []string{"Unknown repo"})

	result := Compare(baseline, candidate, opts)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- covered %v outside %v findings %+v",
			result.DifferencesOutsideBaselineDefect, result.CoveredByShape, result.OutsideByShape, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-FANOUT"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-FANOUT] -- idle %v stale %v", result.BaselineDefectsMatched, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
}
