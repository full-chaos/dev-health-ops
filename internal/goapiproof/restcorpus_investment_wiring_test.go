package goapiproof

import (
	"reflect"
	"testing"
)

// TestInvestmentDeclarationsCarryShapes pins the actual registered
// corpus declarations for the rest of the investment family (GET/POST
// /api/v1/investment, /investment/sunburst, /investment/flow,
// /investment/flow/repo-team) -- not a hand-built Options -- so a future
// edit that silently drops a shape field, reorders an inherited slice,
// or restores a blanket path over the deliberately-dropped chosen_mode/
// label/description fails here rather than only being noticed by a live
// prove run. Mirrors TestSankeyAndHeatmapDedupDeclarationsCarryShapes'
// own pattern (restcorpus_sankeyheatmap_wiring_test.go): entries are
// distinguished by WHICH shape they carry, never by their ticket string,
// so this test does not need to hardcode one.
func TestInvestmentDeclarationsCarryShapes(t *testing.T) {
	// investmentBaselineDefects: five entries, one DictKeyDirectionShape
	// or ScalarDirectionShape each. Order matters for the assertions
	// below.
	if len(investmentBaselineDefects) != 5 {
		t.Fatalf("investmentBaselineDefects has %d entries, want 5", len(investmentBaselineDefects))
	}
	wantDictPaths := []string{
		"data.theme_distribution",
		"data.subcategory_distribution",
		"data.evidence_quality_distribution",
		"data.evidence_quality_stats.band_counts",
	}
	for i, want := range wantDictPaths {
		shape := investmentBaselineDefects[i].DictKeyDirectionShape
		if shape == nil {
			t.Fatalf("investmentBaselineDefects[%d] carries no DictKeyDirectionShape", i)
		}
		if shape.DictPath != want {
			t.Fatalf("investmentBaselineDefects[%d].DictKeyDirectionShape.DictPath = %q, want %q", i, shape.DictPath, want)
		}
	}
	totalShape := investmentBaselineDefects[4].ScalarDirectionShape
	if totalShape == nil {
		t.Fatal("investmentBaselineDefects[4] carries no ScalarDirectionShape")
	}
	if totalShape.Path != "data.evidence_quality_stats.total" || !totalShape.BaselineMustBeGreater {
		t.Fatalf("investmentBaselineDefects[4].ScalarDirectionShape = %+v, want Path=data.evidence_quality_stats.total, BaselineMustBeGreater=true", totalShape)
	}
	firstTicket := investmentBaselineDefects[0].Ticket
	for i, defect := range investmentBaselineDefects {
		if defect.Ticket != firstTicket {
			t.Fatalf("investmentBaselineDefects[%d].Ticket = %q, want the SAME ticket every other entry in this table carries (%q) -- these are all facets of the one supersession-exclusion mechanism", i, defect.Ticket, firstTicket)
		}
	}

	// investmentParity composes investmentBaselineDefects unchanged.
	if len(investmentParity.BaselineDefects) != len(investmentBaselineDefects) {
		t.Fatalf("investmentParity.BaselineDefects has %d entries, want %d (inherited from investmentBaselineDefects)", len(investmentParity.BaselineDefects), len(investmentBaselineDefects))
	}

	// investmentSunburstBaselineDefects: the five inherited
	// investmentBaselineDefects entries, PLUS its own repos-join-fan-out
	// entry with a KeyedDirectionShape and Paths narrowed to data.value
	// alone (data.scope dropped as an unreachable leaf once rows are
	// paired by a key that already includes it).
	if len(investmentSunburstBaselineDefects) != len(investmentBaselineDefects)+1 {
		t.Fatalf("investmentSunburstBaselineDefects has %d entries, want %d (inherited) + 1 (its own fan-out entry)", len(investmentSunburstBaselineDefects), len(investmentBaselineDefects))
	}
	sunburstDefect := investmentSunburstBaselineDefects[len(investmentSunburstBaselineDefects)-1]
	if sunburstDefect.Ticket == firstTicket {
		t.Fatalf("investmentSunburstBaselineDefects' own last entry shares investmentBaselineDefects' ticket (%q) -- it should carry its OWN, different mechanism's ticket", firstTicket)
	}
	if sunburstDefect.KeyedDirectionShape == nil {
		t.Fatal("investmentSunburstBaselineDefects' own last entry carries no KeyedDirectionShape")
	}
	if len(sunburstDefect.Paths) != 1 || sunburstDefect.Paths[0] != "data.value" {
		t.Fatalf("investmentSunburstBaselineDefects' own last entry Paths = %v, want exactly [data.value]", sunburstDefect.Paths)
	}
	if len(investmentSunburstOrderInsensitiveLists) != 1 || investmentSunburstOrderInsensitiveLists[0].Path != "data" {
		t.Fatalf("investmentSunburstOrderInsensitiveLists = %+v, want one entry for data", investmentSunburstOrderInsensitiveLists)
	}

	// investmentFlowRepoDedupParity: ordering declarations plus nine
	// BaselineDefects (a fan-out entry, a conservation entry, and seven
	// direction entries sharing one ticket), and NO entry anywhere citing
	// chosen_mode/label/description.
	if len(investmentFlowRepoDedupParity.OrderInsensitiveLists) != 2 {
		t.Fatalf("investmentFlowRepoDedupParity.OrderInsensitiveLists = %+v, want 2 entries (data.nodes, data.links)", investmentFlowRepoDedupParity.OrderInsensitiveLists)
	}
	if len(investmentFlowRepoDedupParity.BaselineDefects) != 9 {
		t.Fatalf("investmentFlowRepoDedupParity has %d BaselineDefects, want 9", len(investmentFlowRepoDedupParity.BaselineDefects))
	}
	fanout := investmentFlowRepoDedupParity.BaselineDefects[0]
	if fanout.SankeyRepoFanoutShape == nil {
		t.Fatalf("investmentFlowRepoDedupParity[0] = %+v, want a SankeyRepoFanoutShape", fanout)
	}
	if fanout.SankeyRepoFanoutShape.NodeValuePath != "data.nodes.value" {
		t.Fatalf("investmentFlowRepoDedupParity's fan-out entry NodeValuePath = %q, want data.nodes.value (this route populates real node values, unlike plain sankey)", fanout.SankeyRepoFanoutShape.NodeValuePath)
	}
	conserve := investmentFlowRepoDedupParity.BaselineDefects[1]
	if conserve.ConservationShape == nil {
		t.Fatalf("investmentFlowRepoDedupParity[1] = %+v, want a ConservationShape", conserve)
	}

	wantKeyedPaths := []string{"data.links", "data.nodes"}
	var supersessionTicket string
	for i, want := range wantKeyedPaths {
		defect := investmentFlowRepoDedupParity.BaselineDefects[2+i]
		if defect.KeyedDirectionShape == nil {
			t.Fatalf("investmentFlowRepoDedupParity[%d] = %+v, want a KeyedDirectionShape", 2+i, defect)
		}
		if defect.KeyedDirectionShape.ListPath != want {
			t.Fatalf("investmentFlowRepoDedupParity[%d].KeyedDirectionShape.ListPath = %q, want %q", 2+i, defect.KeyedDirectionShape.ListPath, want)
		}
		if i == 0 {
			supersessionTicket = defect.Ticket
		} else if defect.Ticket != supersessionTicket {
			t.Fatalf("investmentFlowRepoDedupParity[%d].Ticket = %q, want the same supersession-exclusion ticket as its sibling data.links entry (%q)", 2+i, defect.Ticket, supersessionTicket)
		}
	}
	if supersessionTicket == fanout.Ticket || supersessionTicket == conserve.Ticket {
		t.Fatalf("the supersession-exclusion entries' ticket (%q) must differ from the fan-out (%q) and conservation (%q) entries' -- three different mechanisms, three different tickets", supersessionTicket, fanout.Ticket, conserve.Ticket)
	}

	wantScalar := []struct {
		path    string
		greater bool
	}{
		{"data.team_coverage", false},
		{"data.repo_coverage", false},
		{"data.distinct_team_targets", true},
		{"data.distinct_repo_targets", true},
	}
	for i, want := range wantScalar {
		defect := investmentFlowRepoDedupParity.BaselineDefects[4+i]
		shape := defect.ScalarDirectionShape
		if shape == nil {
			t.Fatalf("investmentFlowRepoDedupParity[%d] = %+v, want a ScalarDirectionShape", 4+i, defect)
		}
		if defect.Ticket != supersessionTicket {
			t.Fatalf("investmentFlowRepoDedupParity[%d].Ticket = %q, want the same supersession-exclusion ticket (%q)", 4+i, defect.Ticket, supersessionTicket)
		}
		if shape.Path != want.path || shape.BaselineMustBeGreater != want.greater {
			t.Fatalf("investmentFlowRepoDedupParity[%d].ScalarDirectionShape = %+v, want Path=%s BaselineMustBeGreater=%v", 4+i, shape, want.path, want.greater)
		}
		if len(shape.ContestedPaths) != 0 {
			t.Fatalf("investmentFlowRepoDedupParity[%d].ScalarDirectionShape.ContestedPaths = %v, want none -- no sibling citation shares this path on this route", 4+i, shape.ContestedPaths)
		}
	}

	unassigned := investmentFlowRepoDedupParity.BaselineDefects[8]
	if unassigned.DictKeyDirectionShape == nil {
		t.Fatalf("investmentFlowRepoDedupParity[8] = %+v, want a DictKeyDirectionShape", unassigned)
	}
	if unassigned.Ticket != supersessionTicket {
		t.Fatalf("investmentFlowRepoDedupParity[8].Ticket = %q, want the same supersession-exclusion ticket (%q)", unassigned.Ticket, supersessionTicket)
	}
	if unassigned.DictKeyDirectionShape.DictPath != "data.unassigned_reasons" {
		t.Fatalf("investmentFlowRepoDedupParity[8].DictKeyDirectionShape.DictPath = %q, want data.unassigned_reasons", unassigned.DictKeyDirectionShape.DictPath)
	}

	for i, defect := range investmentFlowRepoDedupParity.BaselineDefects {
		for _, path := range defect.Paths {
			if path == "data.chosen_mode" || path == "data.label" || path == "data.description" {
				t.Fatalf("investmentFlowRepoDedupParity[%d] cites %q -- chosen_mode/label/description must stay uncovered (categorical threshold flip, no honest shape)", i, path)
			}
		}
	}
}

// TestInvestmentFlowSplitCarriesParentDeclarations is the
// split-integrity pin: investmentFlowDynamicParity/ModeParity/
// RepoTeamParity are each derived from investmentFlowRepoDedupParity by
// VALUE (BaselineDefects/OrderInsensitiveLists copied at package-init
// time, per-scenario numeric-leaf declarations added on top), exactly
// the shape TestSankeyAndHeatmapDedupDeclarationsCarryShapes already
// pins for heatmap's own four-way split. The risk a length-only check
// would miss is not a conflict -- it is a declaration silently NOT
// carried over (a dedup shape or citation present on the parent, absent
// from one child): nothing would fail elsewhere, a finding would simply
// stop being covered, surfacing later as a new uncovered case on a live
// prove run with no visible cause here. reflect.DeepEqual on the full
// slice, not a length comparison, is what actually catches that.
func TestInvestmentFlowSplitCarriesParentDeclarations(t *testing.T) {
	for name, opts := range map[string]Options{
		"investmentFlowDynamicParity":  investmentFlowDynamicParity,
		"investmentFlowModeParity":     investmentFlowModeParity,
		"investmentFlowRepoTeamParity": investmentFlowRepoTeamParity,
	} {
		if !opts.NumericLeavesDeclared {
			t.Fatalf("%s.NumericLeavesDeclared = false, want true", name)
		}
		if !reflect.DeepEqual(opts.OrderInsensitiveLists, investmentFlowRepoDedupParity.OrderInsensitiveLists) {
			t.Fatalf("%s.OrderInsensitiveLists = %+v, want the same as investmentFlowRepoDedupParity's own %+v", name, opts.OrderInsensitiveLists, investmentFlowRepoDedupParity.OrderInsensitiveLists)
		}
		if !reflect.DeepEqual(opts.BaselineDefects, investmentFlowRepoDedupParity.BaselineDefects) {
			t.Fatalf("%s.BaselineDefects = %+v, want the same as investmentFlowRepoDedupParity's own %+v -- a length-only check would miss a content change", name, opts.BaselineDefects, investmentFlowRepoDedupParity.BaselineDefects)
		}
	}

	// Each split's own numeric-leaf declarations, present where that
	// scenario group actually reaches the leaf.
	for _, path := range []string{"data.links.value", "data.nodes.value", "data.team_coverage", "data.repo_coverage"} {
		if _, ok := investmentFlowDynamicParity.FloatTierB[path]; !ok {
			t.Fatalf("investmentFlowDynamicParity.FloatTierB missing %q", path)
		}
		if _, ok := investmentFlowModeParity.FloatTierB[path]; !ok {
			t.Fatalf("investmentFlowModeParity.FloatTierB missing %q", path)
		}
	}
	for _, path := range []string{"data.distinct_team_targets", "data.distinct_repo_targets"} {
		if _, ok := investmentFlowDynamicParity.IntegerLeaves[path]; !ok {
			t.Fatalf("investmentFlowDynamicParity.IntegerLeaves missing %q", path)
		}
		if _, ok := investmentFlowModeParity.IntegerLeaves[path]; !ok {
			t.Fatalf("investmentFlowModeParity.IntegerLeaves missing %q", path)
		}
	}
	// investmentFlowModeParity ALONE reaches coverage/unassigned_reasons/
	// top_n_repos (BuildFlowResponse's flow_mode branch sets them; the
	// dynamic branch and BuildRepoTeamFlowResponse never do -- see this
	// section's own doc comment in restcorpus.go).
	if _, ok := investmentFlowModeParity.FloatTierB["data.coverage"]; !ok {
		t.Fatal("investmentFlowModeParity.FloatTierB missing data.coverage")
	}
	if _, ok := investmentFlowModeParity.IntegerLeaves["data.unassigned_reasons"]; !ok {
		t.Fatal("investmentFlowModeParity.IntegerLeaves missing data.unassigned_reasons")
	}
	if _, ok := investmentFlowModeParity.IntegerLeaves["data.top_n_repos"]; !ok {
		t.Fatal("investmentFlowModeParity.IntegerLeaves missing data.top_n_repos")
	}
	for _, path := range []string{"data.coverage", "data.team_coverage", "data.repo_coverage"} {
		if _, ok := investmentFlowDynamicParity.FloatTierB["data.coverage"]; ok && path == "data.coverage" {
			t.Fatal("investmentFlowDynamicParity must NOT declare data.coverage -- the dynamic branch never sets it, so a declaration here would always read as unused/undeclared-wrongly for that scenario group")
		}
	}

	// investmentFlowRepoTeamParity reaches ONLY links/nodes -- asserting
	// the negative half explicitly, since BuildRepoTeamFlowResponse
	// leaves every scalar/map field nil and a declaration here would
	// always read as unused for this scenario group (this section's own
	// doc comment in restcorpus.go).
	if len(investmentFlowRepoTeamParity.FloatTierB) != 2 {
		t.Fatalf("investmentFlowRepoTeamParity.FloatTierB = %+v, want exactly data.links.value/data.nodes.value", investmentFlowRepoTeamParity.FloatTierB)
	}
	if len(investmentFlowRepoTeamParity.IntegerLeaves) != 0 {
		t.Fatalf("investmentFlowRepoTeamParity.IntegerLeaves = %+v, want none", investmentFlowRepoTeamParity.IntegerLeaves)
	}
}
