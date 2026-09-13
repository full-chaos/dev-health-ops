package goapiproof

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// This file exercises RepoFanoutShape (repofanout.go) directly through
// the registered investmentFull CHAOS-4773 declaration: the SHAPE-SPECIFIC
// admission that replaced its blanket "any difference under
// sankey.nodes/.edges/.coverage is covered" rule. Every baseline here is
// built from the committed job5 CANDIDATE capture
// (testdata/investmentfull_candidate_job5_7ae7cb5b.json) so every field
// not deliberately mutated stays byte-identical between the two legs --
// unlike investmentfull_repojoin_fanout_test.go's own fixture (a real
// captured baseline with its own pre-existing float noise), which is not
// suitable for asserting an EXACT outside-count.

// loadSankeyCandidateCopy reads the committed candidate capture twice --
// once as the untouched candidate, once as a plain map the caller mutates
// into a synthetic baseline -- so the two never alias each other's data.
func loadSankeyCandidateCopy(t *testing.T) (candidateJSON string, baselineBody map[string]any) {
	t.Helper()
	const path = "testdata/investmentfull_candidate_job5_7ae7cb5b.json"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	candidateJSON = string(raw)
	if err := json.Unmarshal(raw, &baselineBody); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return candidateJSON, baselineBody
}

func sankeyOf(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	sankey, ok := body["data"].(map[string]any)["analytics"].(map[string]any)["sankey"].(map[string]any)
	if !ok {
		t.Fatal("body carries no data.analytics.sankey")
	}
	return sankey
}

// applyRepoFanout mutates a baseline's sankey subtree to look exactly
// like what CHAOS-4773's real mechanism produces for the given per-repo
// multipliers: each named REPO node and every THEME->REPO edge into it
// scale by its own k; each affected THEME node gains the SAME per-repo
// inflation its own edges now carry; and the aggregate total that
// inflation implies is dumped onto one arbitrarily-chosen TEAM node and
// one TEAM->THEME edge (the shape's own TEAM/THEME rule 4 only checks the
// AGGREGATE total, never a specific node, so which one carries it is
// immaterial to the test). coverage is left untouched -- callers that
// care about it set coverage explicitly afterward.
func applyRepoFanout(t *testing.T, body map[string]any, repoMultiplier map[string]int) {
	t.Helper()
	sankey := sankeyOf(t, body)
	nodes := sankey["nodes"].([]any)
	edges := sankey["edges"].([]any)

	repoValue := map[string]float64{}
	for _, n := range nodes {
		node := n.(map[string]any)
		id := node["id"].(string)
		if strings.HasPrefix(id, "REPO:") {
			repoValue[id] = node["value"].(float64)
		}
	}
	var totalInflation float64
	for id, k := range repoMultiplier {
		totalInflation += float64(k-1) * repoValue[id]
	}

	for _, n := range nodes {
		node := n.(map[string]any)
		id := node["id"].(string)
		if k, ok := repoMultiplier[id]; ok {
			node["value"] = node["value"].(float64) * float64(k)
		}
	}

	themeInflation := map[string]float64{}
	for _, e := range edges {
		edge := e.(map[string]any)
		source, target := edge["source"].(string), edge["target"].(string)
		if !strings.HasPrefix(source, "THEME:") || !strings.HasPrefix(target, "REPO:") {
			continue
		}
		k, ok := repoMultiplier[target]
		if !ok {
			continue
		}
		value := edge["value"].(float64)
		themeInflation[source] += float64(k-1) * value
		edge["value"] = value * float64(k)
	}
	for _, n := range nodes {
		node := n.(map[string]any)
		id := node["id"].(string)
		if delta, ok := themeInflation[id]; ok {
			node["value"] = node["value"].(float64) + delta
		}
	}

	if totalInflation == 0 {
		return
	}
	for _, n := range nodes {
		node := n.(map[string]any)
		if node["id"].(string) == "TEAM:Fullchaos" {
			node["value"] = node["value"].(float64) + totalInflation
			break
		}
	}
	for _, e := range edges {
		edge := e.(map[string]any)
		if edge["source"].(string) == "TEAM:Fullchaos" && edge["target"].(string) == "THEME:feature_delivery" {
			edge["value"] = edge["value"].(float64) + totalInflation
			break
		}
	}
}

// driftEdge scales one named edge's value by factor -- a change the
// repos-join fan-out mechanism cannot itself produce, standing in for an
// unrelated real Go regression on that one edge.
func driftEdge(t *testing.T, body map[string]any, source, target string, factor float64) {
	t.Helper()
	sankey := sankeyOf(t, body)
	for _, e := range sankey["edges"].([]any) {
		edge := e.(map[string]any)
		if edge["source"].(string) == source && edge["target"].(string) == target {
			edge["value"] = edge["value"].(float64) * factor
			return
		}
	}
	t.Fatalf("no edge %s -> %s in the fixture", source, target)
}

func marshalBody(t *testing.T, body map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode mutated baseline: %v", err)
	}
	return string(raw)
}

func compareAsInvestmentFull(t *testing.T, baselineJSON, candidateJSON string) Result {
	t.Helper()
	spec, err := SpecFor("investmentFull")
	if err != nil {
		t.Fatalf("SpecFor(investmentFull): %v", err)
	}
	return Compare(snapshotFromJSON(t, baselineJSON), snapshotFromJSON(t, candidateJSON), spec.Parity)
}

// Per-repo k may differ (2 and 3): each repo's own multiplier explains its
// own node and edges independently, and the TEAM/THEME totals still
// reconcile once against the combined inflation.
func TestRepoFanoutShape_DifferingPerRepoMultipliersAreAdmitted(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	applyRepoFanout(t, baseline, map[string]int{
		"REPO:full-chaos/dev-health-ops": 2,
		"REPO:full-chaos/dev-health-web": 3,
	})

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), candidateJSON)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a declared defect never converts one", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{investmentFullRepoJoinFanoutTicket}) {
		t.Fatalf("matched = %v, want [%s]", result.BaselineDefectsMatched, investmentFullRepoJoinFanoutTicket)
	}
}

// A non-integer ratio on a REPO node is not a fan-out multiple of
// anything -- the join duplicates whole physical rows, never a fraction
// of one -- so it is reported outside the citation, not admitted.
func TestRepoFanoutShape_NonIntegerMultiplierIsNotAdmitted(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	sankey := sankeyOf(t, baseline)
	for _, n := range sankey["nodes"].([]any) {
		node := n.(map[string]any)
		if node["id"].(string) == "REPO:full-chaos/dev-health-ops" {
			node["value"] = node["value"].(float64) * 2.3
			break
		}
	}

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), candidateJSON)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 (the non-integer repo node) -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// A genuine 2x fan-out on one repo is admitted; an UNRELATED +1% drift on
// a different repo's edge is not multiplication by any repo's version
// count and must be reported outside the citation even though it shares
// the same cited path.
func TestRepoFanoutShape_UnrelatedEdgeDriftAlongsideARealFanoutIsNotAdmitted(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	applyRepoFanout(t, baseline, map[string]int{"REPO:full-chaos/dev-health-ops": 2})
	driftEdge(t, baseline, "THEME:quality", "REPO:full-chaos/dev-health-web", 1.01)

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), candidateJSON)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want exactly 1 (the drifted edge) -- the real fan-out must stay admitted alongside it: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	found := false
	for _, f := range result.Findings {
		if f.Kind == FindingMismatch && strings.Contains(f.Detail, `THEME:quality`) && strings.Contains(f.Detail, `dev-health-web`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("the drifted edge itself must be among the findings: %+v", result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{investmentFullRepoJoinFanoutTicket}) {
		t.Fatalf("matched = %v, want [%s] -- the real fan-out is still a live citation", result.BaselineDefectsMatched, investmentFullRepoJoinFanoutTicket)
	}
}

// repoCoverage within the bound a k=2 fan-out implies is admitted.
func TestRepoFanoutShape_CoverageWithinTheMultiplierBoundIsAdmitted(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	applyRepoFanout(t, baseline, map[string]int{"REPO:full-chaos/dev-health-ops": 2})

	sankey := sankeyOf(t, baseline)
	coverage := sankey["coverage"].(map[string]any)
	candidateRepoCoverage := coverage["repoCoverage"].(float64)
	coverage["repoCoverage"] = repoFanoutRepoCoverageCeiling(candidateRepoCoverage, 2)

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), candidateJSON)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- repoCoverage at the k=2 ceiling is exactly what the fan-out implies: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// repoCoverage OUTSIDE the bound a k=2 fan-out implies -- here, above the
// ceiling no observed multiplier can explain -- is not admitted.
func TestRepoFanoutShape_CoverageOutsideTheMultiplierBoundIsNotAdmitted(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	applyRepoFanout(t, baseline, map[string]int{"REPO:full-chaos/dev-health-ops": 2})

	sankey := sankeyOf(t, baseline)
	coverage := sankey["coverage"].(map[string]any)
	coverage["repoCoverage"] = 1.0 + 1e-3 // impossible: a coverage ratio can never exceed 1

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), candidateJSON)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want at least 1 -- an out-of-bound repoCoverage must not be silently admitted: findings %+v", result.Findings)
	}
	outsideRepoCoverage := false
	for _, f := range result.Findings {
		if f.Kind == FindingMismatch && tieredPath(f.Path) == "data.analytics.sankey.coverage.repoCoverage" {
			outsideRepoCoverage = true
		}
	}
	if !outsideRepoCoverage {
		t.Fatalf("repoCoverage itself must be among the mismatch findings: %+v", result.Findings)
	}
}
