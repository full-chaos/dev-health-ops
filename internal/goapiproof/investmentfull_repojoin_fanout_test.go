package goapiproof

import (
	"encoding/json"
	"os"
	"sort"
	"testing"
)

// investmentFull's Python sankey and coverage queries join `repos` without
// FINAL, so a repo row with an unmerged physical version multiplies every
// sankey value and the repo-assigned coverage for units in that repo. The
// Go plane reads `repos FINAL`. The divergence exists only between a repos
// write and the next background merge, so the declaration must be
// intermittent: it covers the fan-out when present and stays quiet when
// the table is merged.
var investmentFullRepoJoinFanoutPaths = []string{
	"data.analytics.sankey.coverage.repoCoverage",
	"data.analytics.sankey.coverage.teamCoverage",
	"data.analytics.sankey.edges.value",
	"data.analytics.sankey.nodes.value",
}

const investmentFullRepoJoinFanoutTicket = "CHAOS-4773"

func TestInvestmentFullDeclaresTheRepoJoinFanoutAsIntermittent(t *testing.T) {
	spec, err := SpecFor("investmentFull")
	if err != nil {
		t.Fatalf("SpecFor(investmentFull): %v", err)
	}
	// investmentFull also declares CHAOS-4547 (the argMax null-transition
	// shift, coverageshift.go / investmentfull_argmax_coverage_shift_shape_test.go)
	// over the same two coverage leaves -- this test only asserts the
	// repos-join fan-out's OWN entry, found by ticket rather than assumed
	// to be the only one declared.
	var defect BaselineDefect
	found := false
	for _, candidate := range spec.Parity.BaselineDefects {
		if candidate.Ticket == investmentFullRepoJoinFanoutTicket {
			defect = candidate
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("investmentFull declares no %s baseline defect: %#v", investmentFullRepoJoinFanoutTicket, spec.Parity.BaselineDefects)
	}
	if !defect.Intermittent {
		t.Fatal("the fan-out exists only while repos holds unmerged versions; without Intermittent every merged-state run refuses as stale")
	}
	if err := validateBaselineDefects(spec.Parity.BaselineDefects); err != nil {
		t.Fatalf("the committed declaration is invalid: %v", err)
	}
	paths := append([]string(nil), defect.Paths...)
	sort.Strings(paths)
	if !equalStrings(paths, investmentFullRepoJoinFanoutPaths) {
		t.Fatalf("paths = %v, want exactly %v", paths, investmentFullRepoJoinFanoutPaths)
	}
}

// Merged state: the committed matching captures still match, and the
// declaration is idle, not stale.
func TestInvestmentFullRepoJoinFanout_MergedStateCapturesMatchWithTheDeclarationIdle(t *testing.T) {
	spec, err := SpecFor("investmentFull")
	if err != nil {
		t.Fatalf("SpecFor(investmentFull): %v", err)
	}
	for _, pair := range [][2]string{
		{"testdata/investmentfull_baseline_job5_773418f7.json", "testdata/investmentfull_candidate_job5_7ae7cb5b.json"},
		{"testdata/investmentfull_baseline_job5_a5e7c5d3.json", "testdata/investmentfull_candidate_job5_fe29a0ab.json"},
	} {
		result := Compare(snapshotFromFile(t, pair[0]), snapshotFromFile(t, pair[1]), spec.Parity)
		if !result.IsMatch() {
			t.Fatalf("%s: terminal = %q, findings %+v", pair[0], result.TerminalState, result.Findings)
		}
		if len(result.StaleBaselineDefects) != 0 {
			t.Fatalf("%s: stale = %v -- a merged-state run would refuse", pair[0], result.StaleBaselineDefects)
		}
		// Both declared defects cover no difference in a merged-state
		// capture -- CHAOS-4547 (the argMax null-transition shift) is
		// idle here for the same reason CHAOS-4773 is: nothing differs
		// under either's cited paths.
		if !equalStrings(result.IdleIntermittentBaselineDefects, []string{"CHAOS-4547", investmentFullRepoJoinFanoutTicket}) {
			t.Fatalf("%s: idle = %v, want [CHAOS-4547 %s]", pair[0], result.IdleIntermittentBaselineDefects, investmentFullRepoJoinFanoutTicket)
		}
	}
}

// Fan-out state: a baseline whose sankey values are multiplied and whose
// coverage ratios moved is a mismatch, every difference is covered, and
// nothing is outside the citation.
func TestInvestmentFullRepoJoinFanout_FannedOutBaselineIsACoveredMismatch(t *testing.T) {
	spec, err := SpecFor("investmentFull")
	if err != nil {
		t.Fatalf("SpecFor(investmentFull): %v", err)
	}
	const baselinePath = "testdata/investmentfull_baseline_job5_773418f7.json"
	const candidatePath = "testdata/investmentfull_candidate_job5_7ae7cb5b.json"

	raw, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatalf("read %s: %v", baselinePath, err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode %s: %v", baselinePath, err)
	}
	sankey := body["data"].(map[string]any)["analytics"].(map[string]any)["sankey"].(map[string]any)
	for _, list := range []string{"nodes", "edges"} {
		for _, element := range sankey[list].([]any) {
			item := element.(map[string]any)
			if value, ok := item["value"].(float64); ok && value != 0 {
				item["value"] = value * 2
			}
		}
	}
	candidateRaw, err := os.ReadFile(candidatePath)
	if err != nil {
		t.Fatalf("read %s: %v", candidatePath, err)
	}
	var candidateBody map[string]any
	if err := json.Unmarshal(candidateRaw, &candidateBody); err != nil {
		t.Fatalf("decode %s: %v", candidatePath, err)
	}
	candidateCoverage := candidateBody["data"].(map[string]any)["analytics"].(map[string]any)["sankey"].(map[string]any)["coverage"].(map[string]any)
	candidateRepoCoverage := candidateCoverage["repoCoverage"].(float64)
	candidateTeamCoverage := candidateCoverage["teamCoverage"].(float64)

	coverage := sankey["coverage"].(map[string]any)
	// Every repo doubles uniformly here (k=2 on every nonzero node
	// above): repoCoverage's unassigned share is PROVABLY immune to the
	// repos join (a repo_id IS NULL row can never join `repos`), so
	// under a uniform fan-out it rises all the way to
	// repoFanoutRepoCoverageCeiling's k=2 ceiling. teamCoverage shares
	// the SAME sum(repoEffortCol) denominator as repoCoverage
	// (sankeycoverage.go: repoTotalExpr = totalExpr) and every repo is
	// affected here, so its numerator and denominator double together
	// and the ratio is UNCHANGED -- left as the candidate's own value.
	coverage["repoCoverage"] = repoFanoutRepoCoverageCeiling(candidateRepoCoverage, 2)
	coverage["teamCoverage"] = candidateTeamCoverage
	fanned, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode fanned-out baseline: %v", err)
	}

	result := Compare(snapshotFromJSON(t, string(fanned)), snapshotFromFile(t, candidatePath), spec.Parity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a declared defect never converts one", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{investmentFullRepoJoinFanoutTicket}) {
		t.Fatalf("matched = %v, want [%s]", result.BaselineDefectsMatched, investmentFullRepoJoinFanoutTicket)
	}
	if len(result.StaleBaselineDefects) != 0 {
		t.Fatalf("the fan-out entry matched, so it is not stale: %v", result.StaleBaselineDefects)
	}
	// CHAOS-4547 (the argMax null-transition shift) covers nothing here:
	// this baseline's coverage shift is the fan-out's own uniform k=2
	// (CoverageShiftShape's rule 1 sees the fanned-out node/edge
	// differences too, so it refuses), and CHAOS-4547 is Intermittent, so
	// it reads as idle rather than stale.
	if !equalStrings(result.IdleIntermittentBaselineDefects, []string{"CHAOS-4547"}) {
		t.Fatalf("idle = %v, want [CHAOS-4547]", result.IdleIntermittentBaselineDefects)
	}
}
