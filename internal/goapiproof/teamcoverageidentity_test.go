package goapiproof

import (
	"encoding/json"
	"os"
	"testing"
)

// This file exercises TeamCoverageIdentityShape (teamcoverageidentity.go)
// directly through its registered declaration on investmentFlowModeParity
// (restcorpus.go), against real captured POST /api/v1/investment/flow
// response pairs.

// teamCoverageIdentityTicket reads the ticket the registered
// TeamCoverageIdentityShape declaration actually carries, rather than a
// literal copy that could drift out of sync with restcorpus.go.
func teamCoverageIdentityTicket(t *testing.T) string {
	t.Helper()
	for _, defect := range investmentFlowRepoDedupParity.BaselineDefects {
		if defect.TeamCoverageIdentityShape != nil {
			return defect.Ticket
		}
	}
	t.Fatal("investmentFlowRepoDedupParity carries no TeamCoverageIdentityShape entry")
	return ""
}

// teamCoverageFlowFixtures names the three captured flow_mode-branch
// scenario pairs this route's own coverageStats/finishPresenceEdges
// identity was proven against: every one carries the SAME doubled
// repository (an unmerged physical repos row) feeding one team's own
// node total, and the SAME two team_coverage leaves that mechanism
// moves.
var teamCoverageFlowFixtures = []struct {
	name, baselinePath, candidatePath string
}{
	{
		name:          "team_category_repo_org",
		baselinePath:  "testdata/investmentflow_teamcategoryrepo_baseline_6f417283.json",
		candidatePath: "testdata/investmentflow_teamcategoryrepo_candidate_bfbc2328.json",
	},
	{
		name:          "team_category_subcategory_repo_org",
		baselinePath:  "testdata/investmentflow_teamcategorysubcategoryrepo_baseline_a5e3d505.json",
		candidatePath: "testdata/investmentflow_teamcategorysubcategoryrepo_candidate_86dd8f4e.json",
	},
	{
		name:          "team_subcategory_repo_with_drill_org",
		baselinePath:  "testdata/investmentflow_teamsubcategoryrepodrill_baseline_6ca90a71.json",
		candidatePath: "testdata/investmentflow_teamsubcategoryrepodrill_candidate_093ba6a0.json",
	},
}

func readFlowFixtureBody(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return body
}

func flowSnapshotFromBody(t *testing.T, body map[string]any) Snapshot {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	snapshot, err := DecodeRESTSnapshot(raw)
	if err != nil {
		t.Fatalf("decode REST fixture: %v", err)
	}
	return snapshot
}

// TestTeamCoverageIdentityShape_RealCapturedFlowCasesAreFullyAdmitted
// pins three real production cases: the doubled repository's own
// node/link differences are covered by the sibling fan-out/supersession-
// exclusion entries, and team_coverage/coverage.team_coverage are
// admitted by the identity, on every one of the three flow_mode scenario
// shapes, leaving nothing outside.
func TestTeamCoverageIdentityShape_RealCapturedFlowCasesAreFullyAdmitted(t *testing.T) {
	wantTicket := teamCoverageIdentityTicket(t)
	for _, fixture := range teamCoverageFlowFixtures {
		t.Run(fixture.name, func(t *testing.T) {
			baseline := flowSnapshotFromBody(t, readFlowFixtureBody(t, fixture.baselinePath))
			candidate := flowSnapshotFromBody(t, readFlowFixtureBody(t, fixture.candidatePath))

			result := Compare(baseline, candidate, investmentFlowModeParity)

			if result.TerminalState != TerminalStateMismatch {
				t.Fatalf("terminal = %q, want mismatch -- a declared defect never converts one", result.TerminalState)
			}
			if result.DifferencesOutsideBaselineDefect != 0 {
				t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
			}
			sawIdentity := false
			for _, ticket := range result.BaselineDefectsMatched {
				if ticket == wantTicket {
					sawIdentity = true
				}
			}
			if !sawIdentity {
				t.Fatalf("matched = %v, want %q present -- team_coverage/coverage.team_coverage must be admitted by the identity, not merely absent from this capture", result.BaselineDefectsMatched, wantTicket)
			}
		})
	}
}

// TestTeamCoverageIdentityShape_CoverageNotMatchingItsOwnNodesStaysOutside
// is RED: the baseline leg's own reported team_coverage is set to a
// value its OWN nodes cannot reproduce (every node difference is left
// exactly as captured, still fully covered by the sibling entries) --
// the identity's rule 1 fails on that leg alone, and team_coverage must
// stay outside even though nothing else changed.
func TestTeamCoverageIdentityShape_CoverageNotMatchingItsOwnNodesStaysOutside(t *testing.T) {
	wantTicket := teamCoverageIdentityTicket(t)
	fixture := teamCoverageFlowFixtures[0]
	baselineBody := readFlowFixtureBody(t, fixture.baselinePath)
	candidateBody := readFlowFixtureBody(t, fixture.candidatePath)

	baselineBody["team_coverage"] = baselineBody["team_coverage"].(float64) * 1.2
	baselineCoverage := baselineBody["coverage"].(map[string]any)
	baselineCoverage["team_coverage"] = baselineCoverage["team_coverage"].(float64) * 1.2

	baseline := flowSnapshotFromBody(t, baselineBody)
	candidate := flowSnapshotFromBody(t, candidateBody)

	result := Compare(baseline, candidate, investmentFlowModeParity)

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- a baseline team_coverage its own nodes cannot reproduce must never be admitted: findings %+v", result.Findings)
	}
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			t.Fatalf("matched = %v, must not include %q -- the identity does not hold on the baseline leg", result.BaselineDefectsMatched, wantTicket)
		}
	}
}

// teamCoverageFromNodesForTest computes the SAME identity
// TeamCoverageIdentityShape's own teamCoverageFromNodes does (assigned
// team-group node values / all team-group node values, "Unassigned
// team" excluded from the numerator), directly from a decoded nodes
// list -- used to re-derive a leg's OWN reported coverage after
// mutating that SAME leg's own nodes, so a test can isolate rule 2
// (sibling coverage) from rule 1 (the identity itself) by keeping rule 1
// satisfied on purpose.
func teamCoverageFromNodesForTest(t *testing.T, nodes []any) float64 {
	t.Helper()
	var total, assigned float64
	found := false
	for _, n := range nodes {
		node := n.(map[string]any)
		if node["group"] != "team" {
			continue
		}
		value, ok := node["value"].(float64)
		if !ok {
			continue
		}
		found = true
		total += value
		if node["name"] != "Unassigned team" {
			assigned += value
		}
	}
	if !found || total <= 0 {
		t.Fatal("no team-group nodes found to compute coverage from")
	}
	return assigned / total
}

// TestTeamCoverageIdentityShape_UncoveredTeamNodeDifferenceStaysOutside
// is RED, isolating rule 2 specifically: a team-group node's own value
// is reversed (candidate strictly greater than baseline) so the sibling
// supersession-exclusion entry's own direction-only citation over
// data.nodes refuses it -- but EACH leg's own reported team_coverage is
// re-derived from that SAME leg's own (mutated) nodes, so rule 1's
// identity still holds independently on both legs. Only rule 2 (every
// team-group node difference already covered) can explain why this
// stays outside; a prior version of this test reversed the node without
// re-deriving the reported ratios, which also broke rule 1 and could not
// tell the two rules apart.
func TestTeamCoverageIdentityShape_UncoveredTeamNodeDifferenceStaysOutside(t *testing.T) {
	wantTicket := teamCoverageIdentityTicket(t)
	fixture := teamCoverageFlowFixtures[0]
	baselineBody := readFlowFixtureBody(t, fixture.baselinePath)
	candidateBody := readFlowFixtureBody(t, fixture.candidatePath)

	baselineNodes := baselineBody["nodes"].([]any)
	candidateNodes := candidateBody["nodes"].([]any)

	var teamNodeName string
	for _, n := range baselineNodes {
		node := n.(map[string]any)
		if node["group"] == "team" && node["name"] != "Unassigned team" {
			teamNodeName = node["name"].(string)
			node["value"] = node["value"].(float64) * 0.5
			break
		}
	}
	if teamNodeName == "" {
		t.Fatal("fixture carries no assigned team node to mutate")
	}
	found := false
	for _, n := range candidateNodes {
		node := n.(map[string]any)
		if node["name"] == teamNodeName {
			// Candidate strictly greater than baseline at this key: the
			// OPPOSITE of the direction the sibling supersession-
			// exclusion entry's KeyedDirectionShape ever admits.
			node["value"] = node["value"].(float64) * 1.5
			found = true
		}
	}
	if !found {
		t.Fatalf("candidate fixture carries no node named %q to mutate", teamNodeName)
	}

	// Re-derive each leg's OWN reported coverage from that SAME leg's
	// own mutated nodes: rule 1 must stay satisfied on both legs, so
	// this test can isolate rule 2.
	baselineCoverage := teamCoverageFromNodesForTest(t, baselineNodes)
	candidateCoverage := teamCoverageFromNodesForTest(t, candidateNodes)
	baselineBody["team_coverage"] = baselineCoverage
	baselineBody["coverage"].(map[string]any)["team_coverage"] = baselineCoverage
	candidateBody["team_coverage"] = candidateCoverage
	candidateBody["coverage"].(map[string]any)["team_coverage"] = candidateCoverage

	baseline := flowSnapshotFromBody(t, baselineBody)
	candidate := flowSnapshotFromBody(t, candidateBody)

	result := Compare(baseline, candidate, investmentFlowModeParity)

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- an uncovered team-node difference must leave team_coverage outside too, even when each leg's own identity still holds: findings %+v", result.Findings)
	}
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			t.Fatalf("matched = %v, must not include %q -- the mutated team node is not covered by any sibling defect", result.BaselineDefectsMatched, wantTicket)
		}
	}
	sawCoverageOutside := false
	for _, f := range result.Findings {
		if (f.Path == "$.data.team_coverage" || f.Path == "$.data.coverage.team_coverage") && f.Kind == FindingMismatch {
			sawCoverageOutside = true
		}
	}
	if !sawCoverageOutside {
		t.Fatal("expected a team_coverage/coverage.team_coverage finding in this mutated fixture")
	}
}
