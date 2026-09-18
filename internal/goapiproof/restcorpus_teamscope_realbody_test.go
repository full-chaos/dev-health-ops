package goapiproof

import (
	"os"
	"testing"
)

// This file exercises the team-scope declarations this ticket
// adds (restcorpus.go: heatmapRepoTouchpointsTeamScopeSubsetDefect and
// its two siblings, investmentTeamScopeDictSubsetDefects,
// investmentSunburstTeamScopeSubsetDefect) against REAL production
// ORG-SCOPE bodies captured from a deployed-vs-deployed prove run
// (internal/goapiproof/testdata/*, copied verbatim from
// .remember/lanes/gwc-api-teamcases/captured/ -- never hand-edited).
// None of these captures is itself team-scoped, so this file does not
// (and cannot) prove the new declarations admit a real team-scope
// divergence; it proves the opposite, narrower claim every additive
// declaration needs: adding a new BaselineDefect entry to a route's own
// team-scoped Parity, alongside its EXISTING org-scope Options unchanged,
// changes NOTHING observable on real, non-team-scoped production data --
// the new entry's own admits() simply never matches anything these
// bodies carry, so the org-scope Options and its team-scoped sibling
// must classify a real capture IDENTICALLY.

func teamScopeRealBodySnapshotFromFile(t *testing.T, path string) Snapshot {
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

func assertSameClassification(t *testing.T, label string, orgOpts, teamOpts Options, baseline, candidate Snapshot) {
	t.Helper()
	orgResult := Compare(baseline, candidate, orgOpts)
	teamResult := Compare(baseline, candidate, teamOpts)
	if teamResult.TerminalState != orgResult.TerminalState {
		t.Fatalf("%s: team-scoped terminal = %q, org terminal = %q -- adding a team-scope-only declaration must not change classification of real non-team-scoped data", label, teamResult.TerminalState, orgResult.TerminalState)
	}
	if teamResult.DifferencesOutsideBaselineDefect != orgResult.DifferencesOutsideBaselineDefect {
		t.Fatalf("%s: team-scoped outside = %d, org outside = %d -- findings %+v", label, teamResult.DifferencesOutsideBaselineDefect, orgResult.DifferencesOutsideBaselineDefect, teamResult.Findings)
	}
}

// TestHeatmapHotspotRiskTeamScopedParity_RealOrgBodyUnchanged pins
// heatmapHotspotRiskTeamScopeSubsetDefect: a real captured GET
// /api/v1/heatmap hotspot_risk_org pair (a clean match, INDEX.txt) is
// classified identically whether compared under heatmapHotspotRiskParity
// or its team-scoped sibling.
func TestHeatmapHotspotRiskTeamScopedParity_RealOrgBodyUnchanged(t *testing.T) {
	baseline := teamScopeRealBodySnapshotFromFile(t, "testdata/heatmap_hotspot_risk_org_baseline_14eaf763.json")
	candidate := teamScopeRealBodySnapshotFromFile(t, "testdata/heatmap_hotspot_risk_org_candidate_4f4896fd.json")
	assertSameClassification(t, "hotspot_risk_org", heatmapHotspotRiskParity, heatmapHotspotRiskTeamScopedParity, baseline, candidate)
}

// TestHeatmapReviewWaitDensityTeamScopedParity_RealOrgBodyUnchanged pins
// heatmapReviewWaitDensityTeamScopeSubsetDefect against a real captured
// pair with a KNOWN, already-declared divergence (INDEX.txt:
// review_wait_density_org state=mismatch, admitted by
// heatmapDedupParity's own KeyedDirectionShape) -- the harder case,
// since it proves the new BoundedLeavesAllKeys entry does not ALSO
// double-admit (or destabilize the admission of) a real divergence an
// unrelated sibling mechanism already explains.
func TestHeatmapReviewWaitDensityTeamScopedParity_RealOrgBodyUnchanged(t *testing.T) {
	baseline := teamScopeRealBodySnapshotFromFile(t, "testdata/heatmap_review_wait_density_org_baseline_1fda5260.json")
	candidate := teamScopeRealBodySnapshotFromFile(t, "testdata/heatmap_review_wait_density_org_candidate_0994d5c6.json")
	assertSameClassification(t, "review_wait_density_org", heatmapReviewWaitDensityParity, heatmapReviewWaitDensityTeamScopedParity, baseline, candidate)
}

// TestInvestmentTeamScopedParity_RealOrgBodyUnchanged pins all four
// investmentTeamScopeDictSubsetDefects entries at once against a real
// captured GET /api/v1/investment default_window pair (a clean match,
// INDEX.txt) -- proving the AdmitBaselineOnlyKeys opt-in does not admit
// anything on org-scope data, where every theme/subcategory/quality-band
// key that exists at all necessarily exists on BOTH planes (there is no
// narrower population to leave one behind).
func TestInvestmentTeamScopedParity_RealOrgBodyUnchanged(t *testing.T) {
	baseline := teamScopeRealBodySnapshotFromFile(t, "testdata/investment_default_window_baseline_852da907.json")
	candidate := teamScopeRealBodySnapshotFromFile(t, "testdata/investment_default_window_candidate_3508a814.json")
	assertSameClassification(t, "investment_default_window", investmentParity, investmentTeamScopedParity, baseline, candidate)
}

// TestInvestmentSunburstTeamScopedParity_RealOrgBodyUnchanged pins
// investmentSunburstTeamScopeSubsetDefect against a real captured GET
// /api/v1/investment/sunburst default_window pair (a clean match,
// INDEX.txt).
func TestInvestmentSunburstTeamScopedParity_RealOrgBodyUnchanged(t *testing.T) {
	baseline := teamScopeRealBodySnapshotFromFile(t, "testdata/investmentsunburst_default_window_baseline_460b21c4.json")
	candidate := teamScopeRealBodySnapshotFromFile(t, "testdata/investmentsunburst_default_window_candidate_ddb04883.json")
	assertSameClassification(t, "investmentsunburst_default_window", investmentSunburstParityWithLimit(investmentSunburstDefaultLimit), investmentSunburstTeamScopedParityWithLimit(investmentSunburstDefaultLimit), baseline, candidate)
}

// TestDrilldownPRsTeamScopedParity_RealOrgBodyUnchanged pins
// drilldownPRsTeamScopeSubsetDefect against a real captured GET
// /api/v1/drilldown/prs default_window pair (a LIMIT-saturated 50/50
// mismatch admitted by drilldownPRsParity's own existing dedup
// declarations, INDEX.txt) -- the harder case, proving the new
// membership/EqualLeaves entry does not destabilize a route whose own
// list is already at the LIMIT boundary this ticket's own drilldown/prs
// declaration is deliberately careful about (see its own doc comment,
// restcorpus.go).
func TestDrilldownPRsTeamScopedParity_RealOrgBodyUnchanged(t *testing.T) {
	baseline := teamScopeRealBodySnapshotFromFile(t, "testdata/drilldownprs_default_window_baseline_aefc6e04.json")
	candidate := teamScopeRealBodySnapshotFromFile(t, "testdata/drilldownprs_default_window_candidate_3f2f9f83.json")
	assertSameClassification(t, "drilldownprs_default_window", drilldownPRsParity, drilldownPRsTeamScopedParity, baseline, candidate)
}
