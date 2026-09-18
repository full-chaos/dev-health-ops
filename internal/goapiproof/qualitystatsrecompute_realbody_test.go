package goapiproof

import (
	"encoding/json"
	"math"
	"testing"
)

// Real captured team-scoped GET/POST /api/v1/investment bodies
// (testdata/investment_qualitystats_team_scoped_*, copied verbatim from a
// deployed-vs-deployed prove run, never hand-edited). The baseline leg
// aggregates the whole organisation; the candidate leg aggregates the
// team's own repositories. Without the two evidence-quality shapes, three
// findings sit outside every declaration on each pair:
// evidence_quality_stats.mean, evidence_quality_stats.stddev and
// evidence_quality_stats.quality_drivers (pinned by
// TestInvestmentTeamScopedParity_QualityStatsGuardRemoval).

type qualityStatsFixture struct {
	label, baseline, candidate string
}

var qualityStatsTeamScopedFixtures = []qualityStatsFixture{
	{"GET", "testdata/investment_qualitystats_team_scoped_get_baseline_2830edb5.json", "testdata/investment_qualitystats_team_scoped_get_candidate_887d573c.json"},
	{"POST", "testdata/investment_qualitystats_team_scoped_post_baseline_e23d2da5.json", "testdata/investment_qualitystats_team_scoped_post_candidate_9d0b2411.json"},
}

func TestInvestmentTeamScopedParity_RealCapturedQualityStatsFullyAdmitted(t *testing.T) {
	for _, fx := range qualityStatsTeamScopedFixtures {
		baseline := teamScopeRealBodySnapshotFromFile(t, fx.baseline)
		candidate := teamScopeRealBodySnapshotFromFile(t, fx.candidate)
		result := Compare(baseline, candidate, investmentTeamScopedParity)
		if result.TerminalState != TerminalStateMismatch {
			t.Fatalf("%s: terminal = %q, want mismatch", fx.label, result.TerminalState)
		}
		if result.DifferencesOutsideBaselineDefect != 0 {
			t.Fatalf("%s: outside = %d, want 0: findings %+v", fx.label, result.DifferencesOutsideBaselineDefect, result.Findings)
		}
	}
}

// TestInvestmentTeamScopedParity_MomentPairsTheBandsForbid replaces the
// real GET capture's mean/stddev with pairs no population with those
// band counts can have, and pins the exact outside count for each.
// mean and stddev are admitted or refused together (2 findings); a pair
// that also moves a driver threshold on the baseline adds the
// quality_drivers finding (3).
func TestInvestmentTeamScopedParity_MomentPairsTheBandsForbid(t *testing.T) {
	sd := 0.20818636744691635
	cases := []struct {
		name                               string
		baseMean, baseSD, candMean, candSD string
		want                               int
	}{
		{"negative candidate stddev", "", "", "", "-0.20818636744691635", 2},
		{"candidate variance written as stddev", "", "", "", jsonFloat(sd * sd), 2},
		{"candidate mean below its band interval", "0.25", "0.30", "0.32", "0.37", 3},
		{"candidate E[x^2] below the least its mean allows", "0.25", "0.19", "0.40", "0", 3},
		{"candidate mean above its band interval", "0.42", "0.32", "0.67", "0", 3},
	}
	for _, tc := range cases {
		baseline, candidate := mutatedQualityStatsPair(t, func(base, cand map[string]any) {
			set := func(stats map[string]any, key, value string) {
				if value != "" {
					stats[key] = json.Number(value)
				}
			}
			set(base, "mean", tc.baseMean)
			set(base, "stddev", tc.baseSD)
			set(cand, "mean", tc.candMean)
			set(cand, "stddev", tc.candSD)
		})
		result := Compare(baseline, candidate, investmentTeamScopedParity)
		if result.DifferencesOutsideBaselineDefect != tc.want {
			t.Errorf("%s: outside = %d, want %d: findings %+v", tc.name, result.DifferencesOutsideBaselineDefect, tc.want, result.Findings)
		}
	}
}

// TestInvestmentTeamScopedParity_SampleStddevAdmitted pins what the
// moment shape cannot separate on the real capture: at N = 1029,
// stddevSamp in place of stddevPop moves the candidate's stddev by a
// factor sqrt(N/(N-1)), which stays inside the exact range its band
// counts allow, so the pair is admitted. (On a small population the same
// substitution can leave that range and is refused;
// TestBandMomentSubsetPlan pins one.)
func TestInvestmentTeamScopedParity_SampleStddevAdmitted(t *testing.T) {
	sd := 0.20818636744691635 * math.Sqrt(1029.0/1028.0)
	baseline, candidate := mutatedQualityStatsPair(t, func(_, cand map[string]any) { cand["stddev"] = json.Number(jsonFloat(sd)) })
	result := Compare(baseline, candidate, investmentTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestInvestmentTeamScopedParity_EmptyTeamWindow replaces the real GET
// capture's candidate evidence_quality_stats with what computeQualityStats
// reports for a team with no work units in the window: total 0, every
// band 0, null mean and stddev, no drivers. The whole-organisation
// baseline is unchanged. The null moments are admitted; the same nulls on
// a candidate that does have rows stay outside.
func TestInvestmentTeamScopedParity_EmptyTeamWindow(t *testing.T) {
	empty := func(stats map[string]any) {
		stats["total"] = json.Number("0")
		stats["band_counts"] = map[string]any{"high": json.Number("0"), "moderate": json.Number("0"), "low": json.Number("0"), "very_low": json.Number("0"), "unknown": json.Number("0")}
		stats["mean"] = nil
		stats["stddev"] = nil
		stats["quality_drivers"] = []any{}
	}
	baseline, candidate := mutatedQualityStatsPair(t, func(_, cand map[string]any) { empty(cand) })
	result := Compare(baseline, candidate, investmentTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("empty team: outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}

	baseline, candidate = mutatedQualityStatsPair(t, func(_, cand map[string]any) {
		cand["mean"] = nil
		cand["stddev"] = nil
	})
	result = Compare(baseline, candidate, investmentTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("null moments with rows: outside = %d, want 2: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}
