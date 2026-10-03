package aianalytics

import (
	"math"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

func ptr(v float64) *float64 { return &v }

// TestEveryFlowKindCarriesItsMeasuredValueThresholdUnitAndDirection pins, for each of the seven
// rules, the numbers the rule compared (value, constant threshold), their unit and the side of the
// limit that fires it. They are the values the rationale states, as fields (CHAOS-7626): none is read
// out of the sentence and none is computed anywhere else.
func TestEveryFlowKindCarriesItsMeasuredValueThresholdUnitAndDirection(t *testing.T) {
	const window = 30
	cases := []struct {
		name      string
		rules     []flowRule
		row       flowRow
		kind      model.ImproveOpportunityKind
		value     float64
		threshold float64
		unit      model.ImproveOpportunityUnit
		direction model.ThresholdDirection
		rationale string
	}{
		{"review latency", repoRules, flowRow{EntityID: "r", ReviewP50: ptr(52.04)},
			model.ImproveOpportunityKindHighReviewLatency, 52.04, 24, model.ImproveOpportunityUnitHours,
			model.ThresholdDirectionAbove, "Median first-review time was 52.0 h over the last 30 days (threshold: 24 h)."},
		{"rework", repoRules, flowRow{EntityID: "r", ReworkRatio: ptr(0.31)},
			model.ImproveOpportunityKindHighRework, 0.31, 0.20, model.ImproveOpportunityUnitRatio,
			model.ThresholdDirectionAbove, "PR rework ratio was 31% over the last 30 days (threshold: 20%)."},
		{"churn", repoRules, flowRow{EntityID: "r", ChurnRatio: ptr(0.56)},
			model.ImproveOpportunityKindHighChurn, 0.56, 0.30, model.ImproveOpportunityUnitRatio,
			model.ThresholdDirectionAbove, "Rework churn ratio was 56% over the last 30 days (threshold: 30%)."},
		{"change failure", repoRules, flowRow{EntityID: "r", ChangeFailure: ptr(0.21)},
			model.ImproveOpportunityKindHighChangeFailure, 0.21, 0.15, model.ImproveOpportunityUnitRatio,
			model.ThresholdDirectionAbove, "Change failure rate was 21% over the last 30 days (threshold: 15%)."},
		{"cycle time", teamRules, flowRow{EntityID: "t", CycleP50: ptr(160.5)},
			model.ImproveOpportunityKindSlowCycleTime, 160.5, 120, model.ImproveOpportunityUnitHours,
			model.ThresholdDirectionAbove, "Median cycle time was 160.5 h over the last 30 days (threshold: 120 h)."},
		{"wip", teamRules, flowRow{EntityID: "t", WipCongestion: ptr(1.08)},
			model.ImproveOpportunityKindHighWip, 1.08, 0.40, model.ImproveOpportunityUnitRatio,
			model.ThresholdDirectionAbove, "WIP congestion ratio was 108% over the last 30 days (threshold: 40%)."},
		{"throughput", teamRules, flowRow{EntityID: "t", ItemsCompleted: ptr(0)},
			model.ImproveOpportunityKindLowThroughput, 0, 2, model.ImproveOpportunityUnitItems,
			model.ThresholdDirectionBelow, "Only 0 items were completed over the last 30 days (threshold: 2)."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := applyFlowRules([]flowRow{tc.row}, tc.rules, window)
			if len(got) != 1 {
				t.Fatalf("want exactly one opportunity, got %d", len(got))
			}
			o := got[0]
			if o.Kind != tc.kind {
				t.Fatalf("kind = %v, want %v", o.Kind, tc.kind)
			}
			if math.Abs(o.Value-tc.value) > 1e-9 || math.Abs(o.Threshold-tc.threshold) > 1e-9 {
				t.Errorf("value/threshold = %v/%v, want %v/%v", o.Value, o.Threshold, tc.value, tc.threshold)
			}
			if o.Unit != tc.unit {
				t.Errorf("unit = %v, want %v", o.Unit, tc.unit)
			}
			if o.ThresholdDirection != tc.direction {
				t.Errorf("direction = %v, want %v", o.ThresholdDirection, tc.direction)
			}
			// The rationale sentence is unchanged, byte for byte.
			if o.Rationale != tc.rationale {
				t.Errorf("rationale = %q, want %q", o.Rationale, tc.rationale)
			}
			// The fields say the same as the sentence: the threshold is the rule's constant and the
			// direction agrees with which side fires.
			fires := o.Value > o.Threshold
			if o.ThresholdDirection == model.ThresholdDirectionBelow {
				fires = o.Value < o.Threshold
			}
			if !fires {
				t.Errorf("value %v does not fire against threshold %v (%v)", o.Value, o.Threshold, o.ThresholdDirection)
			}
			// The score is still computed from the same value and threshold.
			want := scoreRatio(o.Value, o.Threshold)
			if o.ThresholdDirection == model.ThresholdDirectionBelow {
				want = scoreDelta(math.Max(0, o.Threshold-o.Value), o.Threshold)
			}
			if math.Abs(o.Score-clamp01(want)) > 1e-9 {
				t.Errorf("score = %v, want %v from the same numbers", o.Score, clamp01(want))
			}
		})
	}
}

// TestEveryDetectorKindIsCoveredByAMeasure makes sure a new kind cannot ship without value fields:
// every rule output must carry a non-empty unit and direction and a positive threshold.
func TestEveryDetectorKindIsCoveredByAMeasure(t *testing.T) {
	rows := []flowRow{{EntityID: "r", ReviewP50: ptr(99), ReworkRatio: ptr(0.9), ChurnRatio: ptr(0.9), ChangeFailure: ptr(0.9)}}
	teams := []flowRow{{EntityID: "t", CycleP50: ptr(999), WipCongestion: ptr(9), ItemsCompleted: ptr(0)}}
	all := append(applyFlowRules(rows, repoRules, 30), applyFlowRules(teams, teamRules, 30)...)
	if len(all) != 7 {
		t.Fatalf("expected the seven kinds, got %d", len(all))
	}
	for _, o := range all {
		if o.Unit == "" || o.ThresholdDirection == "" || o.Threshold <= 0 {
			t.Errorf("%v carries no measure: unit=%q direction=%q threshold=%v", o.Kind, o.Unit, o.ThresholdDirection, o.Threshold)
		}
	}
}
