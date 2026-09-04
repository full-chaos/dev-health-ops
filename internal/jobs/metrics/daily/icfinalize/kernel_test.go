package icfinalize

import (
	"math"
	"testing"
)

// The cases that DISCRIMINATE mean-rank from weak-rank. Every one of these has
// a tie, because without a tie the two conventions agree and the test proves
// nothing about which was ported.
//
// weak  = (count_less + count_equal) / n
// mean  = (count_less + 0.5*count_equal) / n   <- what compute_ic.py computes
func TestPercentileRankIsMeanRankNotWeakRank(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		values   []float64
		value    float64
		wantMean float64
		weak     float64
	}{
		{
			name:     "all tied -- the low-activity team, every member 0",
			values:   []float64{0, 0, 0, 0},
			value:    0,
			wantMean: 0.5,
			weak:     1.0,
		},
		{
			name:     "one tie among distinct values",
			values:   []float64{1, 2, 2, 3},
			value:    2,
			wantMean: 0.5,  // (1 + 0.5*2)/4
			weak:     0.75, // (1 + 2)/4
		},
		{
			name:     "tie at the bottom",
			values:   []float64{0, 0, 5},
			value:    0,
			wantMean: 1.0 / 3.0, // (0 + 0.5*2)/3
			weak:     2.0 / 3.0,
		},
		{
			name:     "tie at the top",
			values:   []float64{1, 9, 9},
			value:    9,
			wantMean: 2.0 / 3.0, // (1 + 0.5*2)/3
			weak:     1.0,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got := PercentileRank(testCase.values, testCase.value)
			if got != testCase.wantMean {
				t.Fatalf("PercentileRank = %v, want %v (mean rank)", got, testCase.wantMean)
			}
			if got == testCase.weak {
				t.Fatalf("PercentileRank returned the WEAK rank %v -- the port followed "+
					"_percentile_rank's comment instead of its code", testCase.weak)
			}
		})
	}
}

// Empty is 0.5, not 0 and not NaN. A one-member team also lands here in
// effect, so the two situations are indistinguishable downstream -- recorded
// because a reader of the output cannot tell them apart.
func TestPercentileRankEmptyIsAHalf(t *testing.T) {
	if got := PercentileRank(nil, 42); got != 0.5 {
		t.Fatalf("PercentileRank(nil) = %v, want 0.5", got)
	}
	if got := PercentileRank([]float64{}, 42); got != 0.5 {
		t.Fatalf("PercentileRank(empty) = %v, want 0.5", got)
	}
	// A one-member team ranks 0.5 too -- same value, different reason.
	if got := PercentileRank([]float64{7}, 7); got != 0.5 {
		t.Fatalf("PercentileRank(single) = %v, want 0.5", got)
	}
}

// The genuinely non-discriminating shape: the value is ABSENT from the vector,
// so countEqual is 0 and mean == weak == countLess/n. Kept deliberately, and
// labelled, so nobody later mistakes it for coverage of the convention.
//
// This test was WRONG in its first revision, in two ways at once, and the
// suite caught both: it used PercentileRank([1,2,3,4], 3), where the value is
// PRESENT, so countEqual is 1 and the answer is 0.625 (mean) vs 0.75 (weak) --
// the case discriminates after all, and 0.5 was simply the wrong expectation.
// "No duplicate values in the vector" is not the same condition as "no ties
// with the value being ranked", and only the second one makes the conventions
// agree.
func TestPercentileRankWithAValueAbsentFromTheVectorCannotDiscriminate(t *testing.T) {
	// 2.5 is absent: countLess=2, countEqual=0 -> 2/4 under BOTH conventions.
	if got := PercentileRank([]float64{1, 2, 3, 4}, 2.5); got != 0.5 {
		t.Fatalf("PercentileRank = %v, want 0.5", got)
	}
	// The contrast, proving the distinction above is real: the SAME vector with
	// a value that IS present does discriminate.
	if got := PercentileRank([]float64{1, 2, 3, 4}, 3); got != 0.625 {
		t.Fatalf("PercentileRank(present value) = %v, want 0.625 (mean rank); "+
			"weak rank would be 0.75", got)
	}
}

// bool is checked FIRST and yields 0.0 for both values -- necessary because
// bool subclasses int in Python. A Go port that omitted the case would return
// 1.0 for true and diverge silently.
func TestFloatValueBoolIsZeroForBoth(t *testing.T) {
	if got := FloatValue(true); got != 0 {
		t.Fatalf("FloatValue(true) = %v, want 0 -- bool subclasses int in Python "+
			"and _float_value returns 0.0 for it", got)
	}
	if got := FloatValue(false); got != 0 {
		t.Fatalf("FloatValue(false) = %v, want 0", got)
	}
}

func TestFloatValueNumerics(t *testing.T) {
	for _, testCase := range []struct {
		in   any
		want float64
	}{
		{int(3), 3}, {int64(-4), -4}, {uint8(255), 255},
		{float32(1.5), 1.5}, {float64(2.25), 2.25},
		{nil, 0}, {struct{}{}, 0},
	} {
		if got := FloatValue(testCase.in); got != testCase.want {
			t.Fatalf("FloatValue(%#v) = %v, want %v", testCase.in, got, testCase.want)
		}
	}
}

// The reachability recorder must DETECT, or "no strings seen" would be
// meaningless. This is the positive control for the measurement that decides
// whether a Python-float() primitive gets written at all.
func TestStringInputRecorderActuallyDetects(t *testing.T) {
	ResetStringInputSeen()
	if seen, _ := StringInputSeen(); seen {
		t.Fatal("recorder reported a string before any was passed")
	}
	_ = FloatValue("1e3")
	seen, example := StringInputSeen()
	if !seen || example != "1e3" {
		t.Fatalf("recorder did not detect a string input (seen=%v example=%q) -- "+
			"a false from this recorder would prove nothing", seen, example)
	}
	ResetStringInputSeen()
}

// The axis asymmetry is the reference's: log1p on churn and cycle, RAW on wip.
func TestLandscapeAxesApplyLog1pToChurnAndCycleButNotWip(t *testing.T) {
	churnXY, cycleXY, wipXY := LandscapeAxes(100, 7, 48, 5)
	if churnXY[0] != math.Log1p(100) {
		t.Fatalf("churn x = %v, want log1p(100)", churnXY[0])
	}
	if cycleXY[0] != math.Log1p(48) {
		t.Fatalf("cycle x = %v, want log1p(48)", cycleXY[0])
	}
	if wipXY[0] != 5 {
		t.Fatalf("wip x = %v, want RAW 5 -- log1p is NOT applied to wip", wipXY[0])
	}
	for _, axis := range [][2]float64{churnXY, cycleXY, wipXY} {
		if axis[1] != 7 {
			t.Fatalf("y = %v, want delivery 7 on every map", axis[1])
		}
	}
}
