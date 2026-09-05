package remaining

import (
	"math"
	"testing"
)

// TestComputeConfidenceFMABarrierIsLoadBearing pins the exact bit pattern the
// BARRIERED (CPython-matching) evaluation produces, so removing the float64()
// barriers in ComputeConfidence reddens this test.
//
// MEASURED ON arm64 (bigboy, aarch64), 2026-09-05: over 1,920,000 input
// combinations, the barriered and compiler-fused forms disagree on 399,209 of
// them -- 20.8%, always by exactly one ULP. This is not a theoretical hazard:
// one input in five would diverge from CPython.
//
//	cov=0.0025 samp=0.0025 conf=1/9
//	  barriered = 0.035083333333333334  bits=0x3fa1f671529a485d   <- CPython
//	  fused     = 0.03508333333333333   bits=0x3fa1f671529a485c
//
// Inputs below are chosen to reproduce that separating case through the real
// signature: sampleScore = 1/400 = 0.0025, confoundScore = 1/(1+8) = 1/9.
//
// Compared by BIT PATTERN, never by tolerance -- a tolerance is exactly what
// would hide a one-ULP FMA divergence, which is the only thing this test exists
// to catch.
func TestComputeConfidenceFMABarrierIsLoadBearing(t *testing.T) {
	got := ComputeConfidence(0.0025, 1, 8, 400)

	const wantBits = 0x3fa1f671529a485d
	if math.Float64bits(got) != wantBits {
		t.Fatalf("ComputeConfidence FMA barrier broken:\n got  = %v bits=%#016x\n want = %v bits=%#016x\n"+
			"If the barriers in ComputeConfidence were removed, arm64 fuses the\n"+
			"multiply-adds and rounds once instead of twice, diverging from CPython.",
			got, math.Float64bits(got), math.Float64frombits(wantBits), uint64(wantBits))
	}
}

// TestComputeDeltaNullSemantics pins _compute_delta's three None paths and the
// one arithmetic path (release_impact.py:254-299). The guard ORDER matters: the
// sample-size check precedes the pre-rate check.
func TestComputeDeltaNullSemantics(t *testing.T) {
	f := func(v float64) *float64 { return &v }

	for _, tc := range []struct {
		name         string
		pre, post    SignalRate
		minSessions  int
		wantDeltaNil bool
		wantDelta    float64
		wantPostNil  bool
	}{
		{
			name: "too few sessions returns nil delta but keeps postRate",
			pre:  SignalRate{Rate: f(0.5), Sessions: 10}, post: SignalRate{Rate: f(0.75), Sessions: 10},
			minSessions: 300, wantDeltaNil: true, wantPostNil: false,
		},
		{
			name: "nil pre rate returns nil delta",
			pre:  SignalRate{Rate: nil, Sessions: 200}, post: SignalRate{Rate: f(0.75), Sessions: 200},
			minSessions: 300, wantDeltaNil: true, wantPostNil: false,
		},
		{
			name: "zero pre rate returns nil delta (division guard)",
			pre:  SignalRate{Rate: f(0.0), Sessions: 200}, post: SignalRate{Rate: f(0.75), Sessions: 200},
			minSessions: 300, wantDeltaNil: true, wantPostNil: false,
		},
		{
			name: "nil post rate returns nil delta and nil postRate",
			pre:  SignalRate{Rate: f(0.5), Sessions: 200}, post: SignalRate{Rate: nil, Sessions: 200},
			minSessions: 300, wantDeltaNil: true, wantPostNil: true,
		},
		{
			name: "computes (post-pre)/pre when all guards pass",
			pre:  SignalRate{Rate: f(0.5), Sessions: 200}, post: SignalRate{Rate: f(0.75), Sessions: 200},
			minSessions: 300, wantDeltaNil: false, wantDelta: 0.5,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delta, postRate := ComputeDelta(tc.pre, tc.post, tc.minSessions)
			if (delta == nil) != tc.wantDeltaNil {
				t.Fatalf("delta nil = %v, want nil = %v", delta == nil, tc.wantDeltaNil)
			}
			if !tc.wantDeltaNil && math.Float64bits(*delta) != math.Float64bits(tc.wantDelta) {
				t.Fatalf("delta = %v, want %v (bit-exact)", *delta, tc.wantDelta)
			}
			if (postRate == nil) != tc.wantPostNil {
				t.Fatalf("postRate nil = %v, want nil = %v", postRate == nil, tc.wantPostNil)
			}
		})
	}
}

// TestSignalRateNullSemantics pins _signal_rate (release_impact.py:200-253):
// no rows and zero sessions BOTH yield (nil, 0), so a caller summing sessions
// across windows cannot pick up a phantom count from either.
func TestSignalRateNullSemantics(t *testing.T) {
	if r := NewSignalRate(false, 99, 99); r.Rate != nil || r.Sessions != 0 {
		t.Fatalf("no rows: got rate=%v sessions=%d, want nil/0", r.Rate, r.Sessions)
	}
	if r := NewSignalRate(true, 50, 0); r.Rate != nil || r.Sessions != 0 {
		t.Fatalf("zero sessions: got rate=%v sessions=%d, want nil/0", r.Rate, r.Sessions)
	}
	r := NewSignalRate(true, 50, 200)
	if r.Rate == nil || math.Float64bits(*r.Rate) != math.Float64bits(0.25) || r.Sessions != 200 {
		t.Fatalf("normal: got rate=%v sessions=%d, want 0.25/200", r.Rate, r.Sessions)
	}
}

// TestCoverageRatioAndCompleteness pins the two clamped ratios.
func TestCoverageRatioAndCompleteness(t *testing.T) {
	if got := CoverageRatio(3, 0); got != 0.0 {
		t.Fatalf("zero denominator must be 0.0 not NaN, got %v", got)
	}
	if got := CoverageRatio(1, 4); math.Float64bits(got) != math.Float64bits(0.25) {
		t.Fatalf("CoverageRatio(1,4) = %v, want 0.25", got)
	}
	if got := DataCompleteness(48); got != 1.0 {
		t.Fatalf("DataCompleteness clamps at 1.0, got %v", got)
	}
	if got := DataCompleteness(6); math.Float64bits(got) != math.Float64bits(0.25) {
		t.Fatalf("DataCompleteness(6) = %v, want 0.25", got)
	}
}

// TestMissingRequiredFieldsCountsNils pins the `missing` tally.
func TestMissingRequiredFieldsCountsNils(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	if n := MissingRequiredFields(nil, nil, nil, nil); n != 4 {
		t.Fatalf("all nil = %d, want 4", n)
	}
	if n := MissingRequiredFields(f(1), nil, f(3), nil); n != 2 {
		t.Fatalf("two nil = %d, want 2", n)
	}
	if n := MissingRequiredFields(f(1), f(2), f(3), f(4)); n != 0 {
		t.Fatalf("none nil = %d, want 0", n)
	}
}
