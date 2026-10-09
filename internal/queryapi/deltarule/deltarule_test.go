package deltarule

import "testing"

func TestADeltaNeedsTwoMeasuredValues(t *testing.T) {
	for _, tc := range []struct {
		name             string
		cur, prior       float64
		curHas, priorHas bool
		wantAbs, wantPct float64
		wantComplete     bool
	}{
		{"both measured", 0, 50, true, true, -50, -100, true},
		{"both measured, prior zero", 5, 0, true, true, 5, 0, true},
		{"current has no data", 0, 50, false, true, 0, 0, false},
		{"prior has no data", 25, 0, true, false, 0, 0, false},
		{"neither has data", 0, 0, false, false, 0, 0, false},
	} {
		if got := Complete(tc.curHas, tc.priorHas); got != tc.wantComplete {
			t.Errorf("%s: Complete = %v, want %v", tc.name, got, tc.wantComplete)
		}
		if got := Absolute(tc.cur, tc.prior, tc.curHas, tc.priorHas); got != tc.wantAbs {
			t.Errorf("%s: Absolute = %v, want %v", tc.name, got, tc.wantAbs)
		}
		if got := Pct(tc.cur, tc.prior, tc.curHas, tc.priorHas); got != tc.wantPct {
			t.Errorf("%s: Pct = %v, want %v", tc.name, got, tc.wantPct)
		}
	}
}
