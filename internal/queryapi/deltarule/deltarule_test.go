package deltarule

import "testing"

func ptrOrNaN(p *float64) any {
	if p == nil {
		return nil
	}
	return *p
}

func TestADeltaNeedsTwoMeasuredValuesAndIsUndefinedFromAMeasuredZero(t *testing.T) {
	for _, tc := range []struct {
		name             string
		cur, prior       float64
		curHas, priorHas bool
		kind             Kind
		pct              any // nil = null
		abs              float64
		complete         bool
	}{
		{"both measured", 0, 50, true, true, KindPct, -100.0, -50, true},
		{"both measured, a rise", 75, 50, true, true, KindPct, 50.0, 25, true},
		{"measured zero in both windows is a true 0 %", 0, 0, true, true, KindPct, 0.0, 0, true},
		{"a measured zero before, a value now is undefined, not 0 %", 5, 0, true, true, KindFromZero, nil, 5, true},
		{"a measured zero before, a negative value now", -5, 0, true, true, KindFromZero, nil, -5, true},
		{"current has no data", 0, 50, false, true, KindNone, 0.0, 0, false},
		{"prior has no data", 25, 0, true, false, KindNone, 0.0, 0, false},
		{"neither has data", 0, 0, false, false, KindNone, 0.0, 0, false},
	} {
		d := Of(tc.cur, tc.prior, tc.curHas, tc.priorHas)
		if d.Kind != tc.kind {
			t.Errorf("%s: Kind = %v, want %v", tc.name, d.Kind, tc.kind)
		}
		if got := ptrOrNaN(d.Pct); got != tc.pct {
			t.Errorf("%s: Pct = %v, want %v", tc.name, got, tc.pct)
		}
		if got := Absolute(tc.cur, tc.prior, tc.curHas, tc.priorHas); got != tc.abs {
			t.Errorf("%s: Absolute = %v, want %v", tc.name, got, tc.abs)
		}
		if got := Complete(tc.curHas, tc.priorHas); got != tc.complete {
			t.Errorf("%s: Complete = %v, want %v", tc.name, got, tc.complete)
		}
	}
}
