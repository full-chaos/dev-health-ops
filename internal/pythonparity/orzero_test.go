package pythonparity

import (
	"math"
	"testing"
)

// TestOrZeroMatchesPythonTruthyOrIdiom pins OrZero against Python's
// `value or 0.0` for every non-finite class plus the two finite zeros.
func TestOrZeroMatchesPythonTruthyOrIdiom(t *testing.T) {
	cases := []struct {
		name  string
		value float64
		want  float64
	}{
		{"positive zero unchanged", 0.0, 0.0},
		{"negative zero normalizes to positive zero", math.Copysign(0, -1), 0.0},
		{"NaN passes through (bool(nan) is True in Python)", math.NaN(), math.NaN()},
		{"+Inf passes through (truthy)", math.Inf(1), math.Inf(1)},
		{"-Inf passes through (truthy)", math.Inf(-1), math.Inf(-1)},
		{"ordinary negative value unchanged", -5.5, -5.5},
		{"ordinary positive value unchanged", 5.5, 5.5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := OrZero(tc.value)
			if math.IsNaN(tc.want) {
				if !math.IsNaN(got) {
					t.Errorf("OrZero(%v) = %v, want NaN", tc.value, got)
				}
				return
			}
			if got != tc.want || math.Signbit(got) != math.Signbit(tc.want) {
				t.Errorf("OrZero(%v) = %v (signbit=%v), want %v (signbit=%v)",
					tc.value, got, math.Signbit(got), tc.want, math.Signbit(tc.want))
			}
		})
	}
}
