package pythonparity

import (
	"math"
	"testing"
)

// TestMax2MatchesPythonTwoArgumentMax pins Max2 against CPython's `max(a, b)`
// semantics: the first argument wins ties, and a NaN in either position
// resolves according to WHICH argument it occupies, not to NaN itself.
func TestMax2MatchesPythonTwoArgumentMax(t *testing.T) {
	nan := math.NaN()
	cases := []struct {
		name string
		a, b float64
		want float64
	}{
		{"ordinary a larger", 2.0, 1.0, 2.0},
		{"ordinary b larger", 1.0, 2.0, 2.0},
		{"tie keeps first argument", 5.0, 5.0, 5.0},
		{"negative values", -1.0, -3.0, -1.0},
		// max(0.0, nan): "nan > 0.0" is false, so the candidate (0.0) is never
		// replaced -- the result is 0.0, not NaN.
		{"NaN as second argument keeps first", 0.0, nan, 0.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Max2(tc.a, tc.b); got != tc.want {
				t.Errorf("Max2(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}

	// max(nan, 0.0): the candidate starts as NaN, and "0.0 > nan" is false, so
	// the candidate is never replaced -- the result is NaN, not 0.0.
	if got := Max2(nan, 0.0); !math.IsNaN(got) {
		t.Errorf("Max2(NaN, 0.0) = %v, want NaN", got)
	}

	// The composed clamp idiom `Max2(lo, Min2(hi, value))` resolves a NaN
	// value to hi, matching CPython: `max(0.0, min(1.0, float('nan'))) == 1.0`,
	// because `min(1.0, nan)` keeps 1.0 (nan < 1.0 is false) and
	// `max(0.0, 1.0)` then keeps 1.0 (1.0 > 0.0 is true).
	if got := Max2(0.0, Min2(1.0, nan)); got != 1.0 {
		t.Errorf("Max2(0.0, Min2(1.0, NaN)) = %v, want 1.0 (Python's clamp NaN behaviour)", got)
	}
}
