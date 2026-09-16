package pythonparity

import (
	"math"
	"testing"
)

// TestMin2MatchesPythonTwoArgumentMin pins Min2 against CPython's `min(a, b)`
// semantics: the first argument wins ties, and a NaN in either position
// resolves according to WHICH argument it occupies, not to NaN itself.
func TestMin2MatchesPythonTwoArgumentMin(t *testing.T) {
	nan := math.NaN()
	cases := []struct {
		name string
		a, b float64
		want float64
	}{
		{"ordinary a smaller", 1.0, 2.0, 1.0},
		{"ordinary b smaller", 2.0, 1.0, 1.0},
		{"tie keeps first argument", 5.0, 5.0, 5.0},
		{"negative values", -3.0, -1.0, -3.0},
		// min(1.0, nan): "nan < 1.0" is false, so the candidate (1.0) is never
		// replaced -- the result is 1.0, not NaN.
		{"NaN as second argument keeps first", 1.0, nan, 1.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Min2(tc.a, tc.b); got != tc.want {
				t.Errorf("Min2(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}

	// min(nan, 1.0): the candidate starts as NaN, and "1.0 < nan" is false, so
	// the candidate is never replaced -- the result is NaN, not 1.0. Checked
	// separately because NaN != NaN under ==.
	if got := Min2(nan, 1.0); !math.IsNaN(got) {
		t.Errorf("Min2(NaN, 1.0) = %v, want NaN", got)
	}
}
