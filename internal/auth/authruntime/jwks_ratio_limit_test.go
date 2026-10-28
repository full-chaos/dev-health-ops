package authruntime

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/token"
)

// jwksRatioLimit is the bound of one ratio check of the JWKS stall test: factor times the baseline measured in the same
// test, but never below a floor. The floor is derived from the production promise, not from a wall time: a JWKS request
// that starts a key-state refresh waits at most token.KeyStateInlineWait for it, so a correct faulted request is never
// faster than about that, whatever the baseline.
func jwksRatioLimit(baseline time.Duration, factor int64, floor time.Duration) time.Duration {
	return max(time.Duration(factor)*baseline, floor)
}

// jwksMaxFloor and jwksP50Floor are the floors of the two checks of the JWKS stall test: four and one times the inline wait.
const (
	jwksMaxFloor = 4 * token.KeyStateInlineWait
	jwksP50Floor = token.KeyStateInlineWait
)

// The floor must not hide a slowdown that the ratio alone would catch: at a 20 ms baseline the max limit is 500 ms, so a
// 900 ms response (45x) is over it; only responses at or under the floor can hide behind it.
func TestJWKSRatioLimitCannotHideASlowdown(t *testing.T) {
	cases := []struct {
		name              string
		baseline, faulted time.Duration
		factor            int64
		floor             time.Duration
		wantOver          bool
	}{
		{"900 ms at a 20 ms baseline is 45x and over the max limit", 20 * time.Millisecond, 900 * time.Millisecond, 25, jwksMaxFloor, true},
		{"480 ms at a 20 ms baseline is 24x and within", 20 * time.Millisecond, 480 * time.Millisecond, 25, jwksMaxFloor, false},
		{"2.4 s at a 100 ms baseline is 24x and within", 100 * time.Millisecond, 2400 * time.Millisecond, 25, jwksMaxFloor, false},
		{"2.6 s at a 100 ms baseline is 26x and over", 100 * time.Millisecond, 2600 * time.Millisecond, 25, jwksMaxFloor, true},
		{"399 ms at a 2 ms baseline hides behind the floor", 2 * time.Millisecond, 399 * time.Millisecond, 25, jwksMaxFloor, false},
		{"401 ms at a 2 ms baseline is over the floor", 2 * time.Millisecond, 401 * time.Millisecond, 25, jwksMaxFloor, true},
		{"the 4.001 s request of a waiting probe is over at any baseline", 50 * time.Millisecond, 4001 * time.Millisecond, 25, jwksMaxFloor, true},
		{"p50: 900 ms at a 20 ms baseline is over", 20 * time.Millisecond, 900 * time.Millisecond, 5, jwksP50Floor, true},
		{"p50: 90 ms at a 20 ms baseline is within", 20 * time.Millisecond, 90 * time.Millisecond, 5, jwksP50Floor, false},
	}
	for _, tc := range cases {
		over := tc.faulted > jwksRatioLimit(tc.baseline, tc.factor, tc.floor)
		if over != tc.wantOver {
			t.Errorf("%s: over the limit = %v (limit %v), want %v", tc.name, over, jwksRatioLimit(tc.baseline, tc.factor, tc.floor), tc.wantOver)
		}
	}
	if jwksMaxFloor >= time.Second || jwksP50Floor >= jwksMaxFloor {
		t.Errorf("the floors are %v and %v: derived from the inline wait (%v), far under 1 s", jwksMaxFloor, jwksP50Floor, token.KeyStateInlineWait)
	}
}
