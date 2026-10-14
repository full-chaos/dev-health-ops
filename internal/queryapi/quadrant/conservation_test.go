package quadrant

import (
	"math"
	"testing"
)

// conservesTotal reports whether one axis of the team points (0 = x, 1 = y; the
// y of cycle x throughput is an additive count) sums to the organization's
// total (D5983), and the sum.
func conservesTotal(points map[string][2]float64, axis int, total float64) (float64, bool) {
	var sum float64
	for _, point := range points {
		sum += point[axis]
	}
	return sum, math.Abs(sum-total) <= 1e-6
}

// TestTheConservationCheckDetectsAWholePointForEachOwner keeps the old overcount
// detectable (CHAOS-9562): the points a reader gives when every owner of a
// co-owned item holds the whole item (5 items, 9 counted: i1, i2, i5 for team-a
// and team-b, i4 for team-a and team-d, i3 for team-a) fail the check that the
// owner-share points pass.
func TestTheConservationCheckDetectsAWholePointForEachOwner(t *testing.T) {
	whole := map[string][2]float64{"team-a": {0, 5}, "team-b": {0, 3}, "team-d": {0, 1}}
	if sum, ok := conservesTotal(whole, 1, 5); ok || sum != 9 {
		t.Fatalf("whole points: conserves = %v with sum %v, want a failed check at sum 9", ok, sum)
	}
	shared := map[string][2]float64{"team-a": {0, 3.6}, "team-b": {0, 0.9}, "team-d": {0, 0.5}}
	if sum, ok := conservesTotal(shared, 1, 5); !ok {
		t.Fatalf("owner-share points: conserves = %v with sum %v, want a pass", ok, sum)
	}
}
