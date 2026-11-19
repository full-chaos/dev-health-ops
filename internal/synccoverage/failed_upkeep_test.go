package synccoverage

import (
	"math/rand"
	"reflect"
	"testing"
	"time"
)

// TestFailedUpkeepMatchesTheWholeListFold runs random add/subtract sequences through pairState.addFailed and
// subtractFailed and, after every step, requires the list the fold computed before CHAOS-14167:
// mergeIntervals(append(failed, x)) for a failed unit, subtractIntervals(failed, []coverageInterval{x}) for a
// covering one.
func TestFailedUpkeepMatchesTheWholeListFold(t *testing.T) {
	random := rand.New(rand.NewSource(14167))
	base := time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC)
	lengths := []time.Duration{time.Microsecond, 2 * time.Microsecond, time.Hour - time.Microsecond, time.Hour,
		time.Hour + time.Microsecond, 2 * time.Hour, 3 * time.Hour, 7 * time.Hour, 24 * time.Hour}
	offsets := []time.Duration{0, time.Microsecond, -time.Microsecond, 2 * time.Microsecond, -2 * time.Microsecond}
	steps, splits, shortDrops := 0, 0, 0
	for sequence := 0; sequence < 10_000; sequence++ {
		state := &pairState{}
		var want []coverageInterval
		for step := 0; step < 1+random.Intn(60); step++ {
			since := base.Add(time.Duration(random.Intn(72))*time.Hour + offsets[random.Intn(len(offsets))])
			interval := coverageInterval{Since: since, Before: since.Add(lengths[random.Intn(len(lengths))])}
			before := len(want)
			if random.Intn(3) == 0 {
				want = mergeIntervals(append(want, interval))
				state.addFailed(interval)
			} else {
				hadShort := false
				for _, item := range want {
					hadShort = hadShort || item.Before.Sub(item.Since) <= intervalAdjacencyTolerance
				}
				want = subtractIntervals(want, []coverageInterval{interval})
				state.subtractFailed(interval)
				if len(want) > before {
					splits++
				}
				if hadShort {
					shortDrops++
				}
			}
			steps++
			if len(want) == 0 && len(state.Failed) == 0 {
				continue
			}
			if !reflect.DeepEqual(state.Failed, want) {
				t.Fatalf("sequence %d step %d (%s .. %s): failed list\n got %v\nwant %v",
					sequence, step, interval.Since.Format(time.RFC3339Nano), interval.Before.Format(time.RFC3339Nano), state.Failed, want)
			}
		}
	}
	if splits < 1_000 || shortDrops < 100 {
		t.Fatalf("random sequences reach too few shapes: %d splits, %d subtracts over a short interval", splits, shortDrops)
	}
	t.Logf("%d steps, %d splits, %d subtracts over a short interval", steps, splits, shortDrops)
}
