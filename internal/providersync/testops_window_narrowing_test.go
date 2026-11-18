package providersync

import (
	"testing"
	"time"
)

func TestTestOpsBudgetBlockedCountLooksBackTheLookbackOnly(t *testing.T) {
	for _, test := range []struct {
		name    string
		blocked []bool
		want    int
	}{
		{"none", nil, 0},
		{"a success does not reset the count", []bool{false, true, false, true}, 2},
		{"all newest blocked", []bool{true, true, true}, 3},
		{"a unit past the lookback does not count", append(append([]bool{true}, make([]bool, TestOpsNarrowingLookback-1)...), true), 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := TestOpsBudgetBlockedCount(test.blocked); got != test.want {
				t.Fatalf("count = %d, want %d", got, test.want)
			}
		})
	}
}

func TestNarrowTestOpsWindowHalvesPerBlockedUnitDownToTheFloor(t *testing.T) {
	start := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name    string
		start   *time.Time
		span    time.Duration
		blocked int
		want    time.Duration
		changed bool
	}{
		{"no blocked unit", &start, 72 * time.Hour, 0, 72 * time.Hour, false},
		{"one halving", &start, 72 * time.Hour, 1, 36 * time.Hour, true},
		{"five halvings", &start, 72 * time.Hour, 5, 135 * time.Minute, true},
		{"the floor stops the halving", &start, 72 * time.Hour, 10, TestOpsWindowHalvingFloor, true},
		{"a span at the floor stays", &start, TestOpsWindowHalvingFloor, 3, TestOpsWindowHalvingFloor, false},
		{"a span below the floor stays", &start, 30 * time.Minute, 3, 30 * time.Minute, false},
		{"an open start stays", nil, 72 * time.Hour, 3, 72 * time.Hour, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			end := start.Add(test.span)
			got, changed := NarrowTestOpsWindow(test.start, end, test.blocked)
			if changed != test.changed || got.Sub(start) != test.want {
				t.Fatalf("narrowed span = %s changed %v, want %s changed %v", got.Sub(start), changed, test.want, test.changed)
			}
		})
	}
}

// The halved end is a whole microsecond (PostgreSQL's timestamptz precision), and the candidates are each distinct
// end the halving gives for 1 to TestOpsNarrowingLookback blocks, ending at the floor.
func TestNarrowTestOpsWindowEndsOnAMicrosecond(t *testing.T) {
	start := time.Date(2026, 11, 14, 0, 0, 0, 1000, time.UTC)
	end := start.Add(72*time.Hour + time.Microsecond)
	for blocked := 1; blocked <= TestOpsNarrowingLookback; blocked++ {
		got, _ := NarrowTestOpsWindow(&start, end, blocked)
		if got.Nanosecond()%1000 != 0 {
			t.Fatalf("blocked %d: end %s is not a whole microsecond", blocked, got.Format(time.RFC3339Nano))
		}
	}
	candidates := TestOpsNarrowingCandidates(&start, end)
	if len(candidates) == 0 || candidates[len(candidates)-1].Sub(start) != TestOpsWindowHalvingFloor {
		t.Fatalf("candidates = %v, want them to end at the floor", candidates)
	}
	for index := 1; index < len(candidates); index++ {
		if !candidates[index].Before(candidates[index-1]) {
			t.Fatalf("candidates are not strictly shrinking: %v", candidates)
		}
	}
	if got := TestOpsNarrowingCandidates(&start, start.Add(30*time.Minute)); len(got) != 0 {
		t.Fatalf("a span under the floor has candidates %v, want none", got)
	}
}
