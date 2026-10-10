package investment

import (
	"testing"
	"time"
)

// A unit takes its repository ownership at the end of its own span, and never
// at a time after the run: a span that ends after the run, or has no end, takes
// the run's time.
func TestAUnitTakesItsOwnershipAtItsEndAndNeverAfterTheRun(t *testing.T) {
	run := time.Date(2026, 1, 20, 2, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name          string
		unitEnd, want time.Time
	}{
		{"ended days before the run", run.AddDate(0, 0, -10), run.AddDate(0, 0, -10)},
		{"ended at the instant of the run", run, run},
		{"ends after the run", run.Add(time.Hour), run},
		{"no end", time.Time{}, run},
	} {
		if got := unitOwnershipAsOf(c.unitEnd, run); !got.Equal(c.want) {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}
