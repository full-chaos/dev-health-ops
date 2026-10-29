package sync

import (
	"fmt"
	"testing"
	"time"
)

// CHAOS-11774: a planned window is never empty (Since >= Before): a unit over an empty window is a zero-width unit that
// the provider route refuses and the coverage projection renders as a 1-microsecond failed range. The windows tile the
// request exactly: the first starts at the requested since, the last ends at the requested before, and each end is the
// next start.
func TestResolveBackfillWindowsNeverPlansAnEmptyWindowAndTilesTheRequest(t *testing.T) {
	day := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	offsets := []time.Duration{
		0, time.Nanosecond, time.Microsecond, time.Microsecond + time.Nanosecond, time.Hour,
		24*time.Hour - time.Microsecond - time.Nanosecond, 24*time.Hour - time.Microsecond, 24*time.Hour - time.Nanosecond,
	}
	for _, dataset := range []struct{ provider, key string }{{"github", "cicd"}, {"linear", "work-items"}, {"github", "work-items"}} {
		for sinceDays := 0; sinceDays < 3; sinceDays++ {
			for _, so := range offsets {
				for span := 0; span <= 36; span++ {
					for _, bo := range offsets {
						since := day.AddDate(0, 0, sinceDays).Add(so)
						before := day.AddDate(0, 0, sinceDays+span).Add(bo)
						name := fmt.Sprintf("%s/%s since=%s before=%s", dataset.provider, dataset.key, since.Format(time.RFC3339Nano), before.Format(time.RFC3339Nano))
						windows, err := resolveBackfillWindows(since, before, dataset.provider, dataset.key)
						if !since.Before(before) {
							continue // an empty request is not planned at all (covered below)
						}
						if err != nil {
							t.Fatalf("%s: %v", name, err)
						}
						if len(windows) == 0 {
							t.Fatalf("%s: a non-empty request produced no window", name)
						}
						for i, w := range windows {
							if !w.Since.Before(w.Before) {
								t.Fatalf("%s: window %d is empty or inverted: [%s, %s)", name, i, w.Since.Format(time.RFC3339Nano), w.Before.Format(time.RFC3339Nano))
							}
							if i > 0 && !windows[i-1].Before.Equal(w.Since) {
								t.Fatalf("%s: windows %d and %d do not tile", name, i-1, i)
							}
						}
						if !windows[0].Since.Equal(since) || !windows[len(windows)-1].Before.Equal(before) {
							t.Fatalf("%s: the windows do not cover exactly the request: first %s last %s", name,
								windows[0].Since.Format(time.RFC3339Nano), windows[len(windows)-1].Before.Format(time.RFC3339Nano))
						}
					}
				}
			}
		}
	}
}
