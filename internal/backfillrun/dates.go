package backfillrun

import (
	"fmt"
	"time"
)

// Window is the resolved backfill window: the last day and the day count, and
// the two instants the trigger carries (since at the start of its day, before
// at the last microsecond of the end day), as run_backfill_via_planner
// computes them.
type Window struct {
	EndDay time.Time // the last day, inclusive (midnight UTC)
	Days   int
	Since  time.Time
	Before time.Time
}

func midnight(day time.Time) time.Time {
	year, month, dayOfMonth := day.UTC().Date()
	return time.Date(year, month, dayOfMonth, 0, 0, 0, 0, time.UTC)
}

// ResolveWindow is resolve_date_range followed by the window arithmetic of
// _cmd_backfill_run and run_backfill_via_planner. since and before are the
// explicit --since and --before days (nil when absent); backfill is --backfill
// (Python's default is 1). today is the current UTC day.
func ResolveWindow(since, before *time.Time, backfill int, today time.Time) (Window, error) {
	var beforeDay time.Time
	if before != nil {
		beforeDay = midnight(*before)
	} else {
		beforeDay = midnight(today).AddDate(0, 0, 1)
	}
	endDay := beforeDay.AddDate(0, 0, -1)
	days := max(1, backfill)
	if since != nil {
		sinceDay := midnight(*since)
		if sinceDay.After(endDay) {
			return Window{}, fmt.Errorf("--since (%s) must be before --before (%s).", sinceDay.Format("2006-01-02"), beforeDay.Format("2006-01-02"))
		}
		days = int(endDay.Sub(sinceDay)/(24*time.Hour)) + 1
	}
	start := endDay.AddDate(0, 0, -(days - 1))
	return Window{
		EndDay: endDay, Days: days, Since: start,
		Before: endDay.Add(24*time.Hour - time.Microsecond),
	}, nil
}
