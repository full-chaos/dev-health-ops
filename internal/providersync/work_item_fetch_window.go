package providersync

import "time"

// workItemFetchWindow is the window a work-item route puts into its provider
// request. Both bounds are whole-day aligned in UTC; a nil bound means the
// unit carries no instant on that side and the request sends no filter there.
type workItemFetchWindow struct {
	// Since is 00:00:00.000000 UTC of the day that holds the unit's SinceAt.
	Since *time.Time
	// Until is 23:59:59.999999 UTC of the day that holds the unit's BeforeAt.
	Until *time.Time
}

// workItemWholeDayFetchWindow turns a work-item unit's window into the
// whole-day fetch window. It is the ONLY place a work-item route may read the
// claim's window instants for a provider request (github, gitlab, jira,
// linear); TestWorkItemRoutesReadTheWindowOnlyThroughTheFetchWindowHelper
// fails on any other read.
//
// The rule is the frozen Python one (commit 0dcecf34a^, the last tree before
// the Go provider runtime), which had ONE job for all four providers:
//
//	processors/dataset_adapters.py:244-259  day = window_end.date()
//	                                        backfill_days = (window_end.date() - window_start.date()).days + 1
//	metrics/job_work_items.py:112-116       _date_range(day, backfill_days)
//	metrics/job_work_items.py:436-438       days = _date_range(day, backfill_days)
//	                                        since_dt = datetime.combine(min(days), time.min, tzinfo=timezone.utc)
//	                                        until_dt = datetime.combine(max(days), time.max, tzinfo=timezone.utc)
//
// So a run held every item changed since 00:00 UTC of the window's first day,
// and the per-day event counters of the sync deriver were computed over the
// full set of that day. The planner is equal in Python and Go; this alignment
// lived in the job, one hop outside the planner, and the port dropped it
// (CHAOS-8808): the routes sent the planner's exact instants, so an hourly
// unit rewrote the day's derived rows from one hour of items.
//
// Three things to know:
//
//   - The end day is the day of the end instant ITSELF (Python
//     `window_end.date()`), not of the instant before it. The deriver's day
//     loop (githubWorkItemDerivedDays) ends at date(BeforeAt - 1ns), so for a
//     window that ends exactly at 00:00:00 UTC the fetch window names one day
//     more than the loop. In every other case they name the same days.
//     TestWorkItemDerivedDaysStayInsideTheFetchWindow pins that relation.
//   - A nil bound stays nil. Python sent the start of the end day when the
//     start was absent; a Go unit with no start is a full fetch, and this
//     helper never narrows a fetch.
//   - Instants are converted to UTC before the day is taken. The Python
//     instants came from Postgres timestamptz columns and were UTC.
//
// time.max in Python is 23:59:59.999999 (microseconds), so Until carries
// 999999000 nanoseconds, not 999999999.
//
// This is the interim parity restore. It does not repair days that are
// already partial, and it does not make a derived table correct that needs
// items with no change on the day.
func workItemWholeDayFetchWindow(claim Claim) workItemFetchWindow {
	var window workItemFetchWindow
	if claim.SinceAt != nil {
		since := workItemFetchWindowUTCDay(*claim.SinceAt)
		window.Since = &since
	}
	if claim.BeforeAt != nil {
		until := workItemFetchWindowUTCDay(*claim.BeforeAt).
			Add(24*time.Hour - time.Microsecond)
		window.Until = &until
	}
	return window
}

func workItemFetchWindowUTCDay(value time.Time) time.Time {
	utc := value.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}
