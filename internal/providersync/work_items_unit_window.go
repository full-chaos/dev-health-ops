package providersync

import "time"

// workItemsUnitWindowDays validates the window of a work-items sync unit and
// returns the UTC days it covers, ascending.
//
// It is the ONE window rule of the four work-items routes, and each route
// applies it before any provider request. A unit whose `since` day is after
// its `before` day is malformed, not an empty range: it fails the unit. A
// window of more than githubWorkItemDerivedMaxBackfillDays days fails too:
// the planner chunks a backfill, so a wider window means the chunker did not
// run. The derivation of the daily tables used to enforce both when it built
// its day loop; the unit derives nothing now, and the rule stays as a
// validation of the unit, because the days of the window are what the unit
// reports as touched.
//
// The day mapping and the bound are WorkItemsUnitWindowDays and
// githubWorkItemDerivedMaxBackfillDays: one rule, in one place.
func workItemsUnitWindowDays(claim Claim, normalizedAt time.Time) ([]time.Time, error) {
	return WorkItemsUnitWindowDays(claim.SinceAt, claim.BeforeAt, normalizedAt)
}

// WorkItemsUnitWindowDays returns the UTC days that the window of a work-items
// unit covers, ascending, or ErrInvalidConfiguration for a malformed window.
//
// It is the one day mapping of a unit window. The routes validate a unit with
// it, and the post-sync fan-out marks the days it returns as touched, so the
// days a unit is validated for and the days the daily job recomputes for it
// cannot drift.
//
//   - `before` is EXCLUSIVE: the last day is the day of the last instant
//     before it. A mid-day `before` keeps the day it partly covers.
//   - no `before`: the last day is the day of normalizedAt, the clock of the
//     run.
//   - no `since`: the window is the last day alone.
//   - `since` after `before`, a zero time, or more than
//     githubWorkItemDerivedMaxBackfillDays days: malformed.
func WorkItemsUnitWindowDays(since, before *time.Time, normalizedAt time.Time) ([]time.Time, error) {
	if normalizedAt.IsZero() {
		return nil, ErrInvalidConfiguration
	}
	endDay := githubWorkItemDerivedUTCDate(normalizedAt)
	if before != nil {
		if before.IsZero() {
			return nil, ErrInvalidConfiguration
		}
		endDay = githubWorkItemDerivedUTCDate(before.Add(-time.Nanosecond))
	}
	startDay := endDay
	if since != nil {
		if since.IsZero() {
			return nil, ErrInvalidConfiguration
		}
		startDay = githubWorkItemDerivedUTCDate(*since)
	}
	if startDay.After(endDay) {
		// A unit whose window is inverted is malformed, not an empty range.
		return nil, ErrInvalidConfiguration
	}
	days := make([]time.Time, 0, githubWorkItemDerivedMaxBackfillDays)
	for day := startDay; !day.After(endDay); day = day.AddDate(0, 0, 1) {
		if len(days) == githubWorkItemDerivedMaxBackfillDays {
			return nil, ErrInvalidConfiguration
		}
		days = append(days, day)
	}
	return days, nil
}
