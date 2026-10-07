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
// The day mapping and the bound are githubWorkItemDerivedDays and its
// constant: one rule, in one place.
func workItemsUnitWindowDays(claim Claim, normalizedAt time.Time) ([]time.Time, error) {
	return githubWorkItemDerivedDays(claim, normalizedAt)
}
