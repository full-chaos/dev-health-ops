package analytics

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/timewindow"
)

// windowToday is the clock of every window of this package: the one UTC day of timewindow, as the REST readers use it.
// A test replaces it. TODO: the org's own timezone, where an org sets one, has no resolver in ops yet (D6083 names it).
var windowToday = func() time.Time { return time.Now().UTC() }

// windowEndExclusive is the end a request binds as {end_date}, for the work-unit reads (`to_ts < end`) and the daily
// reads (`day < end`) alike: the day after the last day the dateRange names, never later than today (D6101: the named end
// counts, inclusive, and a day that is not over is in no window, even when named). It is timewindow.ExclusiveEnd, the
// bound every REST reader's window has.
func windowEndExclusive(end graphqldate.Date) (graphqldate.Date, error) {
	exclusive, err := timewindow.ExclusiveEnd(end.Time(), windowToday())
	if err != nil {
		return graphqldate.Date{}, newValidationError("dateRange", nil, "dateRange.endDate is out of range")
	}
	return graphqldate.New(exclusive), nil
}

// dateRangeBounds is the start and the exclusive end a dateRange input binds.
func dateRangeBounds(dr *model.DateRangeInput) (start, endExclusive graphqldate.Date, err error) {
	endExclusive, err = windowEndExclusive(dr.EndDate)
	return dr.StartDate, endExclusive, err
}
