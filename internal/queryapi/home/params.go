// Filters is the subset of api/models/filters.py's MetricFilter this
// package's readers actually consume (time, scope, what.repos,
// why.work_category) -- the same "narrower than full Pydantic
// validation" boundary investment_explain_route.go's own filters
// handling documents, since request-body validation itself is handled
// by the shared validateMetricFilter (pydantic_metric_filter.go) before
// a Filters value is ever built.
package home

import "time"

// TimeFilter ports TimeFilter (api/models/filters.py:9-13).
type TimeFilter struct {
	RangeDays   int
	CompareDays int
	StartDate   *time.Time
	EndDate     *time.Time
}

// ScopeFilter ports ScopeFilter (api/models/filters.py:16-18).
type ScopeFilter struct {
	Level string
	IDs   []string
}

// WhatFilter ports the one field of WhatFilter
// (api/models/filters.py:26-29) this package reads.
type WhatFilter struct {
	Repos []string
}

// WhyFilter ports the one field of WhyFilter
// (api/models/filters.py:32-35) this package reads.
type WhyFilter struct {
	WorkCategory []string
}

// Filters ports the fields of MetricFilter (api/models/filters.py:44-50)
// this package's readers consume.
type Filters struct {
	Time  TimeFilter
	Scope ScopeFilter
	What  WhatFilter
	Why   WhyFilter
}

// DefaultFilters ports MetricFilter's own default_factory chain --
// TimeFilter(range_days=14, compare_days=14), ScopeFilter(level="org"),
// empty What/Why -- for a request whose body/query carries no
// overriding value at all.
func DefaultFilters() Filters {
	return Filters{
		Time:  TimeFilter{RangeDays: 14, CompareDays: 14},
		Scope: ScopeFilter{Level: "org"},
	}
}

// TimeWindow ports time_window (api/services/filtering.py:78-92)
// exactly: (start_day, end_day, compare_start, compare_end), each a UTC
// midnight instant.
func TimeWindow(f Filters, now time.Time) (startDay, endDay, compareStart, compareEnd time.Time) {
	rangeDays := f.Time.RangeDays
	if rangeDays < 1 {
		rangeDays = 1
	}
	compareDays := f.Time.CompareDays
	if compareDays < 1 {
		compareDays = 1
	}

	var endDate time.Time
	if f.Time.EndDate != nil {
		endDate = *f.Time.EndDate
	} else {
		endDate = now.UTC().Truncate(24 * time.Hour)
	}
	endDay = endDate.AddDate(0, 0, 1)

	if f.Time.StartDate != nil {
		startDay = *f.Time.StartDate
		if !startDay.Before(endDay) {
			startDay = endDay.AddDate(0, 0, -1)
		}
	} else {
		startDay = endDay.AddDate(0, 0, -rangeDays)
	}

	compareEnd = startDay
	compareStart = compareEnd.AddDate(0, 0, -compareDays)
	return startDay, endDay, compareStart, compareEnd
}
