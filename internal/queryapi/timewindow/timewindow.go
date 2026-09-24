// Package timewindow ports the Python api's report window,
// api/services/filtering.py time_window (and services/people.py
// _time_window, the same arithmetic with no start or end date), with
// Python's date limits: a date is 0001-01-01..9999-12-31 and a timedelta
// holds at most 999999999 days, and passing either limit raises
// OverflowError. Every Python route computes the window inside its
// `except Exception: raise HTTPException(503, ...)`, so a route answers
// ErrOverflow with its own 503.
package timewindow

import (
	"errors"
	"time"
)

// ErrOverflow is the window's OverflowError: a day count past 999999999,
// or a date outside 0001-01-01..9999-12-31. A route answers it with its
// own 503, as the Python route's except clause does.
var ErrOverflow = errors.New("time window: date value out of range (Python OverflowError)")

// maxTimedeltaDays is datetime.timedelta.max.days.
const maxTimedeltaDays = 999999999

var (
	minDate = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxDate = time.Date(9999, time.December, 31, 0, 0, 0, 0, time.UTC)
)

// Window is time_window's (start_day, end_day, compare_start,
// compare_end), each a UTC midnight.
type Window struct {
	StartDay, EndDay, CompareStart, CompareEnd time.Time
}

// Compute is time_window: range_days and compare_days are raised to at
// least 1; end_day is the end date (today when nil) plus one day; the
// start is the start date (moved to end_day - 1 when not before end_day)
// or end_day - range_days; the comparison window is compare_days before
// the start. Each step fails where Python's raises. Day counts are Go ints:
// a caller that saturated a larger Python int past the int range still
// fails here, since any count past 999999999 does.
func Compute(rangeDays, compareDays int, startDate, endDate *time.Time, today time.Time) (Window, error) {
	rangeDays = max(1, rangeDays)
	compareDays = max(1, compareDays)
	end := midnight(today)
	if endDate != nil {
		end = midnight(*endDate)
	}
	endDay, err := addDays(end, 1)
	if err != nil {
		return Window{}, err
	}
	var startDay time.Time
	if startDate != nil {
		startDay = midnight(*startDate)
		if !startDay.Before(endDay) {
			if startDay, err = addDays(endDay, -1); err != nil {
				return Window{}, err
			}
		}
	} else if startDay, err = addDays(endDay, -rangeDays); err != nil {
		return Window{}, err
	}
	compareStart, err := addDays(startDay, -compareDays)
	if err != nil {
		return Window{}, err
	}
	return Window{StartDay: startDay, EndDay: endDay, CompareStart: compareStart, CompareEnd: startDay}, nil
}

// addDays is date + timedelta(days=days): the timedelta itself fails past
// 999999999 days, and the sum fails outside Python's date range.
func addDays(day time.Time, days int) (time.Time, error) {
	if days > maxTimedeltaDays || days < -maxTimedeltaDays {
		return time.Time{}, ErrOverflow
	}
	sum := day.AddDate(0, 0, days)
	if sum.Before(minDate) || sum.After(maxDate) {
		return time.Time{}, ErrOverflow
	}
	return sum, nil
}

func midnight(t time.Time) time.Time {
	year, month, day := t.UTC().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}
