package docstools

import (
	"fmt"
	"time"
)

// pdFromISOFormat is datetime.date.fromisoformat(s) of CPython 3.14 (its C parser): 7, 8 or 10 bytes of UTF-8, ASCII
// digits, YYYY-MM-DD, YYYYMMDD, YYYY-Www[-D] or YYYYWww[D]. Like the C parser it ignores bytes after the last field it
// reads ("20260716xx" is 2026-07-16). An invalid string is "Invalid isoformat string"; an impossible date is the
// ValueError of the date constructor.
func pdFromISOFormat(s string) (time.Time, error) {
	invalid := puValueError{"Invalid isoformat string: " + a1Repr(s)}
	n := len(s)
	if n != 7 && n != 8 && n != 10 {
		return time.Time{}, invalid
	}
	p := 0
	digits := func(k int) (int, bool) {
		v := 0
		for i := 0; i < k; i++ {
			if p >= n || s[p] < '0' || s[p] > '9' {
				return 0, false
			}
			v = v*10 + int(s[p]-'0')
			p++
		}
		return v, true
	}
	year, ok := digits(4)
	if !ok {
		return time.Time{}, invalid
	}
	sep := p < n && s[p] == '-'
	if sep {
		p++
	}
	if p < n && s[p] == 'W' {
		p++
		week, ok := digits(2)
		if !ok {
			return time.Time{}, invalid
		}
		day := 1
		if p < n {
			if sep {
				if s[p] != '-' {
					return time.Time{}, invalid
				}
				p++
			}
			if day, ok = digits(1); !ok {
				return time.Time{}, invalid
			}
		}
		if year < 1 || year > 9999 || day < 1 || day > 7 || !pdWeekOK(year, week) {
			return time.Time{}, invalid
		}
		jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		wd := (int(jan1.Weekday()) + 6) % 7
		monday := jan1.AddDate(0, 0, -wd)
		if wd > 3 {
			monday = monday.AddDate(0, 0, 7)
		}
		t := monday.AddDate(0, 0, (week-1)*7+day-1)
		if t.Year() < 1 || t.Year() > 9999 {
			return time.Time{}, puValueError{fmt.Sprintf("year must be in 1..9999, not %d", t.Year())}
		}
		return t, nil
	}
	month, ok := digits(2)
	if !ok {
		return time.Time{}, invalid
	}
	if sep {
		if p >= n || s[p] != '-' {
			return time.Time{}, invalid
		}
		p++
	}
	day, ok := digits(2)
	if !ok {
		return time.Time{}, invalid
	}
	if err := ylCheckDate(year, month, day); err != nil {
		return time.Time{}, puValueError{err.(ylError).msg}
	}
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
}

// pdWeekOK is week 1..52, or 53 in an ISO year that starts on a Thursday (or on a Wednesday in a leap year).
func pdWeekOK(year, week int) bool {
	if week >= 1 && week <= 52 {
		return true
	}
	if week != 53 {
		return false
	}
	wd := (int(time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC).Weekday()) + 6) % 7
	leap := year%4 == 0 && (year%100 != 0 || year%400 == 0)
	return wd == 3 || wd == 2 && leap
}

// pdISO is date.isoformat().
func pdISO(t time.Time) string { return t.Format("2006-01-02") }

// pdDays is (a - b).days for two dates (UTC midnights; no Duration, which overflows past 292 years).
func pdDays(a, b time.Time) int { return int((a.Unix() - b.Unix()) / 86400) }
