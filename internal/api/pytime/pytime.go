// Package pytime is the subset of Python datetime behaviour the api's
// stored values need: datetime.fromisoformat for the strings
// datetime.isoformat() writes, and pydantic's JSON form of a datetime.
package pytime

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DateTime is a Python datetime: Aware is false for a naive value, whose
// wall clock is kept in Time's UTC fields.
type DateTime struct {
	Time  time.Time
	Aware bool
	// Offset is the UTC offset in seconds of an aware value.
	Offset int
}

// FromISOFormat is datetime.fromisoformat for the extended forms
// isoformat() produces and their common variants: YYYY-MM-DD, then an
// optional separator ('T' or any single character) and HH[:MM[:SS[.f]]]
// with 1-6 fraction digits, then an optional "Z" or ±HH:MM[:SS[.ffffff]].
// ok is false where Python raises ValueError. The basic (no-dash) and
// week/ordinal forms fromisoformat also reads are refused (named limit).
func FromISOFormat(text string) (DateTime, bool) {
	if len(text) < 10 || text[4] != '-' || text[7] != '-' {
		return DateTime{}, false
	}
	year, e1 := strconv.Atoi(text[0:4])
	month, e2 := strconv.Atoi(text[5:7])
	day, e3 := strconv.Atoi(text[8:10])
	if e1 != nil || e2 != nil || e3 != nil || !digits(text[0:4]) || !digits(text[5:7]) || !digits(text[8:10]) {
		return DateTime{}, false
	}
	rest := text[10:]
	hour, minute, second, micro := 0, 0, 0, 0
	aware, offset := false, 0
	if rest != "" {
		if len(rest) < 3 {
			return DateTime{}, false
		}
		clock := rest[1:]
		tz := ""
		if index := strings.IndexAny(clock, "Z+-"); index >= 0 {
			clock, tz = clock[:index], clock[index:]
		}
		var ok bool
		if hour, minute, second, micro, ok = parseClock(clock); !ok {
			return DateTime{}, false
		}
		if tz != "" {
			if offset, ok = parseOffset(tz); !ok {
				return DateTime{}, false
			}
			aware = true
		}
	}
	if month < 1 || month > 12 || day < 1 || day > daysIn(year, month) || year < 1 || hour > 23 || minute > 59 || second > 59 {
		return DateTime{}, false
	}
	wall := time.Date(year, time.Month(month), day, hour, minute, second, micro*1000, time.UTC)
	if aware {
		wall = wall.Add(-time.Duration(offset) * time.Second)
	}
	return DateTime{Time: wall, Aware: aware, Offset: offset}, true
}

func digits(text string) bool {
	for _, r := range text {
		if r < '0' || r > '9' {
			return false
		}
	}
	return text != ""
}

func parseClock(clock string) (hour, minute, second, micro int, ok bool) {
	main, fraction, hasFraction := strings.Cut(clock, ".")
	if !hasFraction {
		main, fraction, hasFraction = strings.Cut(clock, ",")
	}
	parts := strings.Split(main, ":")
	if len(parts) > 3 {
		return 0, 0, 0, 0, false
	}
	values := []*int{&hour, &minute, &second}
	for index, part := range parts {
		if len(part) != 2 || !digits(part) {
			return 0, 0, 0, 0, false
		}
		*values[index], _ = strconv.Atoi(part)
	}
	if hasFraction {
		if len(parts) != 3 || fraction == "" || !digits(fraction) {
			return 0, 0, 0, 0, false
		}
		if len(fraction) > 6 {
			fraction = fraction[:6]
		}
		for len(fraction) < 6 {
			fraction += "0"
		}
		micro, _ = strconv.Atoi(fraction)
	}
	return hour, minute, second, micro, true
}

func parseOffset(tz string) (int, bool) {
	if tz == "Z" {
		return 0, true
	}
	sign := 1
	if tz[0] == '-' {
		sign = -1
	}
	hour, minute, second, micro, ok := parseClock(tz[1:])
	if !ok || micro != 0 || hour > 23 {
		return 0, false
	}
	return sign * (hour*3600 + minute*60 + second), true
}

func daysIn(year, month int) int {
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// Pydantic is pydantic's JSON form of the value: ISO date and time,
// ".ffffff" only when microseconds are non-zero, then "Z" for a zero
// offset, "±HH:MM" otherwise, nothing when naive.
func Pydantic(value DateTime) string {
	wall := value.Time.Add(time.Duration(value.Offset) * time.Second).UTC()
	text := wall.Format("2006-01-02T15:04:05")
	if micro := wall.Nanosecond() / 1000; micro != 0 {
		text += fmt.Sprintf(".%06d", micro)
	}
	if !value.Aware {
		return text
	}
	if value.Offset == 0 {
		return text + "Z"
	}
	sign, offset := '+', value.Offset
	if offset < 0 {
		sign, offset = '-', -offset
	}
	text += fmt.Sprintf("%c%02d:%02d", sign, offset/3600, offset%3600/60)
	if offset%60 != 0 {
		text += fmt.Sprintf(":%02d", offset%60)
	}
	return text
}

// UTC wraps an aware UTC instant (a timestamptz read from Postgres).
func UTC(at time.Time) DateTime {
	return DateTime{Time: at.UTC(), Aware: true}
}

// ISOFormat is datetime.isoformat() of an aware UTC value: microseconds
// only when non-zero, "+00:00".
func ISOFormat(at time.Time) string {
	at = at.UTC()
	text := at.Format("2006-01-02T15:04:05")
	if micro := at.Nanosecond() / 1000; micro != 0 {
		text += fmt.Sprintf(".%06d", micro)
	}
	return text + "+00:00"
}
