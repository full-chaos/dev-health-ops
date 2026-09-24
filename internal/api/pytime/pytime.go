// Package pytime is the subset of Python datetime behaviour the api's
// stored values need: datetime.fromisoformat (every form Python 3.14
// reads), and pydantic's JSON form of a datetime.
package pytime

import (
	"fmt"
	"time"
)

// DateTime is a Python datetime: Aware is false for a naive value, whose
// wall clock is kept in Time's UTC fields.
type DateTime struct {
	Time  time.Time
	Aware bool
	// Offset is the UTC offset in seconds of an aware value, and
	// OffsetMicro its microseconds (the same sign): a Python timezone
	// carries a timedelta.
	Offset      int
	OffsetMicro int
}

func digits(text string) bool {
	for _, r := range text {
		if r < '0' || r > '9' {
			return false
		}
	}
	return text != ""
}

func daysIn(year, month int) int {
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// Pydantic is pydantic-core's JSON form of the value: ISO date and time,
// ".ffffff" only when microseconds are non-zero, then the offset (nothing
// when naive). pydantic-core rounds the offset to whole seconds, half away
// from zero; zero is "Z", anything else the sign and the whole hours and
// minutes ("±HH:MM", seconds dropped, so a non-zero offset under a minute
// is "+00:00" or "-00:00").
func Pydantic(value DateTime) string {
	offset := time.Duration(value.Offset)*time.Second + time.Duration(value.OffsetMicro)*time.Microsecond
	wall := value.Time.Add(offset).UTC()
	text := wall.Format("2006-01-02T15:04:05")
	if micro := wall.Nanosecond() / 1000; micro != 0 {
		text += fmt.Sprintf(".%06d", micro)
	}
	if !value.Aware {
		return text
	}
	micros := int64(offset / time.Microsecond)
	sign := '+'
	if micros < 0 {
		sign, micros = '-', -micros
	}
	seconds := (micros + 500_000) / 1_000_000
	if seconds == 0 {
		return text + "Z"
	}
	minutes := seconds / 60
	return text + fmt.Sprintf("%c%02d:%02d", sign, minutes/60, minutes%60)
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
