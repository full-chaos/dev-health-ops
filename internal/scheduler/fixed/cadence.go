// Package fixed owns the Go replacement for the legacy Celery Beat fixed
// maintenance schedules. It is deliberately separate from
// internal/scheduler/sync, which owns database-backed product schedules and
// their per-configuration cron expressions.
//
// The two packages solve different problems. Product schedules read a tenant
// supplied cron expression out of PostgreSQL and must reproduce croniter
// semantics exactly. Fixed schedules are checked into source with a small,
// closed set of cadences taken from the legacy Beat inventory, so this package
// intentionally implements a narrow deterministic cadence type instead of a
// second general cron parser. A cadence that cannot be expressed here is a
// contract change, not a runtime fallback.
package fixed

import (
	"errors"
	"fmt"
	"time"
)

// CadenceKind enumerates the closed cadence forms required by the checked-in
// Beat inventory. Adding a form is a reviewed contract change.
type CadenceKind string

const (
	// CadenceInterval fires on a fixed period aligned to the Unix epoch.
	CadenceInterval CadenceKind = "interval"
	// CadenceDaily fires once per day at a wall-clock hour and minute.
	CadenceDaily CadenceKind = "daily"
	// CadenceWeekly fires once per week on a weekday at a wall-clock time.
	CadenceWeekly CadenceKind = "weekly"
)

var (
	// ErrInvalidCadence identifies a declaration that cannot produce
	// deterministic occurrence times.
	ErrInvalidCadence = errors.New("fixed schedule cadence is invalid")
	// ErrInvalidTimezone identifies a schedule whose IANA zone cannot load.
	ErrInvalidTimezone = errors.New("fixed schedule timezone is invalid")
)

// minimumInterval keeps a declaration from degenerating into a busy loop and
// from producing more occurrence identities than a bounded window can carry.
const minimumInterval = 10 * time.Second

// Cadence is a deterministic, replica-independent schedule cadence. Every
// occurrence time it produces is a pure function of the cadence declaration,
// the schedule time zone, and the observation instant. No process-local start
// time participates, which is what allows two scheduler replicas to derive the
// same canonical due time without coordination.
type Cadence struct {
	Kind CadenceKind
	// Interval is the period for CadenceInterval.
	Interval time.Duration
	// Hour and Minute are the wall-clock fire time for daily and weekly forms.
	Hour   int
	Minute int
	// Weekday is the fire day for CadenceWeekly.
	Weekday time.Weekday
}

// EveryInterval declares an epoch-aligned period cadence.
//
// Celery Beat measures an interval schedule from the moment its own scheduler
// process last ran the entry, so two Beat processes would disagree about when
// a 300 second entry is due. Epoch alignment is the deterministic replacement:
// the boundary set is identical in every replica and across restarts, which is
// what TRD section 9.2 requires from a fixed occurrence key. The observable
// cadence (one occurrence per period) is unchanged.
func EveryInterval(interval time.Duration) Cadence {
	return Cadence{Kind: CadenceInterval, Interval: interval}
}

// DailyAt declares a once-per-day wall-clock cadence.
func DailyAt(hour, minute int) Cadence {
	return Cadence{Kind: CadenceDaily, Hour: hour, Minute: minute}
}

// WeeklyAt declares a once-per-week wall-clock cadence.
func WeeklyAt(weekday time.Weekday, hour, minute int) Cadence {
	return Cadence{Kind: CadenceWeekly, Weekday: weekday, Hour: hour, Minute: minute}
}

// Validate rejects a declaration that cannot produce bounded occurrences.
func (cadence Cadence) Validate() error {
	switch cadence.Kind {
	case CadenceInterval:
		if cadence.Interval < minimumInterval || cadence.Interval > 24*time.Hour {
			return fmt.Errorf("%w: interval %s is out of range", ErrInvalidCadence, cadence.Interval)
		}
		if cadence.Interval%time.Second != 0 {
			return fmt.Errorf("%w: interval must be a whole number of seconds", ErrInvalidCadence)
		}
		if cadence.Hour != 0 || cadence.Minute != 0 || cadence.Weekday != time.Sunday {
			return fmt.Errorf("%w: interval cadence carries wall-clock fields", ErrInvalidCadence)
		}
		return nil
	case CadenceDaily, CadenceWeekly:
		if cadence.Interval != 0 {
			return fmt.Errorf("%w: wall-clock cadence carries an interval", ErrInvalidCadence)
		}
		if cadence.Hour < 0 || cadence.Hour > 23 || cadence.Minute < 0 || cadence.Minute > 59 {
			return fmt.Errorf("%w: wall-clock time %02d:%02d is out of range", ErrInvalidCadence, cadence.Hour, cadence.Minute)
		}
		if cadence.Kind == CadenceDaily && cadence.Weekday != time.Sunday {
			return fmt.Errorf("%w: daily cadence carries a weekday", ErrInvalidCadence)
		}
		if cadence.Kind == CadenceWeekly && (cadence.Weekday < time.Sunday || cadence.Weekday > time.Saturday) {
			return fmt.Errorf("%w: weekday is out of range", ErrInvalidCadence)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidCadence, cadence.Kind)
	}
}

// Period is the nominal distance between two consecutive occurrences. It is
// used to bound uniqueness windows and missed-occurrence alerting, never to
// compute an occurrence time.
func (cadence Cadence) Period() time.Duration {
	switch cadence.Kind {
	case CadenceInterval:
		return cadence.Interval
	case CadenceDaily:
		return 24 * time.Hour
	case CadenceWeekly:
		return 7 * 24 * time.Hour
	default:
		return 0
	}
}

// Fingerprint is the stable comparison form used by the schedule-coverage test
// against the legacy Beat inventory. It is also embedded in nothing else: the
// occurrence key deliberately uses the schedule identity, not the cadence, so
// a cadence correction does not silently re-emit historical occurrences under
// new identities.
func (cadence Cadence) Fingerprint() string {
	switch cadence.Kind {
	case CadenceInterval:
		return fmt.Sprintf("interval:%ds", int(cadence.Interval/time.Second))
	case CadenceDaily:
		return fmt.Sprintf("cron:%d %d * * *", cadence.Minute, cadence.Hour)
	case CadenceWeekly:
		return fmt.Sprintf("cron:%d %d * * %d", cadence.Minute, cadence.Hour, int(cadence.Weekday))
	default:
		return "invalid"
	}
}

// Previous returns the most recent occurrence time at or before observedAt.
// The second result is false when the cadence is invalid.
func (cadence Cadence) Previous(observedAt time.Time, location *time.Location) (time.Time, bool) {
	if err := cadence.Validate(); err != nil || location == nil || observedAt.IsZero() {
		return time.Time{}, false
	}
	switch cadence.Kind {
	case CadenceInterval:
		seconds := int64(cadence.Interval / time.Second)
		unix := observedAt.UTC().Unix()
		// Floor division keeps pre-epoch instants monotonic; production clocks
		// never reach it, but a fixture must not silently invert.
		bucket := unix / seconds
		if unix < 0 && unix%seconds != 0 {
			bucket--
		}
		return time.Unix(bucket*seconds, 0).UTC(), true
	case CadenceDaily:
		local := observedAt.In(location)
		candidate := cadence.wallClock(local.Year(), local.Month(), local.Day(), location)
		if candidate.After(local) {
			candidate = cadence.wallClock(local.Year(), local.Month(), local.Day()-1, location)
		}
		return candidate.UTC(), true
	case CadenceWeekly:
		local := observedAt.In(location)
		offset := (int(local.Weekday()) - int(cadence.Weekday) + 7) % 7
		candidate := cadence.wallClock(local.Year(), local.Month(), local.Day()-offset, location)
		if candidate.After(local) {
			candidate = cadence.wallClock(local.Year(), local.Month(), local.Day()-offset-7, location)
		}
		return candidate.UTC(), true
	default:
		return time.Time{}, false
	}
}

// Next returns the first occurrence time strictly after observedAt.
func (cadence Cadence) Next(observedAt time.Time, location *time.Location) (time.Time, bool) {
	previous, ok := cadence.Previous(observedAt, location)
	if !ok {
		return time.Time{}, false
	}
	switch cadence.Kind {
	case CadenceInterval:
		return previous.Add(cadence.Interval), true
	case CadenceDaily:
		local := previous.In(location)
		return cadence.wallClock(local.Year(), local.Month(), local.Day()+1, location).UTC(), true
	case CadenceWeekly:
		local := previous.In(location)
		return cadence.wallClock(local.Year(), local.Month(), local.Day()+7, location).UTC(), true
	default:
		return time.Time{}, false
	}
}

// Between returns every occurrence time in the inclusive-exclusive range
// (after, throughInclusive] in ascending order, bounded by limit. It is the
// catch-up enumeration used after a scheduler outage. Returning more than
// limit entries is reported as truncation so a caller can alert instead of
// silently dropping the oldest missed occurrence.
func (cadence Cadence) Between(
	after time.Time,
	throughInclusive time.Time,
	location *time.Location,
	limit int,
) (occurrences []time.Time, truncated bool) {
	if err := cadence.Validate(); err != nil || location == nil || limit <= 0 {
		return nil, false
	}
	if !throughInclusive.After(after) {
		return nil, false
	}
	// Walk backwards from the newest occurrence so a truncated window keeps the
	// most recent work rather than stalling on the oldest.
	reversed := make([]time.Time, 0, limit)
	cursor, ok := cadence.Previous(throughInclusive, location)
	for ok && cursor.After(after) {
		if len(reversed) == limit {
			return reverseTimes(reversed), true
		}
		reversed = append(reversed, cursor)
		cursor, ok = cadence.Previous(cursor.Add(-time.Second), location)
	}
	return reverseTimes(reversed), false
}

func (cadence Cadence) wallClock(year int, month time.Month, day int, location *time.Location) time.Time {
	// time.Date normalizes out-of-range days, which is how the daily and weekly
	// forms step across month and year boundaries without a calendar table. It
	// also resolves a nonexistent or repeated DST wall clock deterministically.
	return time.Date(year, month, day, cadence.Hour, cadence.Minute, 0, 0, location)
}

func reverseTimes(values []time.Time) []time.Time {
	reversed := make([]time.Time, len(values))
	for index, value := range values {
		reversed[len(values)-1-index] = value
	}
	return reversed
}
