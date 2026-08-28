// Package graphqldate implements query-api's custom GraphQL `Date` scalar
// marshaler (CHAOS-4368 Wave 2). gqlgen.yml previously bound `Date` to
// `github.com/99designs/gqlgen/graphql.Time` as a Wave-0 placeholder,
// documented there as KNOWN INCORRECT for this scalar specifically:
// graphql.Time's UnmarshalGQL parses RFC3339Nano, which REJECTS a bare
// date value like "2026-08-27" (no time component), and its MarshalGQL
// emits a full timestamp, not a date -- neither matches what a real
// client sends/expects for `reviewEdges`'s `sinceDate`/`untilDate`/`day`
// fields.
//
// The correct wire format is confirmed against the actual Python producer,
// not assumed: Strawberry's built-in `Date` scalar (used for every
// `datetime.date`-typed field in the canonical schema, `contracts/graphql/
// v1/schema.graphql`'s `scalar Date`) serializes via
// `operator.methodcaller("isoformat")` on `datetime.date` -- i.e.
// `date.isoformat()`, which is always exactly "YYYY-MM-DD" (unlike
// `datetime.isoformat()`, `date.isoformat()` has no fractional-second or
// timezone-offset suffix to conditionally emit -- confirmed via
// strawberry.schema.types.scalar.DEFAULT_SCALAR_REGISTRY at the Python
// REPL: `datetime.date -> serialize=operator.methodcaller('isoformat')`).
// Its `parse_value` accepts the same "YYYY-MM-DD" string back
// (`datetime.date.fromisoformat`), which similarly rejects anything with a
// time component.
//
// Distinct Go type from graphql.Time deliberately: gqlgen's scalar binding
// is method-based (graphql.Marshaler/graphql.Unmarshaler), so it applies
// per-Go-type, not per-scalar-name -- `Date` and `DateTime` could not share
// graphql.Time's methods without also sharing its (still-placeholder,
// out-of-scope-for-this-port) DateTime behavior. See gqlgen.yml's model
// mapping doc comment for the DateTime/JSON scalars this port does NOT
// touch.
package graphqldate

import (
	"fmt"
	"io"
	"regexp"
	"strconv"
	"time"
)

// Layout is the wire format for the GraphQL `Date` scalar: "YYYY-MM-DD",
// matching Python's `date.isoformat()` / `date.fromisoformat()` exactly.
const Layout = "2006-01-02"

// Date is query-api's GraphQL `Date` scalar Go representation. The
// underlying time.Time always carries a zero time-of-day and UTC location
// -- constructing one from a ClickHouse `Date` column (which the native
// driver already returns as a Go time.Time at UTC midnight) needs no
// normalization; constructing one by parsing client input goes through
// New/UnmarshalGQL, which parse with Layout (no time component to carry).
type Date time.Time

// New wraps t as a Date, truncating to the UTC calendar date -- callers
// passing an already-midnight-UTC time.Time (e.g. a ClickHouse `Date`
// column scan) get an identical value back; this exists so a caller is
// never tempted to skip normalization for a time.Time that carries a
// non-midnight or non-UTC component (e.g. an object built in a non-UTC
// deployment locale).
func New(t time.Time) Date {
	t = t.UTC()
	return Date(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC))
}

// basicLayout is ISO 8601's "basic format" (no separators), e.g.
// "20260827" -- accepted by Python's `datetime.date.fromisoformat` since
// Python 3.11 alongside the canonical extended form.
const basicLayout = "20060102"

// isoWeekDateRE matches every ISO 8601 week-date form Python's
// `datetime.date.fromisoformat` accepts (verified empirically, codex
// review round 2, 2026-08-28 -- round 1's fix only handled the single
// extended-with-day form "YYYY-Www-D"): extended with day ("2026-W34-4"),
// extended reduced/no day ("2026-W34", defaults to Monday), basic with
// day ("2026W344"), and basic reduced ("2026W34"). Both the "-" before
// "W" and the "-" before the day digit are independently optional, which
// is exactly what lets one regex cover all four forms. NOTE: an ISO
// week-date's calendar date does not follow from its digits by simple
// substitution (e.g. "2026-W34-4" is 2026-08-20, not "26 Aug"-shaped) --
// see parseISOWeekDate.
var isoWeekDateRE = regexp.MustCompile(`^(\d{4})-?W(\d{2})(?:-?([1-7]))?$`)

// validYear reports whether t's year is in Python's `datetime.date`
// range, 1..9999 -- Go's time.Time has no such restriction (year 0 or
// year 10000 both construct without error), so every Parse success path
// must check this explicitly (codex review round 2: an earlier version
// let "00000101" and a week-date that rolls past year 9999 both succeed,
// where Python's fromisoformat rejects both with
// "year must be in 1..9999, not <n>").
func validYear(t time.Time) bool {
	y := t.Year()
	return y >= 1 && y <= 9999
}

// Parse parses a Date string, matching every wire form Python's
// `datetime.date.fromisoformat` actually accepts (confirmed empirically
// against the real interpreter, not assumed -- codex review, 2026-08-28,
// two rounds: round 1 added basic-format and single-form week-date
// acceptance; round 2 added the reduced/basic week-date variants, ISO
// week-count validation, and the year-range check, all found to diverge
// from Python by the same empirical-verification method). MarshalGQL/
// String always emit the canonical "YYYY-MM-DD" form regardless of which
// input form was parsed -- this widens what Parse ACCEPTS, it does not
// change what this type PRODUCES.
func Parse(s string) (Date, error) {
	if t, err := time.Parse(Layout, s); err == nil && validYear(t) {
		return Date(t), nil
	}
	if t, err := time.Parse(basicLayout, s); err == nil && validYear(t) {
		return Date(t), nil
	}
	if t, ok := parseISOWeekDate(s); ok {
		return Date(t), nil
	}
	return Date{}, fmt.Errorf("graphqldate: invalid Date %q", s)
}

// parseISOWeekDate parses ISO 8601 week-date notation into the calendar
// date it names. ISO 8601 defines week 1 of a year as the week containing
// that year's first Thursday (equivalently, the week containing January
// 4th) -- there is no Go stdlib layout for this, since it is not a simple
// positional substitution the way "2006-01-02" is. A day group absent
// from the input (the reduced form, "YYYY-Www") defaults to weekday 1
// (Monday), matching Python's fromisoformat.
//
// Returns ok=false for a week number the requested ISO YEAR does not
// actually have -- most years have 52 ISO weeks, but a year whose January
// 1st falls on a Thursday (or a leap year starting on Wednesday) has 53;
// "2025-W53-1" is invalid (2025 has 52) while "2026-W53-1" is valid.
// Rather than re-deriving that 52-vs-53-week rule by hand, this computes
// the candidate date via the same week1Monday arithmetic as any other
// week, then ROUND-TRIPS it through Go's own time.Time.ISOWeek() (the
// stdlib's already-correct implementation of that same rule) and requires
// the result to name back the exact (year, week) that was asked for --
// a week/year combination that does not exist lands the arithmetic on a
// different real ISO (year, week) (typically week 1 of the following ISO
// year), which this comparison catches without a second, hand-written
// copy of the leap/weekday rule to keep in sync with the stdlib's.
func parseISOWeekDate(s string) (time.Time, bool) {
	m := isoWeekDateRE.FindStringSubmatch(s)
	if m == nil {
		return time.Time{}, false
	}
	year, _ := strconv.Atoi(m[1])
	week, _ := strconv.Atoi(m[2])
	weekday := 1
	if m[3] != "" {
		weekday, _ = strconv.Atoi(m[3])
	}
	if week < 1 || week > 53 {
		return time.Time{}, false
	}
	jan4 := time.Date(year, time.January, 4, 0, 0, 0, 0, time.UTC)
	isoWeekday := int(jan4.Weekday()) // Sunday=0..Saturday=6
	if isoWeekday == 0 {
		isoWeekday = 7 // ISO 8601: Monday=1..Sunday=7
	}
	week1Monday := jan4.AddDate(0, 0, -(isoWeekday - 1))
	result := week1Monday.AddDate(0, 0, (week-1)*7+(weekday-1))

	gotYear, gotWeek := result.ISOWeek()
	if gotYear != year || gotWeek != week {
		return time.Time{}, false
	}
	if !validYear(result) {
		return time.Time{}, false
	}
	return result, true
}

// Time returns the underlying time.Time (UTC midnight on the date).
func (d Date) Time() time.Time { return time.Time(d) }

// String formats d as "YYYY-MM-DD".
func (d Date) String() string { return time.Time(d).Format(Layout) }

// MarshalGQL implements graphql.Marshaler.
func (d Date) MarshalGQL(w io.Writer) {
	_, _ = io.WriteString(w, strconv.Quote(d.String()))
}

// UnmarshalGQL implements graphql.Unmarshaler. Accepts only a string in
// "YYYY-MM-DD" form -- a client-sent timestamp (extra characters beyond
// the date) is rejected, matching Strawberry's `date.fromisoformat`
// rejecting the same malformed input on the Python side.
func (d *Date) UnmarshalGQL(v any) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("graphqldate: Date scalar must be a string, got %T", v)
	}
	parsed, err := Parse(s)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}
