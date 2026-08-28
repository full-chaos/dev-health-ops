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

// Parse parses a "YYYY-MM-DD" string into a Date, matching Python's
// `datetime.date.fromisoformat` for this exact wire format (both reject a
// value carrying a time component or a non-hyphenated separator).
func Parse(s string) (Date, error) {
	t, err := time.Parse(Layout, s)
	if err != nil {
		return Date{}, fmt.Errorf("graphqldate: invalid Date %q: %w", s, err)
	}
	return Date(t), nil
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
