// Package deltarule holds the one rule for a change between two windows:
// a delta is a statement about two measured values. A window with no stored
// value is served as a 0 placeholder, and 0 against a real value is a
// "fall of 100 %" that nothing measured. Home, /explain and the person summary
// take their delta numbers from here, and the operating review (operatingreview.go,
// dataIn) applies the same rule to its own weeks, so a client reads one contract.
// There are three states:
//
//   - KindNone: a window has no stored value. The percent is null (a percent
//     has no meaning against a value nobody measured; CHAOS-9111), the
//     absolute change is 0, and the presence flags carry the fact; nothing is
//     said about a move.
//   - KindFromZero: both windows are measured, the prior value is a measured 0
//     and the current value is not. A percent change against zero is undefined:
//     the percent is null (never 0 %, never "steady"), the direction is the
//     sign of the current value, and a sentence states the change in absolute
//     values.
//   - KindPct: both windows are measured and the percent is defined (a prior
//     that is not 0, or a prior and a current that are both 0: a true 0 %).
package deltarule

// Kind is the state of a delta.
type Kind uint8

const (
	// KindNone: a window has no stored value.
	KindNone Kind = iota
	// KindPct: both windows are measured and the percent is defined.
	KindPct
	// KindFromZero: both windows are measured, the prior is 0 and the current is not.
	KindFromZero
)

// Delta is the result of the rule.
type Delta struct {
	Kind Kind
	// Pct is the percent change: nil for KindNone and for KindFromZero.
	Pct *float64
}

// Complete reports whether both windows hold a stored value, the only case in
// which a delta (and anything derived from it: an event, a status, a
// sentence) states something.
func Complete(currentHasData, priorHasData bool) bool {
	return currentHasData && priorHasData
}

// Of applies the rule to a current and a prior value and the two presence flags.
func Of(current, previous float64, currentHasData, priorHasData bool) Delta {
	zero := 0.0
	if !Complete(currentHasData, priorHasData) {
		return Delta{Kind: KindNone}
	}
	if previous == 0 {
		if current == 0 {
			return Delta{Kind: KindPct, Pct: &zero}
		}
		return Delta{Kind: KindFromZero}
	}
	pct := (current - previous) / previous * 100.0
	return Delta{Kind: KindPct, Pct: &pct}
}

// Absolute is current - prior, or 0 when a window has no data.
func Absolute(current, prior float64, currentHasData, priorHasData bool) float64 {
	if !Complete(currentHasData, priorHasData) {
		return 0
	}
	return current - prior
}

// DriverPercentSQL is the ClickHouse expression of the delta between a driver's
// current and prior value, for the queries that rank drivers (the /explain
// drivers and the Home "driven by" lookup). It is the SQL form of Of: a driver
// with no row in the comparison window, or a NULL on either side, has no delta
// (NULL, never the 0 a LEFT JOIN default would give); a measured 0 against a
// measured 0 is a true 0; a measured 0 against a value has no percent (NULL);
// otherwise (current - previous) / previous * 100. current and previous are
// the two value expressions, previousPresent is a column that is 1 on a row
// of the previous side and 0 where the LEFT JOIN found none.
func DriverPercentSQL(current, previous, previousPresent string) string {
	return "CASE" +
		" WHEN " + current + " IS NULL OR " + previousPresent + " = 0 OR " + previous + " IS NULL THEN NULL" +
		" WHEN " + previous + " = 0 AND " + current + " = 0 THEN 0" +
		" WHEN " + previous + " = 0 THEN NULL" +
		" ELSE (" + current + " - " + previous + ") / " + previous + " * 100 END"
}
