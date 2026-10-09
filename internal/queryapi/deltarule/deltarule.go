// Package deltarule holds the one rule for a change between two windows:
// a delta is a statement about two measured values. A window with no stored
// value is served as a 0 placeholder, and 0 against a real value is a
// "fall of 100 %" that nothing measured. Home, /explain and the operating
// review take their delta numbers from here, so a client reads one contract:
// when either window has no data, every delta number is 0 and the presence
// flags carry the fact.
package deltarule

// Complete reports whether both windows hold a stored value, the only case in
// which a delta (and anything derived from it: an event, a status, a
// sentence) states something.
func Complete(currentHasData, priorHasData bool) bool {
	return currentHasData && priorHasData
}

// Absolute is current - prior, or 0 when a window has no data.
func Absolute(current, prior float64, currentHasData, priorHasData bool) float64 {
	if !Complete(currentHasData, priorHasData) {
		return 0
	}
	return current - prior
}

// Pct ports api/utils/numeric.py's delta_pct ((current - previous) / previous
// * 100, 0 for a zero previous), and is 0 when a window has no data.
func Pct(current, previous float64, currentHasData, priorHasData bool) float64 {
	if !Complete(currentHasData, priorHasData) || previous == 0 {
		return 0
	}
	return (current - previous) / previous * 100.0
}
