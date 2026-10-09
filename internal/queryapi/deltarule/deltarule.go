// Package deltarule holds the one rule for a change between two windows:
// a delta is a statement about two measured values. A window with no stored
// value is served as a 0 placeholder, and 0 against a real value is a
// "fall of 100 %" that nothing measured. Home, /explain, the person summary and
// the operating review take their delta numbers from here, so a client reads
// one contract. There are three states:
//
//   - KindNone: a window has no stored value. Every delta number is 0 and the
//     presence flags carry the fact; nothing is said about a move.
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
	// Pct is the percent change: 0 for KindNone, nil for KindFromZero.
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
		return Delta{Kind: KindNone, Pct: &zero}
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

// Defined reports whether the delta carries a percent that is a statement:
// both windows measured and the prior not 0 (or both 0).
func (d Delta) Defined() bool { return d.Kind == KindPct }

// Sign is the direction of a delta of KindFromZero: +1 when the current value
// is above the measured 0, -1 when below. It is 0 for any other kind.
func Sign(d Delta, current float64) int {
	if d.Kind != KindFromZero {
		return 0
	}
	if current < 0 {
		return -1
	}
	return 1
}
