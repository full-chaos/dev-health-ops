package pythonparity

import "math"

// FloatJSON mirrors how Python's json module serializes a float.
//
// For a finite value this is exactly Repr: float.__repr__'s SHORTEST
// round-trip decimal digit string, with CPython's own fixed-vs-scientific
// notation threshold (see Repr's doc comment for why Go's 'g'/'f' verbs
// cannot be substituted here).
//
// json.dumps diverges from repr() only on the non-finite values, because
// NaN/Infinity/-Infinity are not valid JSON tokens. Python's own encoder
// still emits them by default (a non-spec `allow_nan=True` extension), which
// this function deliberately does NOT reproduce: it returns the JSON literal
// `null` instead, so every caller gets syntactically valid JSON regardless of
// whether the input was ever meant to be finite. A caller that must match
// Python's non-spec NaN/Infinity tokens byte-for-byte needs a different
// function; none of this repository's callers do.
func FloatJSON(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "null"
	}
	return Repr(value)
}
