package pythonparity

// Min2 replicates CPython's two-argument `min(a, b)` comparison order
// exactly: `a` is the running candidate and is replaced only on a strict
// "less than" comparison against `b` -- never on a tie, and never when the
// comparison is unorderable (NaN).
//
// Go's `<` on NaN is always false, same as Python's, but CPython's algorithm
// starts from a SPECIFIC argument (the first one), not from an abstract
// "smaller value" -- so a NaN input silently resolves to whichever argument
// happens to sit first at the call site, not to NaN itself. A naive
// `if b < a { b } else { a }` gets this right only because it is written the
// same way; the risk is in believing `min` is symmetric and refactoring the
// argument order "for readability".
func Min2(a, b float64) float64 {
	if b < a {
		return b
	}
	return a
}
