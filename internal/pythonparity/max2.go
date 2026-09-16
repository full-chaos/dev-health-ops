package pythonparity

// Max2 replicates CPython's two-argument `max(a, b)` comparison order
// exactly: `a` is the running candidate and is replaced only on a strict
// "greater than" comparison against `b` -- never on a tie, and never when the
// comparison is unorderable (NaN). See Min2's doc comment for why the
// argument order, not just the two values, is part of the contract.
func Max2(a, b float64) float64 {
	if b > a {
		return b
	}
	return a
}
