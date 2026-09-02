package textrefs

import "unicode"

// pythonAtoi converts a run of `\d` characters the way Python's int() does.
//
// # WHY strconv.Atoi IS WRONG HERE
//
// The Python extractors match `(\d+)` and pass the captured text straight to
// int(). Both halves of that are Unicode-aware:
//
//	'merge pull request #٣٤'  -> extract_pr_refs = [34]    Arabic-Indic
//	'merge pull request #１２'  -> extract_pr_refs = [12]    fullwidth
//	'merge pull request #१२'  -> extract_pr_refs = [12]    Devanagari
//	'merge pull request #4٤'  -> extract_pr_refs = [44]    MIXED scripts
//
// Those are measured against the deployed module, not derived from the docs.
// The last one is the case that rules out every shortcut: Python does not
// require the digits to come from one script, so a per-script lookup table or a
// "detect the numeral system then convert" approach is wrong. int() maps each
// character to its decimal VALUE independently and accumulates.
//
// strconv.Atoi returns an error for all four of the non-ASCII forms. A port
// using it produces NO reference where Python produces PR 34 -- a silently
// dropped edge, and in the python-only direction, which is the one that loses
// data rather than inventing it.
//
// # HOW THE VALUE IS DERIVED
//
// Go's unicode package has no exported digit-value function. But every Nd block
// is by definition ten consecutive code points with values 0..9 in order, so a
// rune's value is its offset from the start of its own block. Walking back at
// most nine positions finds that start. This is exact for Nd rather than
// approximate, and TestPythonAtoiMatchesLivePythonForEveryDigit checks it
// against int() for every Nd rune the interpreter knows.
//
// ok is false when the run is empty or contains a non-digit -- which cannot
// happen for text captured by a `\d+` group, but the caller does not have to
// know that for the function to be safe.
func pythonAtoi(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	value := 0
	for _, r := range s {
		digit, ok := pythonDigitValue(r)
		if !ok {
			return 0, false
		}
		// The Python side has no width limit here, but a PR number that
		// overflows an int is not a PR number. Refusing is correct and is what
		// the caller's "not a reference" branch already handles; silently
		// wrapping would invent an edge pointing at an unrelated PR.
		if value > (1<<62)/10 {
			return 0, false
		}
		value = value*10 + digit
	}
	return value, true
}

// pythonDigitValue returns the decimal value of a single `\d` rune.
//
// # WHY NOT A WALK BACK TO THE BLOCK START
//
// The first implementation walked back at most nine positions to find the start
// of the rune's ten-digit block. That is wrong, and the exhaustive guard caught
// it on its first run: Nd blocks can be ADJACENT with no non-digit between
// them. U+1D7CE..U+1D7FF is five consecutive ten-digit blocks (Mathematical
// Bold, Double-Struck, Sans-Serif, Sans-Serif Bold, Monospace), so the walk
// sailed across the boundaries and returned 9 for every rune past the first
// block -- U+1D7F9 reported 9 where Python says 3.
//
// The range table is the authority instead. unicode.Nd's ranges are contiguous
// runs of decimal digits whose Lo is a zero, so the value is the offset from
// the containing range's Lo, modulo ten. That is exact across adjacent blocks
// because the modulus is what the walk was trying and failing to reconstruct.
func pythonDigitValue(r rune) (int, bool) {
	if !unicode.IsDigit(r) {
		return 0, false
	}
	for _, rng := range unicode.Nd.R16 {
		if rune(rng.Lo) <= r && r <= rune(rng.Hi) && rng.Stride == 1 {
			return int((r - rune(rng.Lo)) % 10), true
		}
	}
	for _, rng := range unicode.Nd.R32 {
		if rune(rng.Lo) <= r && r <= rune(rng.Hi) && rng.Stride == 1 {
			return int((r - rune(rng.Lo)) % 10), true
		}
	}
	return 0, false
}
