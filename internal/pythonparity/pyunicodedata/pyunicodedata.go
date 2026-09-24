// Package pyunicodedata answers the unicodedata questions CPython 3.14
// (Unicode 16.0.0) answers, for code that must decide exactly as a Python
// validator does.
//
// Go's own tables (unicode, golang.org/x/text) follow a newer Unicode
// edition: a code point assigned in that edition is a letter or a mark to Go
// and still unassigned (Cn) to Python, and email-validator rejects an
// unassigned code point where Go would accept it. So the answers here come
// from frozen tables generated from the pinned interpreter (tables.go), and
// TestUnicodeDataTablesMatchLivePython regenerates them from the live interpreter and
// fails on any difference.
//
// Every function takes a code point, including a lone surrogate (Python's
// str can hold one; see pyjson.Runes).
package pyunicodedata

import "sort"

// UnidataVersion is unicodedata.unidata_version of the interpreter the
// tables were generated from.
const UnidataVersion = unidataVersion

// Category is unicodedata.category(chr(r)).
func Category(r rune) string {
	return categoryNames[categoryRuns[runIndex(len(categoryRuns), func(i int) rune { return categoryRuns[i].lo }, r)].value]
}

// Bidirectional is unicodedata.bidirectional(chr(r)); "" for a code point
// with no bidirectional class (Python returns "" too).
func Bidirectional(r rune) string {
	return bidiNames[bidiRuns[runIndex(len(bidiRuns), func(i int) rune { return bidiRuns[i].lo }, r)].value]
}

// Combining is unicodedata.combining(chr(r)).
func Combining(r rune) int {
	return int(combiningRuns[runIndex(len(combiningRuns), func(i int) rune { return combiningRuns[i].lo }, r)].value)
}

// HasName reports whether unicodedata.name(chr(r)) returns a name (it
// raises ValueError otherwise).
func HasName(r rune) bool {
	return !inRanges(noNameRanges[:], r)
}

// Name is unicodedata.name(chr(r)) for a code point whose category is a
// mark (M*), a separator (Z*) or other (C*): the only characters the email
// validator ever names in an error. ok is false when Python has no name
// for r or r is outside those categories.
func Name(r rune) (string, bool) {
	index := sort.Search(len(markSeparatorOtherNames), func(i int) bool { return markSeparatorOtherNames[i].r >= r })
	if index < len(markSeparatorOtherNames) && markSeparatorOtherNames[index].r == r {
		return markSeparatorOtherNames[index].name, true
	}
	return "", false
}

// DecompositionHasFullStop reports whether "002E" is one of the
// space-separated fields of unicodedata.decomposition(chr(r)).
func DecompositionHasFullStop(r rune) bool {
	index := sort.Search(len(fullStopDecompositions), func(i int) bool { return fullStopDecompositions[i] >= r })
	return index < len(fullStopDecompositions) && fullStopDecompositions[index] == r
}

// IsWord reports whether re.match(r"\w", chr(r)) matches (str pattern,
// default flags).
func IsWord(r rune) bool {
	return inRanges(wordRanges[:], r)
}

// runIndex returns the index of the run holding r: the last run whose
// start is <= r. Runs start at 0, so every code point has one.
func runIndex(n int, lo func(int) rune, r rune) int {
	return sort.Search(n, func(i int) bool { return lo(i) > r }) - 1
}

func inRanges(ranges [][2]rune, r rune) bool {
	index := sort.Search(len(ranges), func(i int) bool { return ranges[i][1] >= r })
	return index < len(ranges) && ranges[index][0] <= r
}

type run struct {
	lo    rune
	value uint8
}

type namedRune struct {
	r    rune
	name string
}
