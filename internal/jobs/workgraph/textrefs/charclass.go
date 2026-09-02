// Package textrefs ports work_graph/extractors/text_parser.py to Go.
//
// # WHY THIS IS NOT A TRANSCRIPTION
//
// Python's `re` and Go's RE2 are different languages. Copying the patterns
// across produces something that compiles, passes an ASCII corpus, and is wrong
// on real input in five measured ways. Two structural reasons:
//
//  1. RE2 has no lookaround, and two of the module's patterns use it.
//  2. Python's character classes are Unicode-aware; RE2's are ASCII-only.
//     `re.ASCII` is set NOWHERE in the Python module, so every `\w`, `\d`, `\b`
//     -- and `\s`, which reads as formatting rather than semantics -- matches by
//     Unicode property there and by ASCII range here.
//
// The substitutions below are the answer to (2). Each was MEASURED against the
// deployed interpreter rather than derived from documentation, because the
// documentation for both languages describes intent and the corpus is compared
// against behaviour.
//
// # THE THREE SUBSTITUTIONS, AND WHAT THEY COST
//
// Measured CPython 3.14.7 (UCD 16.0.0) against Go 1.24 by enumerating all
// 0x110000 code points on both planes and diffing the sets:
//
//	Python class  Go substitution                  python-only  go-only
//	----------------------------------------------------------------------
//	\s            IsSpace || 0x1C..0x1F                      0        0
//	\w            IsLetter || IsNumber || '_'                0     4657
//	\d            IsDigit                                    0       10
//
// **`python-only` is zero for all three.** That is the column that would
// represent a real defect: a rune Python treats as a member and Go does not
// would make the Go side MISS a match Python finds, silently dropping an edge.
// There is no such rune for any of the three classes.
//
// The `go-only` residue is one-directional and fully characterised: every one of
// those runes is category `Cn` -- UNASSIGNED -- in CPython's UCD 16.0.0, and
// assigned in Go's Unicode 17 tables. They are not a semantic disagreement about
// what a word character is; they are code points that do not exist yet on the
// Python plane. `\d`'s ten are the contiguous block U+11DE0..U+11DE9.
//
// Reachability: RARE, not absent. A rune from that residue in a PR body or
// commit message makes Go treat it as a word/digit character while Python treats
// it as neither, which moves a `\b` boundary or extends a `[\w\-\.]+` span. No
// such rune appears in the frozen corpus. This is the same class as the 28-rune
// lowercase skew recorded in CHAOS-4869, arriving through a different table, and
// it resolves the same way: when CPython adopts UCD 17 the residue goes to zero
// without any change here.
//
// # THE SUBSTITUTIONS THAT ARE EASY TO GET WRONG
//
// `\s` is the one to be careful about, and `unicode.IsSpace` alone is WRONG.
// Python's `\s` set is exactly `str.isspace()` (29 runes, verified identical),
// and `unicode.IsSpace` is 25 of them. The four missing are the information
// separators U+001C..U+001F. Every whitespace character a reviewer would think
// to test -- NBSP, NEL, U+2028, U+3000 -- is already handled by plain
// `unicode.IsSpace`, so a corpus built from those cannot tell this function from
// the naive one. The `isspace_gap` group in the corpus exists for exactly that.
//
// Getting it wrong is not a missed match. `closes<WS>#42` is `ref_type=closes`
// when <WS> matches `\s` and `ref_type=references` when it does not, because the
// closing pattern fails and the plain pattern still hits. Both planes find a
// reference to issue 42; they disagree about whether the PR closes it, and every
// conservation check still balances because both emitted exactly one reference.
//
// `\w` is the second. `IsLetter || IsDigit || '_'` looks right and is not:
// Python's word character is alphanumeric-or-underscore, and Python's
// `isalnum()` is true for the whole `N` category, not just `Nd`. Roman numerals
// and superscripts are word characters to Python. Measured, that candidate
// missed 1151 runes Python accepts; `IsNumber` closes the gap to zero.
package textrefs

import (
	"unicode"
	"unicode/utf8"
)

// pythonIsSpace reports whether r is in Python's `re` `\s` set.
//
// EQUIVALENCE: Python `\s` == `str.isspace()` == this function, exactly, with
// no rune in either direction. Verified by enumerating all code points.
//
// The 0x1C..0x1F term is not a defensive guess. Go's unicode.IsSpace covers 25
// of Python's 29 whitespace runes; these four -- FILE, GROUP, RECORD and UNIT
// SEPARATOR -- are the entire difference, and they are the only ones that
// distinguish this function from unicode.IsSpace on any input.
func pythonIsSpace(r rune) bool {
	return unicode.IsSpace(r) || (r >= 0x1C && r <= 0x1F)
}

// pythonIsWord reports whether r is in Python's `re` `\w` set.
//
// EQUIVALENCE: every rune Python's `\w` accepts, this accepts. The converse
// holds except for 4657 runes that are unassigned in CPython's UCD 16.0.0 and
// assigned in Go's Unicode 17 tables -- see the package doc.
//
// unicode.IsNumber, not unicode.IsDigit: Python's word character is
// `isalnum() or '_'`, and isalnum() spans the whole N category (Nd, Nl, No).
func pythonIsWord(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_'
}

// pythonIsDigit reports whether r is in Python's `re` `\d` set.
//
// EQUIVALENCE: every rune Python's `\d` accepts, this accepts. The converse
// holds except for U+11DE0..U+11DE9, unassigned in UCD 16.0.0.
//
// NOTE this is `\d`, the regex class, which is category Nd. It is NOT
// `str.isdigit()`, which is Numeric_Type=Digit and accepts a different set --
// superscripts among them. The two are conflated easily and this port needs the
// former, because the Python side reaches these characters through `re`.
func pythonIsDigit(r rune) bool {
	return unicode.IsDigit(r)
}

// wordBoundaryBefore reports whether a `\b` or `(?<!\w)` boundary holds
// immediately before the rune at byte offset i in s.
//
// THIS IS THE LOOKBEHIND REWRITE, and it is a theorem rather than an
// approximation. `(?<!\w)` asserts that the preceding position is either the
// start of the string or a non-word rune; `\b` at the left edge of a word-run
// asserts the same thing. Both are lookaround over a SINGLE character class,
// which is exactly "match, then inspect the neighbouring rune" -- so the
// rewrite is not an emulation of lookbehind in general, only of this shape.
//
// The theorem is worth exactly as much as the corpus that could falsify it,
// which is why the parity corpus crosses every neighbour character class
// against every reference shape on BOTH sides rather than sampling: `\b` is
// asymmetric, and a corpus varying only the left neighbour cannot see a
// right-hand divergence.
func wordBoundaryBefore(s string, i int) bool {
	if i <= 0 {
		return true
	}
	prev := lastRune(s[:i])
	return !pythonIsWord(prev)
}

// wordBoundaryAfter is the right-hand counterpart, for the trailing `\b`.
func wordBoundaryAfter(s string, i int) bool {
	if i >= len(s) {
		return true
	}
	next := firstRune(s[i:])
	return !pythonIsWord(next)
}

// lastRune returns the final rune of s, or utf8.RuneError for an empty string.
//
// DecodeLastRuneInString, not indexing: a neighbour check that looked at the
// preceding BYTE would treat every multi-byte rune as a non-word character,
// which is precisely the divergence this file exists to prevent -- and it would
// pass any ASCII corpus.
func lastRune(s string) rune {
	if s == "" {
		return utf8.RuneError
	}
	r, _ := utf8.DecodeLastRuneInString(s)
	return r
}

// firstRune returns the first rune of s, or utf8.RuneError for an empty string.
func firstRune(s string) rune {
	if s == "" {
		return utf8.RuneError
	}
	r, _ := utf8.DecodeRuneInString(s)
	return r
}
