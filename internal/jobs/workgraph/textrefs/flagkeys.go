package textrefs

import (
	"strings"
	"unicode/utf8"
)

// FlagKeyMinLength mirrors Python's FLAG_KEY_MIN_LENGTH.
const FlagKeyMinLength = 4

// ParsedFlagRef mirrors Python's ParsedFlagRef dataclass.
type ParsedFlagRef struct {
	FlagKey  string
	RawMatch string
}

// ExtractFlagKeyRefs mirrors extract_flag_key_refs.
//
// Feature-flag keys have no canonical shape, so a reference is recognised only
// by matching against the caller's registry of real keys. This is the sixth and
// last extractor, and it is the one with the most ways to look right and be
// wrong: three of Python's semantics here are NOT the ones a Go author reaches
// for by default, and each was confirmed with a separating input before this
// code existed.
//
// # TRAP 1: len(key) COUNTS RUNES, len(goString) COUNTS BYTES
//
// Python skips a key shorter than min_length. `len()` on a str is a count of
// CODE POINTS; Go's `len()` on a string is a count of BYTES. Separating input:
//
//	key "ab<emoji>"   Python len 3 -> SKIPPED
//	                  Go     len 6 -> would be PROCESSED
//
// So this counts runes with utf8.RuneCountInString. A byte length is wrong in
// the direction that surfaces a flag reference Python never surfaces.
//
// # TRAP 2: key.isdigit() IS NOT THE REGEX `\d`
//
// Python skips a purely-numeric key. `str.isdigit()` is Numeric_Type=Digit,
// which is a strict SUPERSET of the regex class `\d` (category Nd) by 128 runes
// -- superscripts U+00B2/B3/B9 and the Ethiopic digits among them. Measured
// against the interpreter: 760 in both, 128 isdigit-only, 0 `\d`-only.
//
//	key "<four superscript twos>"   Python isdigit() true -> SKIPPED
//	                                Nd-based predicate    -> would be PROCESSED
//
// So this uses pythonStrIsDigit, not pythonIsDigit. The two predicates read as
// interchangeable and are not; the package's other five extractors need the
// regex one, and this one needs the other.
//
// # TRAP 3: THE BOUNDARY IS LOOKAROUND ON BOTH SIDES
//
// Python builds a pattern per key:
//
//	(?<![\w./:-])<escaped key>(?![\w./:-])
//
// RE2 has neither lookbehind nor lookahead, so both sides are inspected in code
// -- the same rewrite as the plain-ref pattern, over a wider class. `re.escape`
// disappears entirely: this is a literal substring search plus two neighbour
// checks, so there is no pattern to escape and no metacharacter to get wrong.
//
// Note the boundary class is `\w` PLUS `.`, `/`, `:` and `-`, which is why
// "checkout-v2" is matched as a unit and never as the shorter "checkout": the
// hyphen is itself a boundary character, so "checkout" inside "checkout-v2"
// fails the right-hand check.
//
// # ORDER AND DEDUPLICATION
//
// Python iterates the REGISTRY, not the text, and returns keys in first-seen
// registry order rather than in order of appearance. It takes only the FIRST
// match per key (`pattern.search`, not `finditer`). Both are preserved: a port
// that scanned the text instead would return the same set in a different order.
func ExtractFlagKeyRefs(text string, knownFlagKeys []string, minLength int) []ParsedFlagRef {
	if text == "" {
		return nil
	}
	var out []ParsedFlagRef
	seen := map[string]bool{}
	for _, rawKey := range knownFlagKeys {
		// Python's str.strip(), which is the same 29-rune set as `\s`.
		key := strings.TrimFunc(rawKey, pythonIsSpace)
		if utf8.RuneCountInString(key) < minLength || allStrDigits(key) || seen[key] {
			continue
		}
		index := findFlagKey(text, key)
		if index < 0 {
			continue
		}
		seen[key] = true
		out = append(out, ParsedFlagRef{
			FlagKey:  key,
			RawMatch: text[index : index+len(key)],
		})
	}
	return out
}

// allStrDigits mirrors Python's str.isdigit() over a whole string.
//
// Python's isdigit() is FALSE for the empty string, which matters: an empty key
// would otherwise be treated as numeric and skipped for the wrong reason. It is
// skipped anyway by the length check, but the predicate should still mean what
// it says rather than being right by accident.
func allStrDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !pythonStrIsDigit(r) {
			return false
		}
	}
	return true
}

// findFlagKey returns the byte offset of the first occurrence of key in text
// that sits on flag-key boundaries, or -1.
//
// This is the lookaround rewrite: a literal search, then the neighbouring rune
// on each side. Scanning forward from each candidate rather than taking only
// the first literal hit is load-bearing -- Python's `search` finds the first
// position where the WHOLE pattern including both lookarounds holds, so a
// bounded-out first occurrence must not stop the search.
//
//	text "checkoutX checkout"  key "checkout"
//	  first literal hit at 0 fails the right-hand check;
//	  Python still matches at 10, and so must this.
func findFlagKey(text, key string) int {
	if key == "" {
		return -1
	}
	for offset := 0; ; {
		rel := strings.Index(text[offset:], key)
		if rel < 0 {
			return -1
		}
		start := offset + rel
		end := start + len(key)
		if !flagKeyBoundaryBefore(text, start) || !flagKeyBoundaryAfter(text, end) {
			// Advance by ONE BYTE past the start, not past the whole key: the
			// next valid match may overlap this one.
			offset = start + 1
			continue
		}
		return start
	}
}

// isFlagKeyBoundaryRune reports membership of Python's `[\w./:-]`.
func isFlagKeyBoundaryRune(r rune) bool {
	return pythonIsWord(r) || r == '.' || r == '/' || r == ':' || r == '-'
}

func flagKeyBoundaryBefore(s string, i int) bool {
	if i <= 0 {
		return true
	}
	return !isFlagKeyBoundaryRune(lastRune(s[:i]))
}

func flagKeyBoundaryAfter(s string, i int) bool {
	if i >= len(s) {
		return true
	}
	return !isFlagKeyBoundaryRune(firstRune(s[i:]))
}
