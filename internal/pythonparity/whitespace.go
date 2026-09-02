package pythonparity

import (
	"strings"
	"unicode"
)

// IsSpace reports whether a code point is whitespace to CPython -- that is,
// whether `chr(r).isspace()` is True.
//
// Go's `unicode.IsSpace` is a strict SUBSET. Measured exhaustively over
// 0..0x10FFFF and frozen in tests/fixtures/python_whitespace_python_golden.json,
// the delta is exactly four code points, in one direction only:
//
//	0x1c FILE SEPARATOR    0x1d GROUP SEPARATOR
//	0x1e RECORD SEPARATOR  0x1f UNIT SEPARATOR
//
// Four characters is small enough to look like pedantry, and it has already
// caused one defect: RequireOrganizationScope("\x1c") accepted a tenant scope
// Python rejects, because strings.TrimSpace left it non-empty. Python raised
// before any fetch; Go proceeded into a silent zero-row run.
//
// # PYTHON HAS TWO WHITESPACE RULES AND THIS IS ONLY ONE OF THEM
//
// `str.strip()`, `str.split()` and `str.rstrip()` all use THIS predicate. But
// `int()` and `float()` do NOT -- they REJECT the four separators:
//
//	" 150".strip() -> "150"        int(" 150")     -> 150
//	int("\x1c150")  -> ValueError   float("\x1c1.5") -> ValueError
//
// Go's plain `strings.TrimSpace` happens to match the NUMERIC rule, which is
// why parsePythonInt and confidenceFromString correctly use it and must not be
// "unified for consistency" with this function. Doing so would break the
// parsers while fixing nothing.
func IsSpace(r rune) bool {
	// The four CPython treats as whitespace and Go does not.
	if r >= 0x1c && r <= 0x1f {
		return true
	}
	return unicode.IsSpace(r)
}

// Strip is CPython's `str.strip()` with no argument.
func Strip(value string) string {
	return strings.TrimFunc(value, IsSpace)
}

// LStrip is CPython's `str.lstrip()` with no argument.
func LStrip(value string) string {
	return strings.TrimLeftFunc(value, IsSpace)
}

// RStrip is CPython's `str.rstrip()` with no argument.
func RStrip(value string) string {
	return strings.TrimRightFunc(value, IsSpace)
}

// SplitWhitespace is CPython's `str.split()` with no argument: split on runs of
// whitespace, discarding empty fields, so leading and trailing whitespace
// produce no empty entries and a whitespace-only string yields nothing.
//
// `strings.Fields` is the obvious Go equivalent and is WRONG here for exactly
// the reason IsSpace documents: it uses unicode.IsSpace, so it does not split
// on 0x1c-0x1f and leaves them embedded in the output.
//
// That is not a cosmetic difference. This function backs CollapseWhitespace,
// which backs evidence text truncation, whose output is hashed into
// input_hash -- the LLM skip-existing key. Text that differs by one embedded
// separator byte hashes differently and re-categorizes the unit.
func SplitWhitespace(value string) []string {
	return strings.FieldsFunc(value, IsSpace)
}

// CollapseWhitespace is CPython's `" ".join(value.split())`: collapse every run
// of whitespace to a single space and trim the ends.
//
// Note that this NORMALISES the separator as well as the runs -- a tab, a
// non-breaking space and a unit separator all become U+0020 -- so it is lossy
// by design, matching the Python it mirrors.
func CollapseWhitespace(value string) string {
	return strings.Join(SplitWhitespace(value), " ")
}

// RuneLen is CPython's `len()` for a str: a count of CODE POINTS, not bytes.
//
// Go's `len()` on a string counts BYTES, and the two agree only for pure
// ASCII. Every truncation limit in the investment pipeline is expressed in
// CPython characters, so byte-based slicing silently keeps far less text than
// Python does -- measured at MAX_FIELD_CHARS=280, a byte-sliced port keeps 93
// CJK characters or 70 emoji where Python keeps 280 -- and can cut a rune in
// half, producing invalid UTF-8.
func RuneLen(value string) int {
	return len([]rune(value))
}

// TruncateRunes returns the first `limit` CODE POINTS of value, the equivalent
// of CPython's `value[:limit]`.
//
// Slicing by runes rather than bytes is the whole point: `value[:limit]` in Go
// would cut mid-sequence on any multi-byte character, and the resulting invalid
// UTF-8 would then be encoded as U+FFFD by MarshalPythonJSON -- a divergence
// that compounds rather than cancels.
func TruncateRunes(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
