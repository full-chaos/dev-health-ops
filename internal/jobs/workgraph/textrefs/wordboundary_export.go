package textrefs

// WordBoundaryBefore and WordBoundaryAfter expose wordBoundaryBefore/After
// (charclass.go) to other work-graph text-matching code (CHAOS-4924's
// operational-edges port) that reproduces a Python `\b`-anchored regex
// outside this package's own extractors. RE2's `\b` is ASCII-only where
// Python's is Unicode-aware, so any local `regexp.MustCompile` pattern using
// literal `\b` reproduces that same gap unless it re-checks boundaries with
// this pair -- pure passthrough, no new behaviour.
func WordBoundaryBefore(s string, i int) bool { return wordBoundaryBefore(s, i) }

// WordBoundaryAfter is the right-hand counterpart of WordBoundaryBefore.
func WordBoundaryAfter(s string, i int) bool { return wordBoundaryAfter(s, i) }

// DigitClass is an RE2 character-class BODY (usable inside `[...]`, already
// unwrapped -- see digitClass's own `"[" + pythonDigitClassRanges + "]"`)
// matching every code point Python's `\d` matches (ucdpin.go's
// pythonDigitClassRanges), exposed for CHAOS-4924's operational-edges port:
// its own local regexes (not textrefs' extractors) use bare `\d`, which in
// RE2 is ASCII-only where Python's `\d` accepts any Unicode decimal digit
// (Arabic-Indic, Devanagari, etc.). Use as `"[" + textrefs.DigitClass + "]+"`.
const DigitClass = pythonDigitClassRanges
