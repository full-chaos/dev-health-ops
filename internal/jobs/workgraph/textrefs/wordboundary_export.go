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
