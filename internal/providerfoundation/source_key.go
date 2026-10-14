package providerfoundation

import "strings"

// NormalizeSourceKey mirrors discovery/repos.py::jira_key_norm's shape
// (.strip().lower()) as the ONE normalization implementation for every
// provider/external_id comparison in the scheduler and the sync verb. Comparison happens only in Go, never
// mirrored in SQL -- chris's ruling (CHAOS-4584 gate round 7, reaffirmed for
// CHAOS-4629) after two independent SQL-side normalization mirrors drifted
// from Python's jira_key_norm on real inputs.
//
// NOT Unicode-equivalent to Python's jira_key_norm for every input (CHAOS-4679,
// codex CHAOS-4629 gate round 6, executed): Python's str.lower() applies full
// Unicode SpecialCasing (e.g. U+0130 LATIN CAPITAL LETTER I WITH DOT ABOVE ->
// two codepoints, "i"+U+0307); Go's strings.ToLower does simple 1:1 rune
// mapping and collapses the same input to plain "i". Accepted as a narrow,
// documented limitation (chris/team-lead ruling) rather than pulling in
// golang.org/x/text/cases: exploiting it needs a literal U+0130 in a
// provider/external_id column, which no current write path produces, and
// the Python comparator this could diverge from is on a retiring plane --
// CHAOS-4679 tracks it and dissolves at Python retirement, when Go's own
// self-consistency (which already holds) is the only property that matters.
func NormalizeSourceKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
