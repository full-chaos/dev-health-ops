// Package linktier is the one source of the link provenance tiers and their
// ranks. It imports no first-party package, so a package can rank a link tier
// without taking on the work graph's dependencies.
package linktier

import "fmt"

const (
	Native       = "native"
	ExplicitText = "explicit_text"
	Heuristic    = "heuristic"
)

// RankMultiplier scales a tier's rank above any millisecond timestamp: a
// version_rank is rank*multiplier + milliseconds, so a higher tier wins
// whatever the time, and within a tier the newer row wins. It is the constant
// of work_graph_issue_pr.version_rank (migration 084).
const RankMultiplier uint64 = 1125899906842624

// Rank is a link tier's rank: native outranks explicit_text, which outranks
// heuristic. Inferred links are a fallback only. An unknown tier ranks 0.
func Rank(provenance string) int {
	switch provenance {
	case Native:
		return 3
	case ExplicitText:
		return 2
	case Heuristic:
		return 1
	}
	return 0
}

// VersionRankSQL is the MATERIALIZED expression of a version_rank column over
// a tier column and a timestamp column, built from Rank so that no table
// spells the ranks again. The migrations are text, so a test compares each
// one with this expression.
func VersionRankSQL(tierColumn, timeColumn string) string {
	return fmt.Sprintf("((multiIf(%[1]s = 'native', %[3]d, %[1]s = 'explicit_text', %[4]d, %[1]s = 'heuristic', %[5]d, 0) + 1) * %[6]d) + toUnixTimestamp64Milli(%[2]s)",
		tierColumn, timeColumn,
		Rank(Native), Rank(ExplicitText), Rank(Heuristic),
		RankMultiplier)
}
