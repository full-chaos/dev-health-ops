package edges

import "fmt"

// ProvenanceRankMultiplier scales a tier's rank above any millisecond
// timestamp: a version_rank is rank*multiplier + milliseconds, so a higher tier
// wins whatever the time, and within a tier the newer row wins. It is the
// constant of work_graph_issue_pr.version_rank (migration 084).
const ProvenanceRankMultiplier uint64 = 1125899906842624

// ProvenanceRank is a link tier's rank: native outranks explicit_text, which
// outranks heuristic. Inferred links are a fallback only. An unknown tier ranks 0.
func ProvenanceRank(provenance string) int {
	switch provenance {
	case ProvenanceNative:
		return 3
	case ProvenanceExplicitText:
		return 2
	case ProvenanceHeuristic:
		return 1
	}
	return 0
}

// ProvenanceVersionRankSQL is the MATERIALIZED expression of a version_rank
// column over a tier column and a timestamp column, built from ProvenanceRank
// so that no table spells the ranks again. The migrations are text, so a test
// compares each one with this expression.
func ProvenanceVersionRankSQL(tierColumn, timeColumn string) string {
	return fmt.Sprintf("((multiIf(%[1]s = 'native', %[3]d, %[1]s = 'explicit_text', %[4]d, %[1]s = 'heuristic', %[5]d, 0) + 1) * %[6]d) + toUnixTimestamp64Milli(%[2]s)",
		tierColumn, timeColumn,
		ProvenanceRank(ProvenanceNative), ProvenanceRank(ProvenanceExplicitText), ProvenanceRank(ProvenanceHeuristic),
		ProvenanceRankMultiplier)
}
