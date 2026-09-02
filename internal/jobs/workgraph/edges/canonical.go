package edges

import "strings"

// DependencyRow is one `work_item_dependencies` row as the producer reads it.
//
// Every field is a value type: no slices, maps or pointers. That is deliberate —
// a recorded row that cannot alias anything makes the input-aliasing defect
// (CHAOS-4803) unrepresentable here rather than merely guarded against.
type DependencyRow struct {
	SourceWorkItemID string
	TargetWorkItemID string
	RelationshipType string
	RelationshipRaw  string
	SemanticsVersion string
	LastSynced       string // RFC3339; empty means the producer's parse fell back
}

// blockerTypes is Python's `_BLOCKER_TYPES` (builder.py:83).
var blockerTypes = map[string]struct{}{
	"blocks": {}, "blocked_by": {}, "is_blocked_by": {},
}

// dependencyTypeMap is Python's `DEPENDENCY_TYPE_MAP` (builder.py:64-75).
//
// NOTE `is_blocked_by` maps to IS_BLOCKED_BY here and that entry is UNREACHABLE:
// `is_blocked_by` is also in blockerTypes, so it never reaches this lookup. The
// entry is kept because the port replicates Python verbatim, and its
// unreachability is recorded as CHAOS-4812 item 1 rather than tidied away.
var dependencyTypeMap = map[string]string{
	"blocks":          EdgeTypeBlocks,
	"is_blocked_by":   EdgeTypeIsBlockedBy,
	"relates":         EdgeTypeRelates,
	"is_related_to":   EdgeTypeIsRelatedTo,
	"duplicates":      EdgeTypeDuplicates,
	"is_duplicate_of": EdgeTypeIsDuplicateOf,
	"parent":          EdgeTypeParentOf,
	"child":           EdgeTypeChildOf,
	"is_parent_of":    EdgeTypeParentOf,
	"is_child_of":     EdgeTypeChildOf,
}

// blockerProjectionRuleVersion is Python's BLOCKER_PROJECTION_RULE_VERSION (:82).
const blockerProjectionRuleVersion = "canonical-blocks.v2"

// CanonicalDependency is the Go twin of `_canonical_dependency` (builder.py:86-120).
//
// # THE BRANCH ORDER IS THE CONTRACT
//
// Six branches, and three of them exist ONLY to un-corrupt historical provider
// bugs — GitHub body parsing once encoded both directions backwards, and Jira
// inward links already put the blocker in source. A port that implements the
// obvious rule ("blocked_by means swap") corrupts real rows, because whether a
// swap is correct depends on which provider wrote the row and under which
// semantics version. Reordering these branches changes real edge directions.
//
// # TWO PYTHON DETAILS THAT ARE EASY TO LOSE
//
//  1. `relationship` and `raw` are lowercased; `semantics` is NOT (:97-99). So a
//     row carrying "Canonical-Blocks.V2" fails the equality at branch 2 and falls
//     through to the legacy heuristics — a wrong-direction edge that is still
//     counted as written. Replicated verbatim; recorded as CHAOS-4812 item 2.
//  2. Branch 3's `raw == relationship` compares raw against the ALREADY-LOWERED
//     relationship, so it only fires when raw was itself lowercase. Whitespace or
//     case noise in raw silently skips the intended flip and falls to a later
//     branch.
//
// Returns source, target and edge type. Only the type is derived from the
// relationship; the endpoints may be swapped.
func CanonicalDependency(row DependencyRow) (string, string, string) {
	source := row.SourceWorkItemID
	target := row.TargetWorkItemID
	relationship := pythonLower(row.RelationshipType)
	raw := pythonLower(row.RelationshipRaw)

	// `str(row.get(...) or "legacy.v1")`: empty and missing both become the
	// default, and they are indistinguishable afterwards (CHAOS-4812 context).
	semantics := row.SemanticsVersion
	if semantics == "" {
		semantics = "legacy.v1"
	}

	// 1. Not a blocker at all: type from the map, endpoints untouched.
	if _, isBlocker := blockerTypes[relationship]; !isBlocker {
		edgeType, known := dependencyTypeMap[relationship]
		if !known {
			// Python's `.get(relationship, EdgeType.RELATES)`. An unrecognised
			// type is silently written as `relates` rather than rejected —
			// counted as written, wrong bucket. Replicated; CHAOS-4812.
			edgeType = EdgeTypeRelates
		}
		return source, target, edgeType
	}

	// 2. Already canonical: trust the stored orientation.
	if semantics == blockerProjectionRuleVersion {
		if relationship == "blocks" {
			return source, target, EdgeTypeBlocks
		}
		return target, source, EdgeTypeBlocks
	}

	// 3. Historical GitHub body parsing encoded BOTH directions backwards, so
	//    this branch is the inverse of branch 2 — deliberately, not by mistake.
	if (strings.HasPrefix(source, "gh:") || strings.HasPrefix(source, "ghpr:")) && raw == relationship {
		if relationship == "blocks" {
			return target, source, EdgeTypeBlocks
		}
		return source, target, EdgeTypeBlocks
	}

	// 4. Historical Jira inward links already put the blocker in source.
	if strings.HasPrefix(source, "jira:") &&
		(relationship == "blocked_by" || relationship == "is_blocked_by") {
		return source, target, EdgeTypeBlocks
	}

	// 5. Remaining legacy inward links: swap.
	if relationship == "blocked_by" || relationship == "is_blocked_by" {
		return target, source, EdgeTypeBlocks
	}

	// 6. Legacy outward `blocks`: keep.
	return source, target, EdgeTypeBlocks
}

// EvidenceFor is Python's `rel_type_raw or rel_type or "dependency"` (:906).
//
// Python truthiness on strings, so an EMPTY raw falls through to the type, and
// an empty type falls through to the literal. Note this uses the RAW values, not
// the lowercased ones the canonicalisation works with.
func EvidenceFor(row DependencyRow) string {
	if row.RelationshipRaw != "" {
		return row.RelationshipRaw
	}
	if row.RelationshipType != "" {
		return row.RelationshipType
	}
	return "dependency"
}

// pythonLower is `str.lower()`, which is NOT `strings.ToLower`.
//
// Go applies SIMPLE case mapping: one rune in, one rune out. Python applies FULL
// case mapping, where a few runes lowercase to MORE than one rune. The two
// therefore disagree in both directions, and neither `strings.ToLower` nor an
// ASCII-only fold is correct on its own — measured against the live interpreter:
//
//	input           python .lower()   strings.ToLower   ASCII-only fold
//	"BLOCKS"        "blocks"          "blocks"          "blocks"
//	"İS_BLOCKED_BY" "i̇s_blocked_by"   "is_blocked_by"   "İs_blocked_by"
//	"BLOCKS" (U+212A KELVIN as the K)
//	                "blocks"          "blocks"          "blocKs"
//
// So `ToLower` wrongly folds `İS_BLOCKED_BY` INTO the blocker set that Python
// leaves out, and an ASCII fold wrongly leaves a Kelvin-sign `BLOCKS` OUT of the
// set Python puts it in. Either way a row lands in the wrong branch of
// CanonicalDependency, changing its edge type and its direction.
//
// The whole difference is the runes whose full lowering is longer than their
// simple lowering. In lowercase mapping that is U+0130 alone, which Python turns
// into "i" + U+0307 COMBINING DOT ABOVE. Expanding it first and then applying
// simple mapping reproduces Python for both cases above.
//
// TestPythonLowerMatchesLivePython re-derives the expansion set from the
// interpreter, so a Unicode revision that adds another cannot pass unnoticed.
func pythonLower(value string) string {
	if strings.ContainsRune(value, '\u0130') {
		value = strings.ReplaceAll(value, "\u0130", "i\u0307")
	}
	return strings.ToLower(value)
}
