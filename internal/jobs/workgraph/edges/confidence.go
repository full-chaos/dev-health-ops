package edges

import (
	"fmt"
	"math"
)

// CHAOS-4752 / CHAOS-4758 — the per-kind confidence policy for dependency-derived
// issue<->issue edges, and the ONE place this port diverges from Python.
//
// # WHY
//
// Python writes every work_item_dependencies-derived edge at 1.0 — the same tier
// as a PR's `implements` edge — so an ASSOCIATIVE tracker link is
// indistinguishable from a CAUSAL delivery link. That matters because the
// oversized-component split in units is confidence-ordered: its edge-drop phase
// drops only edges strictly BELOW a component's max confidence. With every edge
// tied at 1.0 that phase is near-vacuous, and the split falls through to hub
// removal, which DELETES the highest-degree node from every output fragment.
//
// Measured on the proof org: 242 of 4,859 nodes destroyed (240 issues, 2 PRs,
// ~10% of the graph). A PR whose linked issue was deleted became a one-node work
// unit with an empty `issues` evidence array and therefore no team bridge — the
// user-visible "Unassigned team" share on CHAOS-4752.
//
// Ranking the associative family strictly below the delivery tier restores the
// edge phase. Measured, same org, same builder: 242 -> 0 dropped nodes,
// 4,859/4,859 nodes present in the output, 1,832 components, 2,904 dropped edges,
// max component 147 against a cap of 150, and 199 of 199 previously-orphaned
// PR-only units reunited with their issue.
//
// This is a GROUPING-only downgrade: dropped edges stay in work_graph_edges for
// display and every other consumer — the same contract as the existing
// heuristic-provenance exclusion. Node removal, by contrast, is global and silent.
//
// It does NOT close CHAOS-4758. Any future graph whose max-confidence edges alone
// exceed the cap still reaches hub removal and still destroys nodes. This removes
// today's trigger; the partition fix (CHAOS-4771) removes the mechanism.
//
// parent_of / child_of deliberately stay at the delivery tier: a sub-issue
// hierarchy is structural containment, and grouping a parent with its children is
// the intended behaviour.
const (
	DeliveryConfidence    float32 = 1.0
	AssociativeConfidence float32 = 0.9
)

// AssociativeEdgeTypes is the family ranked below the delivery tier.
var AssociativeEdgeTypes = map[string]struct{}{
	EdgeTypeRelates:       {},
	EdgeTypeIsRelatedTo:   {},
	EdgeTypeBlocks:        {},
	EdgeTypeIsBlockedBy:   {},
	EdgeTypeDuplicates:    {},
	EdgeTypeIsDuplicateOf: {},
}

// DependencyConfidence is the confidence for a dependency-derived issue<->issue
// edge of the given kind.
func DependencyConfidence(edgeType string) float32 {
	if _, associative := AssociativeEdgeTypes[edgeType]; associative {
		return AssociativeConfidence
	}
	return DeliveryConfidence
}

// GoldenException names one permitted divergence from the frozen Python golden.
//
// The golden is generated from Python, which does not implement the policy above,
// so a plain equality assertion is impossible. Rather than weaken the comparison,
// the divergence is enumerated HERE as data and asserted field-by-field: the
// golden test requires every other field of every row to match byte-for-byte and
// fails on any confidence delta this list does not name. The deliberate change
// becomes the only thing the instrument permits.
type GoldenException struct {
	EdgeType string
	FromPy   float32
	ToGo     float32
	Reason   string
}

// AssociativeConfidenceExceptions is the complete, closed set of divergences
// between this port and the frozen Python golden. Adding to it is a deliberate
// act that must come with its own evidence; the test treats anything absent from
// it as a defect.
var AssociativeConfidenceExceptions = func() []GoldenException {
	reason := "CHAOS-4752/4758 variant C: associative links rank below delivery so the " +
		"oversized-component split drops EDGES instead of deleting NODES"
	exceptions := make([]GoldenException, 0, len(AssociativeEdgeTypes))
	for _, edgeType := range []string{
		EdgeTypeBlocks,
		EdgeTypeDuplicates,
		EdgeTypeIsBlockedBy,
		EdgeTypeIsDuplicateOf,
		EdgeTypeIsRelatedTo,
		EdgeTypeRelates,
	} {
		exceptions = append(exceptions, GoldenException{
			EdgeType: edgeType,
			FromPy:   DeliveryConfidence,
			ToGo:     AssociativeConfidence,
			Reason:   reason,
		})
	}
	return exceptions
}()

// Quantize narrows a confidence to the width of the ClickHouse column.
//
// work_graph_edges.confidence is Float32. A float64 0.9 written through the
// driver comes back as 0.89999997615814208984375, and the split's drop order is
// a sort on (confidence, edge_id) — so a value that was never narrowed sorts
// into a different tier than the identical value read back, and the two planes
// group nodes differently. Narrowing at the write boundary makes the round trip
// an identity.
//
// This is not hypothetical: the same quantisation changed the measured component
// count from 1,833 to 1,832 and produced a DIFFERENT component set on the proof
// org. A port that reproduces 1,833 has skipped this.
func Quantize(confidence float64) float32 {
	return float32(confidence)
}

// ValidateConfidence rejects a confidence that cannot be grouped.
//
// The column is a plain Float32 with no validating writer, so NaN and ±Inf are
// storable and readable today. A NaN is not merely odd: Python partitions a
// component's edges with two independent comprehensions rather than an if/else,
// so a NaN satisfies neither `>= max` nor `< max` and vanishes from grouping
// altogether; and a NaN reaching the max computation poisons it, after which no
// edge belongs to either partition and the component shatters into singletons
// (CHAOS-4441, commit ca0b40349, measured against the deployed Python builder).
//
// This writer therefore refuses to MINT one. Callers count the rejects rather
// than dropping them silently — a rejected edge is a real edge that will not be
// grouped, and that must be visible.
func ValidateConfidence(confidence float32) error {
	value := float64(confidence)
	if math.IsNaN(value) {
		return fmt.Errorf("confidence is NaN: %w", ErrUngroupableConfidence)
	}
	if math.IsInf(value, 0) {
		return fmt.Errorf("confidence is infinite (%v): %w", value, ErrUngroupableConfidence)
	}
	if value < 0 || value > 1 {
		return fmt.Errorf("confidence %v is outside [0, 1]: %w", value, ErrUngroupableConfidence)
	}
	return nil
}

// ErrUngroupableConfidence marks a confidence this writer refuses to emit.
var ErrUngroupableConfidence = fmt.Errorf("ungroupable confidence")

// ValidateConfidence's accept-set is Python's, not a stricter one we preferred.
//
// `WorkGraphEdge.__post_init__` (work_graph/models.py:125-128) raises unless
// `0.0 <= confidence <= 1.0`. NaN fails that chain because every comparison
// against NaN is false, so `not (False)` raises — meaning Python rejects NaN
// through the same guard rather than needing a separate check.
//
// Measured against the deployed dataclass rather than reasoned about:
//
//	NaN   -> ValueError      +Inf  -> ValueError
//	1.5   -> ValueError      -0.5  -> ValueError
//	0.9   -> accepted        0.0   -> accepted
//
// Recorded because a validator is exactly the place where "obviously correct"
// and "matches the reference" quietly part company: a stricter Go check would
// reject rows Python writes, and a looser one would mint rows Python refuses.
// Either is a divergence, and neither would fail a test that only asserted what
// the author thought reasonable. (Sibling lane, CHAOS-4757 round 1: its own test
// had asserted the wrong behaviour as desirable, so passing was not the same
// claim as matching.)
