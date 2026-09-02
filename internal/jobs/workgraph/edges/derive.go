package edges

import (
	"fmt"
	"time"
)

// Outcome is what happened to one dependency row, and why.
//
// Per-row outcomes rather than counts, deliberately: a sibling port shipped a Go
// gate Python lacked, the row was still counted under a different outcome, and
// `read == written + rejected` balanced while the row sat in the wrong bucket.
// Every rejection here carries the reason that put it there.
type Outcome string

const (
	OutcomeEmitted         Outcome = "emitted"
	OutcomeDeduped         Outcome = "deduped"           // same edge_id, later row won
	OutcomeSkippedEmptyID  Outcome = "skipped_empty_id"  // builder.py:869
	OutcomeSkippedPRShaped Outcome = "skipped_pr_shaped" // builder.py:871-874
	OutcomeMalformedPRID   Outcome = "malformed_pr_id"   // the ruled divergence
)

// allOutcomes is the deriver's declared vocabulary, in a stable order.
//
// It exists so other code can be checked FOR EXHAUSTIVENESS against it rather
// than re-listing the constants and drifting. A Go switch over a string type
// has no compiler exhaustiveness check, so without this a sixth outcome would
// compile everywhere and simply be missing from the places that matter.
func zeroedCounts() map[Outcome]int {
	counts := make(map[Outcome]int, len(allOutcomes()))
	for _, outcome := range allOutcomes() {
		counts[outcome] = 0
	}
	return counts
}

func allOutcomes() []Outcome {
	return []Outcome{
		OutcomeEmitted, OutcomeDeduped, OutcomeSkippedEmptyID,
		OutcomeSkippedPRShaped, OutcomeMalformedPRID,
	}
}

// DeriveResult is the edges plus a full accounting of every input row.
type DeriveResult struct {
	Edges []Row
	// Outcomes[i] is the outcome of input row i. len == len(input), always:
	// every row is accounted for exactly once, which is what makes the per-row
	// comparison possible.
	Outcomes []Outcome
	Counts   map[Outcome]int
}

// DeriveIssueIssueEdges is the pure core of `_build_issue_issue_edges`
// (builder.py:828-919): dependency rows in, work_graph_edges rows out. No I/O,
// so it is testable against the frozen golden without a database.
//
// buildClock is the builder's construction time. It becomes discovered_at and
// last_synced on EVERY edge — those are per-BUILD, while event_ts is per-ROW.
// Reversing that changes every ReplacingMergeTree version.
func DeriveIssueIssueEdges(rows []DependencyRow, buildClock time.Time) (DeriveResult, error) {
	result := DeriveResult{
		Outcomes: make([]Outcome, len(rows)),
		// Every outcome is pre-registered at zero rather than appearing only
		// when it first occurs. An absent key and a zero key are different
		// facts -- "this never happened" versus "this is not being counted" --
		// and only the second is a defect, so they must not look alike. It also
		// makes the tally a total function over the vocabulary, which is what
		// lets the telemetry assert it partitions the rows read.
		Counts: zeroedCounts(),
	}

	// Insertion-ordered, last-write-wins — Python's
	// `{edge.edge_id: edge for edge in edges}` (:913). The map records the
	// position so a later duplicate replaces the earlier edge IN PLACE, keeping
	// first-seen order; a Go map iterated directly would produce a different
	// order every run and silently break the golden's footing.
	position := make(map[string]int, len(rows))
	edges := make([]Row, 0, len(rows))
	// emittedBy[slot] is the input-row index currently occupying that slot, so a
	// superseded row can be named rather than silently vanishing from the tally.
	emittedBy := map[int]int{}

	for index, row := range rows {
		source, target, edgeType := CanonicalDependency(row)

		if source == "" || target == "" {
			result.Outcomes[index] = OutcomeSkippedEmptyID
			result.Counts[OutcomeSkippedEmptyID]++
			continue
		}

		// A row whose EITHER endpoint is PR-shaped belongs to the issue<->PR
		// mapping writer, not here. This is the boundary where a divergence
		// costs a row from both pipelines at once.
		prShaped := false
		for _, endpoint := range [2]string{source, target} {
			reference, err := ParsePRDependencySource(endpoint)
			if err != nil {
				// The one ruled divergence: Python raises here and aborts the
				// whole build (CHAOS-4811); we reject this row, named and
				// counted, and carry on.
				result.Outcomes[index] = OutcomeMalformedPRID
				result.Counts[OutcomeMalformedPRID]++
				prShaped = true
				break
			}
			if reference.PRNumber != 0 {
				result.Outcomes[index] = OutcomeSkippedPRShaped
				result.Counts[OutcomeSkippedPRShaped]++
				prShaped = true
				break
			}
		}
		if prShaped {
			continue
		}

		eventTs, err := eventTimestamp(row.LastSynced, buildClock)
		if err != nil {
			return DeriveResult{}, fmt.Errorf("row %d: %w", index, err)
		}

		edge := Row{
			EdgeID:       EdgeID(NodeTypeIssue, source, edgeType, NodeTypeIssue, target),
			SourceType:   NodeTypeIssue,
			SourceID:     source,
			TargetType:   NodeTypeIssue,
			TargetID:     target,
			EdgeType:     edgeType,
			Provenance:   ProvenanceNative,
			Confidence:   DependencyConfidence(edgeType),
			Evidence:     EvidenceFor(row),
			DiscoveredAt: buildClock,
			LastSynced:   buildClock,
			EventTs:      eventTs,
			Day:          DayFor(eventTs),
			OrgID:        "", // stamped by the writer, which knows the scope
		}
		if err := ValidateConfidence(edge.Confidence); err != nil {
			return DeriveResult{}, fmt.Errorf("row %d: %w", index, err)
		}

		if at, seen := position[edge.EdgeID]; seen {
			// Last write wins. The row that previously held this slot is
			// re-marked `deduped`: it WAS emitted and then replaced, which is a
			// different fact from never having been emitted, and the accounting
			// has to say which — otherwise a dedup and a skip are
			// indistinguishable in the totals.
			loser := emittedBy[at]
			result.Outcomes[loser] = OutcomeDeduped
			result.Counts[OutcomeEmitted]--
			result.Counts[OutcomeDeduped]++

			edges[at] = edge
			emittedBy[at] = index
			result.Outcomes[index] = OutcomeEmitted
			result.Counts[OutcomeEmitted]++
			continue
		}
		position[edge.EdgeID] = len(edges)
		emittedBy[len(edges)] = index
		edges = append(edges, edge)
		result.Outcomes[index] = OutcomeEmitted
		result.Counts[OutcomeEmitted]++
	}

	result.Edges = edges
	return result, nil
}

// eventTimestamp is Python's per-row event_ts handling (builder.py:886-895).
//
// A string is parsed with `Z` normalised to `+00:00`; a parse failure falls back
// to the build clock rather than raising (Python swallows the ValueError, so a
// bad timestamp is indistinguishable from a missing one). A naive value is
// coerced to UTC.
func eventTimestamp(lastSynced string, buildClock time.Time) (time.Time, error) {
	if lastSynced == "" {
		return buildClock, nil
	}
	parsed, err := parseGoldenInstant(lastSynced)
	if err != nil {
		// Python's `except ValueError: event_ts = self._now`.
		return buildClock, nil
	}
	return parsed, nil
}

// parseGoldenInstant parses an RFC3339 instant, coercing a naive value to UTC —
// the same coercion Python applies at :892-893, and necessary because
// work_item_dependencies.last_synced is DateTime64(3) with NO timezone while
// work_graph_edges.last_synced is DateTime64(3,'UTC').
func parseGoldenInstant(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC(), nil
	}
	for _, layout := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil // naive -> UTC, as Python does
		}
	}
	return time.Time{}, fmt.Errorf("unparseable instant %q", value)
}
