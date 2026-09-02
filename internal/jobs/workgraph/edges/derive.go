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
	// MissingTimestamps counts rows that had NO last_synced, and whose event_ts
	// is therefore the build clock rather than the row's own time.
	//
	// This replaces a fallback counter that existed because the port could fail
	// to PARSE a timestamp Python read fine. That failure mode is gone -- the
	// value is an instant now, not text -- so the only remaining case is a row
	// that genuinely has no timestamp. It is still counted, because event_ts is
	// decided by merge state (CHAOS-4788) and a build-clock stamp on a row that
	// should have had its own time is worth seeing.
	MissingTimestamps int
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
			if reference.IsPR() {
				result.Outcomes[index] = OutcomeSkippedPRShaped
				result.Counts[OutcomeSkippedPRShaped]++
				prShaped = true
				break
			}
		}
		if prShaped {
			continue
		}

		eventTs, present := eventTimestamp(row.LastSynced, buildClock)
		if !present {
			result.MissingTimestamps++
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
			OrgID:        "", // never used: WriteEdges stamps the run's scope. See scope.go.
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

// eventTimestamp is Python's per-row event_ts handling (builder.py:886-895),
// reduced to what actually remains once the value is an instant rather than a
// string.
//
// Python's string arm (`fromisoformat`, and `except ValueError: self._now`)
// only runs when its driver hands back text. Its `if not event_ts` arm is dead
// for a different reason: `datetime` has no `__bool__`, so it fires only on a
// true None (audit gate 19). What is left on both planes is: use the row's
// instant; if there isn't one, use the build clock.
func eventTimestamp(lastSynced time.Time, buildClock time.Time) (time.Time, bool) {
	if lastSynced.IsZero() {
		return buildClock, false
	}
	return lastSynced.UTC(), true
}
