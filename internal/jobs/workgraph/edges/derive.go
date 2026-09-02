package edges

import (
	"errors"
	"fmt"
	"strings"
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
	// TimestampFallbacks counts rows whose last_synced could not be parsed and
	// whose event_ts therefore became the BUILD clock instead.
	//
	// Python has the same fallback (`except ValueError: self._now`), but its
	// parser accepts far more than this one, so a fallback here can mean "the
	// value was junk" OR "the value was fine and this port could not read it".
	// Both silently produce a plausible row with a wrong event_ts, on the field
	// that is already the most delicate in this build (CHAOS-4788). Counting
	// them is what makes the second case discoverable at all.
	TimestampFallbacks int
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

		eventTs, err := eventTimestamp(row.LastSynced, buildClock)
		switch {
		case errors.Is(err, errFellBackToBuildClock):
			result.TimestampFallbacks++
		case err != nil:
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

// eventTimestamp is Python's per-row event_ts handling (builder.py:886-895).
//
// A string is parsed with `Z` normalised to `+00:00`; a parse failure falls back
// to the build clock rather than raising (Python swallows the ValueError, so a
// bad timestamp is indistinguishable from a missing one). A naive value is
// coerced to UTC.
func eventTimestamp(lastSynced string, buildClock time.Time) (time.Time, error) {
	if lastSynced == "" {
		// Python does not special-case this: `fromisoformat("")` raises, so an
		// empty value takes the SAME fallback arm as junk. Returning it without
		// the signal would produce the right instant and an undercount, which
		// is the more dangerous half — the whole point of the counter is to
		// reveal event_ts values that are not the row's real time.
		return buildClock, errFellBackToBuildClock
	}
	parsed, err := parseGoldenInstant(lastSynced)
	if err != nil {
		// Python's `except ValueError: event_ts = self._now`.
		return buildClock, errFellBackToBuildClock
	}
	return parsed, nil
}

// errFellBackToBuildClock is a signal, not a failure: the row is still emitted,
// exactly as Python emits it. It exists so the substitution can be counted
// rather than inferred from a timestamp that looks ordinary.
var errFellBackToBuildClock = errors.New("last_synced unparseable; event_ts fell back to the build clock")

// parseGoldenInstant models `datetime.fromisoformat(value.replace("Z","+00:00"))`
// (builder.py:886-893), coercing a naive value to UTC.
//
// # WHAT IS MODELLED, AND WHAT IS NOT
//
// `fromisoformat` accepts far more than RFC3339. Measured against the deployed
// interpreter (3.14), it takes basic format `20260901T123456+0000`, a lowercase
// `t`, ANY single character as the date/time separator, an offset without its
// colon, date-only and hour-only values, ISO week dates like `2026-W36-2`, and
// `24:00:00` rolling into the next day.
//
// This models the first five, by NORMALISING the input into layouts Go already
// parses rather than by hand-rolling an ISO parser -- a wrong parser would be
// worse than a narrow one. ISO week dates and the 24:00 rollover are
// deliberately NOT modelled; they are recorded in the timestamp corpus as known
// divergences (CHAOS-4818) rather than left to be rediscovered.
//
// The narrowness is safe for a reason that is worth stating rather than
// assuming: on the production path this value is not user data. ReadDependencies
// scans a time.Time from ClickHouse and formats it with RFC3339Nano itself, so
// the only strings that reach here are the ones this package produced. The wide
// accept set matters for frozen goldens and directly-constructed rows.
//
// Python truncates to MICROSECONDS; Go's RFC3339Nano keeps nanoseconds. That is
// a real value difference, so the result is truncated to match.
func parseGoldenInstant(value string) (time.Time, error) {
	// Python replaces EVERY "Z", not just a trailing one (builder.py:888).
	normalised := strings.ReplaceAll(value, "Z", "+00:00")
	for _, candidate := range isoFormatCandidates(normalised) {
		for _, layout := range isoLayouts {
			if parsed, err := time.Parse(layout, candidate); err == nil {
				// Naive -> UTC, as Python does at :892-893, then truncated to
				// Python's microsecond resolution.
				return parsed.UTC().Truncate(time.Microsecond), nil
			}
		}
	}
	return time.Time{}, fmt.Errorf("unparseable instant %q", value)
}

var isoLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04",
	"2006-01-02T15",
	"2006-01-02",
}

// isoFormatCandidates rewrites the shapes `fromisoformat` accepts into ones the
// layouts above cover. Normalising the INPUT keeps the parsing itself in the
// standard library, where it is already correct.
func isoFormatCandidates(value string) []string {
	candidates := []string{value}
	// Basic format: 20260901T123456 -> 2026-09-01T12:34:56. Only the date half
	// is unambiguous by length, so expand both halves together.
	if expanded, ok := expandBasicISO(value); ok {
		candidates = append(candidates, expanded)
	}
	expanded := make([]string, 0, len(candidates)*2)
	for _, candidate := range candidates {
		expanded = append(expanded, candidate)
		// Any single character may separate date and time; Go's layouts want T.
		if len(candidate) > 10 && candidate[10] != 'T' {
			expanded = append(expanded, candidate[:10]+"T"+candidate[11:])
		}
	}
	final := make([]string, 0, len(expanded)*2)
	for _, candidate := range expanded {
		final = append(final, candidate)
		// Offset without a colon: +0000 -> +00:00.
		if colonised, ok := colonizeOffset(candidate); ok {
			final = append(final, colonised)
		}
	}
	return final
}

func expandBasicISO(value string) (string, bool) {
	if len(value) < 8 {
		return "", false
	}
	for index := 0; index < 8; index++ {
		if value[index] < '0' || value[index] > '9' {
			return "", false
		}
	}
	date := value[:4] + "-" + value[4:6] + "-" + value[6:8]
	rest := value[8:]
	if rest == "" {
		return date, true
	}
	// 20260901T123456[offset]
	if len(rest) >= 7 && (rest[0] == 'T' || rest[0] == 't') {
		clock, tail := rest[1:], ""
		if len(clock) > 6 {
			clock, tail = rest[1:7], rest[7:]
		}
		if len(clock) == 6 {
			return date + "T" + clock[:2] + ":" + clock[2:4] + ":" + clock[4:6] + tail, true
		}
	}
	return date, true
}

func colonizeOffset(value string) (string, bool) {
	if len(value) < 5 {
		return "", false
	}
	tail := value[len(value)-5:]
	if tail[0] != '+' && tail[0] != '-' {
		return "", false
	}
	for _, index := range []int{1, 2, 3, 4} {
		if tail[index] < '0' || tail[index] > '9' {
			return "", false
		}
	}
	return value[:len(value)-5] + tail[:3] + ":" + tail[3:], true
}
