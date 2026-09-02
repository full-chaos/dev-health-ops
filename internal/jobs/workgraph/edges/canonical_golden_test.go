package edges

import (
	"testing"
	"time"
)

// TestDerivationReproducesTheFrozenGolden is what PR1 was built for: the Go
// derivation is run over the 6,531 frozen dependency rows and must reproduce the
// 3,548 edges the deployed Python producer wrote from exactly those rows.
//
// Every field is compared, with ONE enumerated exception — the variant-C
// confidence policy (AssociativeConfidenceExceptions). Any other divergence
// fails, including one the exception list does not name.
//
// This is a per-row OUTCOME comparison, not a count: a row that Python emitted
// and Go skipped, or vice versa, is named individually. Counts would balance
// through exactly the substitution this test exists to catch.
func TestDerivationReproducesTheFrozenGolden(t *testing.T) {
	document := loadGolden(t)

	// Rebuild the producer's input in the order it saw it — order is load-bearing
	// because the dedup below is last-write-wins.
	rows := goldenDependencyRows(t, document)

	derived, outcomes := deriveForTest(t, rows)

	expected := map[string]Row{}
	for index, edge := range document.Edges {
		row, err := document.EdgeRow(edge)
		if err != nil {
			t.Fatalf("golden edge %d: %v", index, err)
		}
		expected[row.EdgeID] = row
	}

	// --- rows Python emitted that Go did not ---
	missing := 0
	for id, want := range expected {
		if _, ok := derived[id]; !ok {
			if missing < 3 {
				t.Errorf(
					"Go did not emit edge %s (%s:%s -%s-> %s:%s) that Python did",
					id[:12], want.SourceType, want.SourceID, want.EdgeType,
					want.TargetType, want.TargetID,
				)
			}
			missing++
		}
	}
	// --- rows Go emitted that Python did not ---
	extra := 0
	for id, got := range derived {
		if _, ok := expected[id]; !ok {
			if extra < 3 {
				t.Errorf(
					"Go emitted edge %s (%s:%s -%s-> %s:%s) that Python did not; outcome=%s",
					id[:12], got.SourceType, got.SourceID, got.EdgeType,
					got.TargetType, got.TargetID, outcomes[id],
				)
			}
			extra++
		}
	}
	if missing != 0 || extra != 0 {
		t.Fatalf("edge sets differ: %d missing from Go, %d extra in Go", missing, extra)
	}

	// --- field-by-field, with the one enumerated exception ---
	permittedDelta := map[string]GoldenException{}
	for _, exception := range AssociativeConfidenceExceptions {
		permittedDelta[exception.EdgeType] = exception
	}
	exercised := map[string]int{}
	for id, want := range expected {
		got := derived[id]
		if got.SourceType != want.SourceType || got.SourceID != want.SourceID ||
			got.TargetType != want.TargetType || got.TargetID != want.TargetID ||
			got.EdgeType != want.EdgeType {
			t.Fatalf("edge %s endpoints/type differ: got %+v want %+v", id[:12], got, want)
		}
		if got.Provenance != want.Provenance {
			t.Fatalf("edge %s provenance %q != %q", id[:12], got.Provenance, want.Provenance)
		}
		if got.Evidence != want.Evidence {
			t.Fatalf("edge %s evidence %q != %q", id[:12], got.Evidence, want.Evidence)
		}
		if !got.EventTs.Equal(want.EventTs) {
			t.Fatalf("edge %s event_ts %s != %s", id[:12], got.EventTs, want.EventTs)
		}
		if !got.Day.Equal(want.Day) {
			t.Fatalf("edge %s day %s != %s", id[:12], got.Day, want.Day)
		}
		if got.Confidence != want.Confidence {
			exception, named := permittedDelta[want.EdgeType]
			if !named {
				t.Fatalf(
					"edge %s confidence %v != %v and %q is not in the exception list",
					id[:12], got.Confidence, want.Confidence, want.EdgeType,
				)
			}
			if want.Confidence != exception.FromPy || got.Confidence != exception.ToGo {
				t.Fatalf(
					"edge %s confidence delta %v -> %v does not match the declared exception %v -> %v",
					id[:12], want.Confidence, got.Confidence, exception.FromPy, exception.ToGo,
				)
			}
			exercised[want.EdgeType]++
		}
	}
	if len(exercised) == 0 {
		t.Fatal("no confidence exception was exercised; the variant-C policy is not being applied")
	}
	t.Logf("%d edges reproduced; variant-C exception exercised by %s", len(expected), formatCounts(exercised))
}

// deriveForTest runs the derivation over frozen rows, returning the edges by id
// and the per-row outcome for each, so a divergence can name WHY rather than
// just that the sets differ.
func deriveForTest(t *testing.T, rows []DependencyRow) (map[string]Row, map[string]string) {
	t.Helper()
	document := loadGolden(t)
	buildClock := parseFrozenInstant(t, document.FrozenNow)
	result, err := DeriveIssueIssueEdges(rows, buildClock)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	byID := map[string]Row{}
	outcomes := map[string]string{}
	for _, edge := range result.Edges {
		byID[edge.EdgeID] = edge
		outcomes[edge.EdgeID] = "emitted"
	}
	return byID, outcomes
}

// TestEveryInputRowGetsExactlyOneOutcome is the completeness invariant that
// makes a per-row comparison possible at all.
//
// It is NOT a conservation check on totals — those balance through exactly the
// substitution this port is guarding against (a row counted under the wrong
// outcome). It asserts something weaker and more useful: no row is silently
// unaccounted for, so when Go and Python disagree the disagreement can be
// attributed to a named row with a named reason.
func TestEveryInputRowGetsExactlyOneOutcome(t *testing.T) {
	document := loadGolden(t)
	buildClock := parseFrozenInstant(t, document.FrozenNow)
	rows := goldenDependencyRows(t, document)

	result, err := DeriveIssueIssueEdges(rows, buildClock)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if len(result.Outcomes) != len(rows) {
		t.Fatalf("%d outcomes for %d rows — some row is unaccounted for", len(result.Outcomes), len(rows))
	}
	total := 0
	for _, count := range result.Counts {
		total += count
	}
	if total != len(rows) {
		t.Fatalf("outcome counts sum to %d, input had %d rows", total, len(rows))
	}
	for index, outcome := range result.Outcomes {
		if outcome == "" {
			t.Fatalf("input row %d has no outcome", index)
		}
	}
	if emitted := result.Counts[OutcomeEmitted]; emitted != len(result.Edges) {
		t.Fatalf("%d rows marked emitted but %d edges produced", emitted, len(result.Edges))
	}
	t.Logf("accounting over %d rows: %v", len(rows), result.Counts)
}

// goldenDependencyRows resolves the frozen intern indexes into rows. Shared so
// that a second decoder cannot drift from this one on field order — the six
// columns are all strings, so a transposition would type-check silently.
func goldenDependencyRows(t *testing.T, document *GoldenDocument) []DependencyRow {
	t.Helper()
	rows := make([]DependencyRow, 0, len(document.Dependencies))
	for index, frozen := range document.Dependencies {
		resolve := func(at int) string {
			value, err := document.String(frozen[at])
			if err != nil {
				t.Fatalf("dependency %d field %d: %v", index, at, err)
			}
			return value
		}
		rows = append(rows, DependencyRow{
			SourceWorkItemID: resolve(0), TargetWorkItemID: resolve(1),
			RelationshipType: resolve(2), RelationshipRaw: resolve(3),
			SemanticsVersion: resolve(4), LastSynced: parseFrozenInstant(t, resolve(5)),
		})
	}
	return rows
}

// parseFrozenInstant reads a timestamp out of the frozen golden.
//
// It accepts RFC3339Nano and NOTHING else, and that narrowness is the point.
// The golden is Go's own artefact -- its generator writes this format -- so
// there is no reference accept-set to match here. The production path does not
// parse timestamps at all any more: ReadDependencies hands the scanned
// time.Time straight to the derivation.
//
// This replaced a parser that modelled `datetime.fromisoformat`, which three
// codex rounds kept finding new shapes for. Keeping the string handling in the
// TEST, where the input is known, is what makes that class unreachable rather
// than merely enumerated.
func parseFrozenInstant(t *testing.T, value string) time.Time {
	t.Helper()
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("frozen instant %q is not RFC3339Nano — the golden generator writes that "+
			"format, so this is generator drift, not an input this port must tolerate: %v",
			value, err)
	}
	return parsed.UTC()
}
