package goapiproof

import (
	"strings"
	"testing"
)

// This file pins drilldownPRsParity's copy-rule entry (restcorpus.go)
// against small, self-contained neutral fixtures built
// straight from the shape's own declaration, so a field ever added to or
// dropped from its field set is exercised here without needing a fresh
// production capture. Every fixture goes through the real REST decode
// path (DecodeRESTSnapshot, then InjectRESTDedupKeys), never a hand-built
// Go value. Baseline and candidate carry the SAME total item count in
// every fixture, exactly as the page-limit mechanism itself produces: the
// baseline spends one extra slot on a duplicate physical row, so its page
// never reaches an id the candidate's own, undeduplicated-length page
// does -- a bare item-count difference is a structural finding no shape
// in this package explains, so a length mismatch here would test nothing
// about the copy rule itself.

// prItem renders one synthetic drilldown/prs item over a single neutral
// identity (repo_id "ABC-123"); mergedAt, firstReviewAt and
// reviewLatencyHours are raw JSON ("null", a quoted timestamp, or a bare
// number).
func prItem(number, title, mergedAt, firstReviewAt, reviewLatencyHours string) string {
	return `{"repo_id":"ABC-123","number":` + number + `,"title":"` + title +
		`","author_name":"a1","created_at":"2024-01-01T00:00:00Z",` +
		`"merged_at":` + mergedAt + `,"first_review_at":` + firstReviewAt +
		`,"review_latency_hours":` + reviewLatencyHours + `}`
}

func prBody(items ...string) string {
	return `{"items":[` + strings.Join(items, ",") + `]}`
}

// prID is InjectRESTDedupKeys' key for ABC-123 / number.
func prID(number string) string {
	return "ABC-123" + restDedupKeySeparator + number
}

func prSnapshots(t *testing.T, baselineBody, candidateBody string) (Snapshot, Snapshot) {
	t.Helper()
	baseline := restSnapshotFromJSON(t, baselineBody)
	candidate := restSnapshotFromJSON(t, candidateBody)
	baseline.Data = InjectRESTDedupKeys(baseline.Data, "items", []string{"repo_id", "number"})
	candidate.Data = InjectRESTDedupKeys(candidate.Data, "items", []string{"repo_id", "number"})
	return baseline, candidate
}

// prWriteOnceOptions wraps the corpus's OWN declared copy-rule shape, both
// entries gated by the family's accounting over the three-row page every
// fixture in this file models,
// (drilldownPRsWriteOnceShape, drilldownprs_mergedat_realbody_test.go)
// alongside a plain duplicate-row sibling, mirroring drilldownPRsParity's
// own pairing: the write-once entry only ever judges a disagreeing id
// (workgraphedgedup.go's own doc comment), so a well-behaved shared or
// singleton id -- a normal, unrelated pull request sharing the same page
// -- needs the plain sibling to be admitted at all. These tests fail if
// the declaration's field set ever drifts, rather than pinning a second
// copy of it.
func prWriteOnceOptions(t *testing.T) Options {
	return Options{BaselineDefects: []BaselineDefect{
		{
			Ticket: "ABC-123", Reason: "test fixture",
			Paths: []string{"data.items"}, Intermittent: true, IntermittentReason: "test fixture",
			WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{EdgesListPath: "data.items", IDField: RESTDedupKeyField},
			Accounting:              pullRequestAccounting(3),
		},
		{
			Ticket: "ABC-456", Reason: "test fixture",
			Paths: []string{"data.items"}, Intermittent: true, IntermittentReason: "test fixture",
			WorkGraphEdgeDedupShape: drilldownPRsWriteOnceShape(t),
			Accounting:              pullRequestAccounting(3),
		},
	}}
}

// tailItem is a plain, agreeing, unique id every fixture below adds so
// baseline and candidate carry the same total item count -- see this
// file's own doc comment.
const tailItem = `{"repo_id":"ABC-123","number":9,"title":"tail","author_name":"a1","created_at":"2024-01-01T00:00:00Z","merged_at":null,"first_review_at":null,"review_latency_hours":null}`

// TestPRItemWriteOnce_AdmitsSingleLeafTransition: only merged_at
// transitions null -> populated; first_review_at and review_latency_hours
// agree null on both copies. The candidate carries the populated
// merged_at. Nothing stays outside.
func TestPRItemWriteOnce_AdmitsSingleLeafTransition(t *testing.T) {
	baseline := prBody(
		prItem("1", "t", "null", "null", "null"),
		prItem("1", "t", `"2024-01-02T05:00:00Z"`, "null", "null"),
		tailItem,
	)
	candidate := prBody(
		prItem("1", "t", `"2024-01-02T05:00:00Z"`, "null", "null"),
		tailItem,
		prItem("2", "u", "null", "null", "null"),
	)
	baselineSnap, candidateSnap := prSnapshots(t, baseline, candidate)
	result := Compare(baselineSnap, candidateSnap, prWriteOnceOptions(t))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"ABC-123", "ABC-456"}) {
		t.Fatalf("matched = %v, want [ABC-123 ABC-456]", result.BaselineDefectsMatched)
	}
}

// TestPRItemWriteOnce_AdmitsTwoLeavesInTheSameGroup: merged_at AND
// first_review_at (with its derived review_latency_hours) all transition
// together in the same duplicate group -- a pull request that was both
// merged and reviewed between the two physical writes.
func TestPRItemWriteOnce_AdmitsTwoLeavesInTheSameGroup(t *testing.T) {
	baseline := prBody(
		prItem("1", "t", "null", "null", "null"),
		prItem("1", "t", `"2024-01-02T05:00:00Z"`, `"2024-01-01T12:00:00Z"`, "12"),
		tailItem,
	)
	candidate := prBody(
		prItem("1", "t", `"2024-01-02T05:00:00Z"`, `"2024-01-01T12:00:00Z"`, "12"),
		tailItem,
		prItem("2", "u", "null", "null", "null"),
	)
	baselineSnap, candidateSnap := prSnapshots(t, baseline, candidate)
	result := Compare(baselineSnap, candidateSnap, prWriteOnceOptions(t))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestPRItemWriteOnce_ReproducesTheStillOpenCapturedCase reproduces the
// captured production shape for the still-open pull request: merged_at
// is null on EVERY copy (nothing to reconcile there), first_review_at
// and its derived review_latency_hours transition null -> populated. A
// declaration naming merged_at alone leaves this outside; the corpus's
// own declaration (naming all three) admits it, and merged_at is never
// named as the reason either way.
func TestPRItemWriteOnce_ReproducesTheStillOpenCapturedCase(t *testing.T) {
	baseline := prBody(
		prItem("1", "t", "null", "null", "null"),
		prItem("1", "t", "null", `"2024-01-01T18:00:00Z"`, "18"),
		tailItem,
	)
	candidate := prBody(
		prItem("1", "t", "null", `"2024-01-01T18:00:00Z"`, "18"),
		tailItem,
		prItem("2", "u", "null", "null", "null"),
	)
	baselineSnap, candidateSnap := prSnapshots(t, baseline, candidate)

	mergedAtOnly := Options{BaselineDefects: []BaselineDefect{
		{
			Ticket: "ABC-998", Reason: "test fixture",
			Paths: []string{"data.items"}, Intermittent: true, IntermittentReason: "test fixture",
			WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{EdgesListPath: "data.items", IDField: RESTDedupKeyField},
		},
		{
			Ticket: "ABC-999", Reason: "test fixture",
			Paths: []string{"data.items"}, Intermittent: true, IntermittentReason: "test fixture",
			WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{
				EdgesListPath: "data.items", IDField: RESTDedupKeyField,
				WriteOnceFields: []string{"merged_at"},
			},
		},
	}}
	narrow := Compare(baselineSnap, candidateSnap, mergedAtOnly)
	if narrow.DifferencesOutsideBaselineDefect == 0 {
		t.Fatal("outside = 0 under a merged_at-only declaration: this fixture must stay outside until first_review_at is named")
	}

	baselineSnap, candidateSnap = prSnapshots(t, baseline, candidate)
	full := Compare(baselineSnap, candidateSnap, prWriteOnceOptions(t))
	if full.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d under the corpus's own declaration, want 0 -- findings %+v", full.DifferencesOutsideBaselineDefect, full.Findings)
	}
	for _, f := range full.Findings {
		if strings.Contains(f.Detail, "write-once") && strings.Contains(f.Detail, `"merged_at"`) {
			t.Errorf("a still-open pull request (merged_at null on every copy) must never be cited as a merged_at disagreement: %s", f.Detail)
		}
	}
}

// TestPRItemWriteOnce_RefusesEachNeighbouringCase keeps the refusals the
// declaration must never blur: a candidate value no baseline copy
// carries, a candidate mixing fields of two copies, and a disagreement on
// a leaf outside the set.
func TestPRItemWriteOnce_RefusesEachNeighbouringCase(t *testing.T) {
	cases := []struct {
		name      string
		baseline  string
		candidate string
	}{
		{
			"candidate value no copy carries",
			prBody(
				prItem("1", "t", "null", "null", "null"),
				prItem("1", "t", "null", `"2024-01-01T10:00:00Z"`, "10"),
				prItem("1", "t", "null", `"2024-01-01T14:00:00Z"`, "14"),
			),
			prBody(
				prItem("1", "t", "null", `"2024-01-01T12:00:00Z"`, "12"),
				prItem("2", "u", "null", "null", "null"),
				tailItem,
			),
		},
		{
			"candidate mixes fields of two copies",
			prBody(
				prItem("1", "t", "null", "null", "null"),
				prItem("1", "retitled", `"2024-01-02T05:00:00Z"`, "null", "null"),
				tailItem,
			),
			prBody(
				prItem("1", "t", `"2024-01-02T05:00:00Z"`, "null", "null"),
				tailItem,
				prItem("2", "u", "null", "null", "null"),
			),
		},
		{
			"disagreement on a leaf outside the set",
			prBody(
				prItem("1", "t", "null", "null", "null"),
				strings.Replace(prItem("1", "t", "null", "null", "null"), `"created_at":"2024-01-01T00:00:00Z"`, `"created_at":"2024-01-01T00:00:01Z"`, 1),
				tailItem,
			),
			prBody(
				prItem("1", "t", "null", "null", "null"),
				tailItem,
				prItem("2", "u", "null", "null", "null"),
			),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baselineSnap, candidateSnap := prSnapshots(t, c.baseline, c.candidate)
			result := Compare(baselineSnap, candidateSnap, prWriteOnceOptions(t))
			if result.DifferencesOutsideBaselineDefect == 0 {
				t.Fatalf("outside = 0, want > 0 -- findings %+v", result.Findings)
			}
			quotedID := `"` + strings.ReplaceAll(prID("1"), restDedupKeySeparator, `\x1f`) + `"`
			named := false
			for _, f := range result.Findings {
				if strings.Contains(f.Detail, quotedID) {
					named = true
				}
			}
			if !named {
				t.Errorf("no finding names id 1 -- findings %+v", result.Findings)
			}
		})
	}
}

// TestPRItemWriteOnce_AdmitsTheOlderWholeCopy pins the declared blind
// spot: no writer gives a field a direction, so a candidate equal to the
// older physical copy (merged_at still null) equals one whole baseline
// copy and is admitted.
func TestPRItemWriteOnce_AdmitsTheOlderWholeCopy(t *testing.T) {
	baseline := prBody(
		prItem("1", "t", "null", "null", "null"),
		prItem("1", "t", `"2024-01-02T05:00:00Z"`, "null", "null"),
		tailItem,
	)
	candidate := prBody(
		prItem("1", "t", "null", "null", "null"),
		tailItem,
		prItem("2", "u", "null", "null", "null"),
	)
	baselineSnap, candidateSnap := prSnapshots(t, baseline, candidate)
	result := Compare(baselineSnap, candidateSnap, prWriteOnceOptions(t))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}
