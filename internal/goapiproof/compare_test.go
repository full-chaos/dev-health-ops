package goapiproof

import (
	"errors"
	"strings"
	"testing"
)

// snapshotFromJSON is the tests' only way to build a Snapshot from a
// literal body, so every case exercises DecodeSnapshot's data-presence
// and number handling rather than a hand-built struct that could
// disagree with what a real response decodes to.
func snapshotFromJSON(t *testing.T, body string) Snapshot {
	t.Helper()
	snapshot, err := DecodeSnapshot([]byte(body))
	if err != nil {
		t.Fatalf("DecodeSnapshot(%s): %v", body, err)
	}
	return snapshot
}

func findingPaths(result Result) []string {
	paths := make([]string, 0, len(result.Findings))
	for _, finding := range result.Findings {
		paths = append(paths, finding.Kind+" "+finding.Path)
	}
	return paths
}

func TestCompareIdenticalResponsesMatch(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a","enabled":true,"rollout":12}]}}`
	result := Compare(snapshotFromJSON(t, body), snapshotFromJSON(t, body), Options{})
	if !result.IsMatch() {
		t.Fatalf("identical responses must match, got %s %v", result.TerminalState, findingPaths(result))
	}
}

// Parity rule 2: null and omission are different. This is the case a
// comparator that decodes into a plain map cannot see at all, because
// both sides become a nil value.
func TestCompareNullIsNotOmission(t *testing.T) {
	t.Run("inside data", func(t *testing.T) {
		baseline := snapshotFromJSON(t, `{"data":{"forecast":{"teamId":null}}}`)
		candidate := snapshotFromJSON(t, `{"data":{"forecast":{}}}`)
		result := Compare(baseline, candidate, Options{})
		if result.TerminalState != TerminalStateMismatch {
			t.Fatalf("null vs omission must mismatch, got %s", result.TerminalState)
		}
		if len(result.Findings) != 1 || result.Findings[0].Path != "$.data.forecast.teamId" {
			t.Fatalf("unexpected findings: %v", findingPaths(result))
		}
	})

	t.Run("at the envelope", func(t *testing.T) {
		baseline := snapshotFromJSON(t, `{"data":null}`)
		candidate := snapshotFromJSON(t, `{"errors":[{"message":"boom","path":["x"],"extensions":{"code":"E"}}]}`)
		result := Compare(baseline, candidate, Options{})
		if result.TerminalState != TerminalStateMismatch {
			t.Fatalf("data-present vs data-absent must mismatch, got %s", result.TerminalState)
		}
		var sawEnvelope bool
		for _, finding := range result.Findings {
			if finding.Path == "$.data" && finding.Detail == "present in baseline, absent in candidate" {
				sawEnvelope = true
			}
		}
		if !sawEnvelope {
			t.Fatalf("expected a $.data presence finding, got %v", findingPaths(result))
		}
	})
}

// Parity rule 2's stated exception: transport-envelope keys, at the top
// level of `data` ONLY.
func TestCompareEnvelopeKeyExceptionIsTopLevelOnly(t *testing.T) {
	opts := Options{EnvelopeKeys: map[string]bool{"extensions": true}}

	top := Compare(
		snapshotFromJSON(t, `{"data":{"extensions":{},"featureFlags":[]}}`),
		snapshotFromJSON(t, `{"data":{"featureFlags":[]}}`),
		opts,
	)
	if !top.IsMatch() {
		t.Fatalf("allowlisted top-level envelope key must be excused, got %v", findingPaths(top))
	}

	nested := Compare(
		snapshotFromJSON(t, `{"data":{"flags":{"extensions":{}}}}`),
		snapshotFromJSON(t, `{"data":{"flags":{}}}`),
		opts,
	)
	if nested.TerminalState != TerminalStateMismatch {
		t.Fatalf("a NESTED same-named key must not be excused, got %s %v", nested.TerminalState, findingPaths(nested))
	}
}

// Parity rule 3: Tier A is exact by default; Tier B is opt-in per field
// with a 1e-9 absolute/relative tolerance.
func TestCompareFloatTiers(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"hotspots":{"score":1.0000000004}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"hotspots":{"score":1.0000000005}}}`)

	tierA := Compare(baseline, candidate, Options{})
	if tierA.TerminalState != TerminalStateMismatch {
		t.Fatalf("Tier A is exact: a 1e-10 difference must mismatch, got %s", tierA.TerminalState)
	}

	tierB := Compare(baseline, candidate, Options{FloatTierB: map[string]string{"data.hotspots.score": "CHAOS-5451 merged Float64 aggregate"}})
	if !tierB.IsMatch() {
		t.Fatalf("Tier B must tolerate 1e-10, got %v", findingPaths(tierB))
	}

	tooFar := Compare(
		snapshotFromJSON(t, `{"data":{"hotspots":{"score":1.0}}}`),
		snapshotFromJSON(t, `{"data":{"hotspots":{"score":1.00001}}}`),
		Options{FloatTierB: map[string]string{"data.hotspots.score": "CHAOS-5451 merged Float64 aggregate"}},
	)
	if tooFar.TerminalState != TerminalStateMismatch {
		t.Fatalf("Tier B must still catch a 1e-5 difference, got %s", tooFar.TerminalState)
	}
}

// A Tier-B declaration is matched by the index-free path, so it applies
// to every element of a list without naming each index.
func TestCompareTierBPathIgnoresListIndices(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"edges":[{"score":1.0},{"score":2.0}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"edges":[{"score":1.0000000004},{"score":2.0000000006}]}}`)
	result := Compare(baseline, candidate, Options{FloatTierB: map[string]string{"data.edges.score": "CHAOS-5451 merged Float64 aggregate"}})
	if !result.IsMatch() {
		t.Fatalf("index-free Tier B path must cover every element, got %v", findingPaths(result))
	}
}

// Parity rule 3's non-finite clause: ALWAYS a mismatch, including when
// both sides agree. Decoding is what surfaces it -- see ErrNonFiniteNumber.
func TestCompareNonFiniteAlwaysMismatches(t *testing.T) {
	_, err := DecodeSnapshot([]byte(`{"data":{"x":NaN}}`))
	if !errors.Is(err, ErrNonFiniteNumber) {
		t.Fatalf("a bare NaN literal must be reported as non-finite, got %v", err)
	}
	if _, err := DecodeSnapshot([]byte(`{"data":{"x":-Infinity}}`)); !errors.Is(err, ErrNonFiniteNumber) {
		t.Fatalf("a bare -Infinity literal must be reported as non-finite, got %v", err)
	}

	// A string that merely CONTAINS the word must not trip the detector.
	if _, err := DecodeSnapshot([]byte(`{"data":{"x":"Infinity and beyond"}}`)); err != nil {
		t.Fatalf("a string containing 'Infinity' is ordinary data: %v", err)
	}

	// And a value that parses to a non-finite float still mismatches, even
	// when both sides carry the identical literal.
	overflow := snapshotFromJSON(t, `{"data":{"x":1e400}}`)
	result := Compare(overflow, overflow, Options{FloatTierB: map[string]string{"data.x": "CHAOS-5451 merged Float64 aggregate"}})
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("identical non-finite values must still mismatch, got %s", result.TerminalState)
	}
}

// Exact integer comparison must survive decoding: a decoder that widens
// to float64 loses this difference entirely.
func TestCompareLargeIntegersAreNotWidened(t *testing.T) {
	result := Compare(
		snapshotFromJSON(t, `{"data":{"count":9007199254740993}}`),
		snapshotFromJSON(t, `{"data":{"count":9007199254740992}}`),
		Options{},
	)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("two distinct int64s beyond float64 precision must mismatch, got %s", result.TerminalState)
	}
}

// Parity rule 1: errors compare by path + extensions.code; a message
// difference at the same identity is recorded drift, not a mismatch.
func TestCompareErrorsByPathAndCode(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"errors":[{"message":"nope","path":["a","b"],"extensions":{"code":"FORBIDDEN"}}]}`)
	candidate := snapshotFromJSON(t, `{"errors":[{"message":"not allowed","path":["a","b"],"extensions":{"code":"FORBIDDEN"}}]}`)
	drift := Compare(baseline, candidate, Options{})
	if !drift.IsMatch() {
		t.Fatalf("message-only drift must not block a match, got %s %v", drift.TerminalState, findingPaths(drift))
	}
	if len(drift.Findings) != 1 || drift.Findings[0].Kind != FindingErrorMessageDrift {
		t.Fatalf("message drift must still be RECORDED, got %v", findingPaths(drift))
	}

	differentCode := snapshotFromJSON(t, `{"errors":[{"message":"nope","path":["a","b"],"extensions":{"code":"INTERNAL"}}]}`)
	result := Compare(baseline, differentCode, Options{})
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("a different extensions.code is a different error, got %s", result.TerminalState)
	}
}

// Error ordering must not matter: identity is (path, code), and the two
// planes have no obligation to emit errors in the same order.
func TestCompareErrorsAreOrderIndependent(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"errors":[{"message":"m","path":["a"],"extensions":{"code":"X"}},{"message":"m","path":["b"],"extensions":{"code":"Y"}}]}`)
	candidate := snapshotFromJSON(t, `{"errors":[{"message":"m","path":["b"],"extensions":{"code":"Y"}},{"message":"m","path":["a"],"extensions":{"code":"X"}}]}`)
	if result := Compare(baseline, candidate, Options{}); !result.IsMatch() {
		t.Fatalf("error order must not matter, got %v", findingPaths(result))
	}
}

// Parity rule 4: a watermark delta short-circuits to `unsupported` and
// must neither produce nor HIDE a data mismatch.
func TestCompareWatermarkDriftShortCircuits(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"x":1}}`)
	baseline.Watermark = "2026-09-08T00:00:00Z"
	candidate := snapshotFromJSON(t, `{"data":{"x":2}}`)
	candidate.Watermark = "2026-09-09T00:00:00Z"

	result := Compare(baseline, candidate, Options{})
	if result.TerminalState != TerminalStateUnsupported {
		t.Fatalf("watermark drift must be unsupported, got %s", result.TerminalState)
	}
	for _, finding := range result.Findings {
		if finding.Kind == FindingMismatch {
			t.Fatalf("a watermark delta must not surface a data mismatch: %v", findingPaths(result))
		}
	}
}

func TestCompareRequiredWatermarkMissingIsUnsupported(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"x":1}}`)
	candidate := snapshotFromJSON(t, `{"data":{"x":1}}`)
	result := Compare(baseline, candidate, Options{RequireWatermark: true})
	if result.TerminalState != TerminalStateUnsupported {
		t.Fatalf("a watermark-bearing operation with no watermark must be unsupported, got %s", result.TerminalState)
	}
}

// The volatile-field exclusion: it excuses the declared field, and ONLY
// the declared field.
func TestCompareVolatileFieldsAreExcused(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"capacityForecast":{"forecastId":"78296c67","computedAt":"2026-09-07T10:17:39Z","backlogSize":8}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"capacityForecast":{"forecastId":"33fb9f32","computedAt":"2026-09-07T10:17:38Z","backlogSize":8}}}`)
	opts := Options{VolatileFields: map[string]string{
		"data.capacityForecast.forecastId": "freshly generated per request",
		"data.capacityForecast.computedAt": "freshly generated per request",
	}}

	result := Compare(baseline, candidate, opts)
	if !result.IsMatch() {
		t.Fatalf("declared volatile fields must be excused, got %v", findingPaths(result))
	}
	if len(result.UnusedExclusions) != 0 {
		t.Fatalf("both exclusions matched, expected none unused, got %v", result.UnusedExclusions)
	}

	// A real difference in an undeclared sibling still mismatches.
	drifted := snapshotFromJSON(t, `{"data":{"capacityForecast":{"forecastId":"33fb9f32","computedAt":"2026-09-07T10:17:38Z","backlogSize":9}}}`)
	if got := Compare(baseline, drifted, opts); got.TerminalState != TerminalStateMismatch {
		t.Fatalf("an undeclared field must still mismatch, got %s", got.TerminalState)
	}
}

// An exclusion that matches nothing is reported so the caller can fail
// the run: a misspelled path excuses NOTHING while reading as though it
// excuses something.
func TestCompareUnusedExclusionIsReported(t *testing.T) {
	body := `{"data":{"capacityForecast":{"backlogSize":8}}}`
	result := Compare(
		snapshotFromJSON(t, body),
		snapshotFromJSON(t, body),
		Options{VolatileFields: map[string]string{"data.capacityForecast.forcastId": "typo"}},
	)
	if len(result.UnusedExclusions) != 1 || result.UnusedExclusions[0] != "data.capacityForecast.forcastId" {
		t.Fatalf("a never-matched exclusion must be reported, got %v", result.UnusedExclusions)
	}
}

// Parity rule 5: positional ordering is the default, and this package
// deliberately ships no relaxation. An order-only difference is a loud
// mismatch, never silently tolerated.
func TestCompareListOrderingIsPositional(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"edges":[{"id":"a"},{"id":"b"}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"edges":[{"id":"b"},{"id":"a"}]}}`)
	result := Compare(baseline, candidate, Options{})
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("list order is part of the contract, got %s", result.TerminalState)
	}
}

func TestCompareListLengthMismatchReportsOnce(t *testing.T) {
	result := Compare(
		snapshotFromJSON(t, `{"data":{"edges":[{"id":"a"},{"id":"b"}]}}`),
		snapshotFromJSON(t, `{"data":{"edges":[{"id":"a"}]}}`),
		Options{},
	)
	if len(result.Findings) != 1 || !strings.Contains(result.Findings[0].Detail, "length 2 != 1") {
		t.Fatalf("a length difference must be reported once, got %v", result.Findings)
	}
}

// Type changes are mismatches, not coincidental equality: 1 is not "1"
// and not true.
func TestCompareTypeChangesMismatch(t *testing.T) {
	for _, testCase := range []struct{ name, baseline, candidate string }{
		{"number vs string", `{"data":{"x":1}}`, `{"data":{"x":"1"}}`},
		{"bool vs number", `{"data":{"x":true}}`, `{"data":{"x":1}}`},
		{"null vs string", `{"data":{"x":null}}`, `{"data":{"x":""}}`},
		{"object vs list", `{"data":{"x":{}}}`, `{"data":{"x":[]}}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result := Compare(snapshotFromJSON(t, testCase.baseline), snapshotFromJSON(t, testCase.candidate), Options{})
			if result.TerminalState != TerminalStateMismatch {
				t.Fatalf("expected mismatch, got %s", result.TerminalState)
			}
		})
	}
}

// An unparseable body is a failure of the MEASUREMENT, and must not be
// confusable with a comparable response -- D15/R4.
func TestDecodeSnapshotRejectsUnparseableBody(t *testing.T) {
	if _, err := DecodeSnapshot([]byte(`{"data":`)); err == nil {
		t.Fatal("a truncated body must fail to decode")
	} else if errors.Is(err, ErrNonFiniteNumber) {
		t.Fatalf("a truncated body is not a non-finite value: %v", err)
	}
}

// r3 P1 (reproduced): encoding/json substitutes U+FFFD (the replacement
// character) for a byte it cannot decode as UTF-8, rather than erroring.
// Without a whole-body validity check, a candidate carrying a genuinely
// invalid byte and a baseline that already carries a LITERAL replacement
// character decode to the identical Go value and would compare equal --
// the executed proof-runner reproduction confirmed this reaches
// production as a `match` verdict on a receipt actually written.
func TestDecodeSnapshotRefusesInvalidUTF8(t *testing.T) {
	if _, err := DecodeSnapshot([]byte("{\"data\":{\"commit\":\"b18e56\xff\"}}")); err == nil {
		t.Fatal("a body containing invalid UTF-8 must refuse, not silently substitute U+FFFD")
	}
}

// r4 P1 (reproduced): the reviewer's own executed repro was against this
// exact function -- a candidate body carrying `\ud800` and a baseline
// body carrying a literal `�` decoded to the SAME Go value and
// certified `match`, minting an enablement-eligible receipt from two
// genuinely different upstream responses.
func TestDecodeSnapshotRefusesUnpairedSurrogateEscape(t *testing.T) {
	if _, err := DecodeSnapshot([]byte(`{"data":{"commit":"\ud800x"}}`)); err == nil {
		t.Fatal("a body carrying an unpaired UTF-16 surrogate escape must refuse, not silently collapse to U+FFFD")
	}
}

// The collapse itself, proven directly: without the guard these two
// bodies would compare EQUAL even though a real client (Python's
// json.loads preserves the lone surrogate) would see them as different.
func TestDecodeSnapshotUnpairedSurrogateDoesNotMaskAsAMatchingBaseline(t *testing.T) {
	candidate := []byte(`{"data":{"commit":"\ud800x"}}`)
	baseline := []byte(`{"data":{"commit":"�x"}}`)
	if _, err := DecodeSnapshot(candidate); err == nil {
		t.Fatal("the candidate leg must refuse before it can be compared against anything")
	}
	// The baseline (a literal, genuinely valid U+FFFD) must still decode
	// fine on its own -- this guard is about the ESCAPE, not the
	// character it would otherwise be confused for.
	if _, err := DecodeSnapshot(baseline); err != nil {
		t.Fatalf("a literal U+FFFD is valid UTF-8 and must decode: %v", err)
	}
}

// r3 P1 (team-lead's decoder sweep): DecodeSnapshot's envelope is
// already a map[string]json.RawMessage keyed by exact string, so a
// "Data"/"DATA" sibling of "data" cannot shadow it -- proven here
// rather than assumed from the mechanism alone, the same discipline
// applied to every other decoder in this package.
func TestDecodeSnapshotDataFieldIsNeverShadowedByADifferentlyCasedKey(t *testing.T) {
	snapshot, err := DecodeSnapshot([]byte(`{"data":{"real":true},"Data":{"tampered":true}}`))
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	got, ok := snapshot.Data.(map[string]any)
	if !ok {
		t.Fatalf("snapshot.Data is not a map: %#v", snapshot.Data)
	}
	if _, tampered := got["tampered"]; tampered {
		t.Fatalf("the shadow-cased \"Data\" key was read instead of the exact \"data\" one: %#v", got)
	}
	if _, real := got["real"]; !real {
		t.Fatalf("the exact \"data\" key's own content is missing: %#v", got)
	}
}

// An unrecognised top-level key in the envelope must not refuse the
// whole response -- only "data" and "errors" are ever read.
func TestDecodeSnapshotAcceptsAnUnknownEnvelopeKey(t *testing.T) {
	snapshot, err := DecodeSnapshot([]byte(`{"data":{"x":1},"extensions":{"anything":"here"}}`))
	if err != nil {
		t.Fatalf("an unrecognised envelope key must not refuse: %v", err)
	}
	if !snapshot.DataPresent {
		t.Fatal("data must still be read")
	}
}

// The masking case, made concrete: a genuinely invalid byte and an
// already-replacement-charactered baseline must NOT decode to Snapshots
// that compare as a match, which is exactly what happened before the
// UTF-8 gate above existed.
func TestDecodeSnapshotInvalidUTF8DoesNotMaskAsAMatchingBaseline(t *testing.T) {
	_, candidateErr := DecodeSnapshot([]byte("{\"data\":{\"commit\":\"b18e56\xff\"}}"))
	if candidateErr == nil {
		t.Fatal("the candidate leg must refuse before it can be compared at all")
	}
	// The baseline leg (valid UTF-8, already carrying the literal
	// replacement character) must still decode fine on its own -- this is
	// not a blanket refusal of the replacement character, only of bytes
	// that cannot be represented as UTF-8 in the first place.
	baseline, err := DecodeSnapshot([]byte(`{"data":{"commit":"b18e56�"}}`))
	if err != nil {
		t.Fatalf("a body that is already valid UTF-8 (replacement character included) must still decode: %v", err)
	}
	if !baseline.DataPresent {
		t.Fatal("the baseline's data field must still be read")
	}
}

// A Tier-B declaration that relaxes nothing must be reported, exactly
// like a stale volatile-field exclusion: it reads as a relaxation that is
// there, and the first person to trust it is trusting nothing.
func TestCompareUnusedTierBIsReported(t *testing.T) {
	body := `{"data":{"hotspots":{"name":"a"}}}`
	result := Compare(
		snapshotFromJSON(t, body),
		snapshotFromJSON(t, body),
		Options{FloatTierB: map[string]string{"data.hotspots.scor": "typo"}},
	)
	if len(result.UnusedTierB) != 1 || result.UnusedTierB[0] != "data.hotspots.scor" {
		t.Fatalf("a Tier-B entry matching no compared field must be reported, got %v", result.UnusedTierB)
	}
}

// A Tier-B field that is COMPARED counts as used even when both sides
// agree -- the declaration's claim is that the field is a merged
// aggregate, not that it currently differs.
func TestCompareTierBCountsAsUsedWhenValuesAgree(t *testing.T) {
	body := `{"data":{"hotspots":{"score":1.5}}}`
	result := Compare(
		snapshotFromJSON(t, body),
		snapshotFromJSON(t, body),
		Options{FloatTierB: map[string]string{"data.hotspots.score": "CHAOS-5451 merged Float64 aggregate"}},
	)
	if len(result.UnusedTierB) != 0 {
		t.Fatalf("a stable Tier-B field must not read as stale, got %v", result.UnusedTierB)
	}
}

// A declared baseline defect changes what the receipt SAYS, never what it
// is: the terminal state stays `mismatch`, and the covered differences
// are attributed to their ticket.
func TestCompareBaselineDefectDoesNotConvertAMismatch(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"workItems":{"cycleTimeDays":9,"title":"a"}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"workItems":{"cycleTimeDays":4,"title":"a"}}}`)
	result := Compare(baseline, candidate, Options{BaselineDefects: []BaselineDefect{{
		Ticket: "CHAOS-5448",
		Reason: "Python omits FINAL on work_item_cycle_times and counts superseded row versions",
		Paths:  []string{"data.workItems.cycleTimeDays"},
	}}})

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("a declared baseline defect must NEVER convert a mismatch, got %s", result.TerminalState)
	}
	if len(result.BaselineDefectsMatched) != 1 || result.BaselineDefectsMatched[0] != "CHAOS-5448" {
		t.Fatalf("the covering ticket must be recorded, got %v", result.BaselineDefectsMatched)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("every difference was covered; want an explicit zero, got %d", result.DifferencesOutsideBaselineDefect)
	}
	if len(result.StaleBaselineDefects) != 0 {
		t.Fatalf("the entry matched, so it is not stale: %v", result.StaleBaselineDefects)
	}
}

// A difference OUTSIDE every declared path must be counted, so "all of
// this is a known Python defect" cannot quietly cover a new one.
func TestCompareCountsDifferencesOutsideABaselineDefect(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"workItems":{"cycleTimeDays":9,"title":"a"}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"workItems":{"cycleTimeDays":4,"title":"b"}}}`)
	result := Compare(baseline, candidate, Options{BaselineDefects: []BaselineDefect{{
		Ticket: "CHAOS-5448",
		Paths:  []string{"data.workItems.cycleTimeDays"},
	}}})

	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("the uncovered title difference must be counted, got %d", result.DifferencesOutsideBaselineDefect)
	}
	if len(result.BaselineDefectsMatched) != 1 {
		t.Fatalf("the covering ticket still applies to its own path, got %v", result.BaselineDefectsMatched)
	}
}

// A cited path covers everything beneath it, so a subtree root can be
// named once rather than every leaf under it.
func TestCompareBaselineDefectCoversASubtree(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"cycles":{"nested":{"a":1,"b":2}}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"cycles":{"nested":{"a":9,"b":8}}}}`)
	result := Compare(baseline, candidate, Options{BaselineDefects: []BaselineDefect{{
		Ticket: "CHAOS-5448",
		Paths:  []string{"data.cycles"},
	}}})
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("a subtree root must cover its leaves, got %d uncovered", result.DifferencesOutsideBaselineDefect)
	}
}

// An entry covering nothing is stale -- the defect was fixed, or the
// paths are wrong. Either way it must be reported so the caller fails the
// run, rather than sitting in the table reading as coverage.
func TestCompareStaleBaselineDefectIsReported(t *testing.T) {
	body := `{"data":{"workItems":{"cycleTimeDays":4}}}`
	result := Compare(
		snapshotFromJSON(t, body),
		snapshotFromJSON(t, body),
		Options{BaselineDefects: []BaselineDefect{{Ticket: "CHAOS-5448", Paths: []string{"data.workItems.cycleTimeDays"}}}},
	)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("no differences means match, got %s", result.TerminalState)
	}
	if len(result.StaleBaselineDefects) != 1 || result.StaleBaselineDefects[0] != "CHAOS-5448" {
		t.Fatalf("an entry covering nothing must be reported stale, got %v", result.StaleBaselineDefects)
	}
}
