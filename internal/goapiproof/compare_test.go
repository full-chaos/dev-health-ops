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

	tierB := Compare(baseline, candidate, Options{FloatTierB: map[string]bool{"data.hotspots.score": true}})
	if !tierB.IsMatch() {
		t.Fatalf("Tier B must tolerate 1e-10, got %v", findingPaths(tierB))
	}

	tooFar := Compare(
		snapshotFromJSON(t, `{"data":{"hotspots":{"score":1.0}}}`),
		snapshotFromJSON(t, `{"data":{"hotspots":{"score":1.00001}}}`),
		Options{FloatTierB: map[string]bool{"data.hotspots.score": true}},
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
	result := Compare(baseline, candidate, Options{FloatTierB: map[string]bool{"data.edges.score": true}})
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
	result := Compare(overflow, overflow, Options{FloatTierB: map[string]bool{"data.x": true}})
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
