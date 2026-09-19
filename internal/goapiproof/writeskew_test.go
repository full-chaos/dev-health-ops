package goapiproof

import (
	"encoding/json"
	"math/big"
	"os"
	"strings"
	"testing"
)

// The two production captures of GET /api/v1/investment/sunburst
// team_scoped where one value leaf sat outside every declaration: the
// candidate read agrees with the same run's org-wide reads, the baseline
// read carries a later generation of one work unit's row.
var writeSkewCaptures = []struct {
	name, baseline, candidate string
}{
	{"first capture", "testdata/investmentsunburst_teamscoped_skew_baseline_3cf72260.json", "testdata/investmentsunburst_teamscoped_skew_candidate_a12379ac.json"},
	{"second capture", "testdata/investmentsunburst_teamscoped_skew_baseline_f8f22530.json", "testdata/investmentsunburst_teamscoped_skew_candidate_309223b4.json"},
}

func readRESTFixture(t *testing.T, path string) Snapshot {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	snapshot, err := DecodeRESTSnapshot(raw)
	if err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return snapshot
}

// cloneSnapshot deep-copies a decoded snapshot through JSON, so a test can
// derive a second read without touching the first.
func cloneSnapshot(t *testing.T, s Snapshot) Snapshot {
	t.Helper()
	raw, err := json.Marshal(s.Data)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := DecodeRESTSnapshot(raw)
	if err != nil {
		t.Fatalf("decode clone: %v", err)
	}
	return out
}

// setKeyedValue sets field on the one element of the top-level keyed list
// whose declared key renders to key; ok is false when no element has it.
func setKeyedValue(t *testing.T, s Snapshot, opts Options, key, field string, value any) {
	t.Helper()
	decl, ok := findOrderInsensitiveList(opts, "data")
	if !ok {
		t.Fatal("the parity declares no order-insensitive list at data")
	}
	for _, element := range s.Data.([]any) {
		if k, ok := orderInsensitiveKey(element, decl.KeyFields); ok && k == key {
			element.(map[string]any)[field] = value
			return
		}
	}
	t.Fatalf("no element with key %q", key)
}

func removeKeyed(t *testing.T, s *Snapshot, opts Options, key string) {
	t.Helper()
	decl, _ := findOrderInsensitiveList(opts, "data")
	var kept []any
	for _, element := range s.Data.([]any) {
		if k, ok := orderInsensitiveKey(element, decl.KeyFields); ok && k == key {
			continue
		}
		kept = append(kept, element)
	}
	s.Data = kept
}

// TestClassifyWriteSkew_CapturedTeamScopedSunburst runs the whole
// decision on both production captures. The second baseline read is the
// first one carrying, at the outside leaf, the value the candidate read:
// the reference plane moved to the candidate's generation.
func TestClassifyWriteSkew_CapturedTeamScopedSunburst(t *testing.T) {
	opts := investmentSunburstTeamScopedParityWithLimit(investmentSunburstDefaultLimit)
	for _, capture := range writeSkewCaptures {
		t.Run(capture.name, func(t *testing.T) {
			baseline := readRESTFixture(t, capture.baseline)
			candidate := readRESTFixture(t, capture.candidate)
			result := Compare(baseline, candidate, opts)
			if result.DifferencesOutsideBaselineDefect != 1 || !WriteSkewRereadNeeded(result) {
				t.Fatalf("first comparison: outside=%d reread=%v, want exactly one outside value leaf", result.DifferencesOutsideBaselineDefect, WriteSkewRereadNeeded(result))
			}
			outside := result.Findings[result.outsideFindings[0]]
			keys := findingKeys(outside.Detail)
			baselineValue, _, _ := leafAt(baseline.Data, outside.Path, keys, opts)
			candidateValue, _, _ := leafAt(candidate.Data, outside.Path, keys, opts)
			if len(keys) != 1 {
				t.Fatalf("outside leaf %s carries keys %v, want one", outside.Path, keys)
			}

			// Skew: the second baseline read carries the candidate's generation.
			second := cloneSnapshot(t, baseline)
			setKeyedValue(t, second, opts, keys[0], "value", candidateValue)
			verdict, leaves, detail := classifyForTest(result, baseline, candidate, second, opts)
			t.Logf("%s: key %q baseline=%v candidate=%v -> %s %s", capture.name, keys[0], baselineValue, candidateValue, verdict, detail)
			if verdict != WriteSkewAdmitted || len(leaves) != 1 || !leafValuesEqual(leaves[0].SecondBaseline, candidateValue) || !leafValuesEqual(leaves[0].FirstBaseline, baselineValue) || !leafValuesEqual(leaves[0].Candidate, candidateValue) {
				t.Fatalf("verdict=%s leaves=%+v detail=%s, want skew_admitted naming B1, C1 and B2", verdict, leaves, detail)
			}

			// Direction trap: the reference plane is unchanged on the re-read.
			verdict, _, _ = classifyForTest(result, baseline, candidate, cloneSnapshot(t, baseline), opts)
			if verdict != WriteSkewStands {
				t.Fatalf("stable second baseline read: verdict=%s, want stands", verdict)
			}

			// A third value: data moving under the case.
			moved := cloneSnapshot(t, baseline)
			setKeyedValue(t, moved, opts, keys[0], "value", addToNumber(t, baselineValue, 1))
			if verdict, _, _ = classifyForTest(result, baseline, candidate, moved, opts); verdict != WriteSkewRefused {
				t.Fatalf("third value: verdict=%s, want refused", verdict)
			}

			// The leaf missing from the second read.
			missing := cloneSnapshot(t, baseline)
			removeKeyed(t, &missing, opts, keys[0])
			if verdict, _, _ = classifyForTest(result, baseline, candidate, missing, opts); verdict != WriteSkewRefused {
				t.Fatalf("leaf missing: verdict=%s, want refused", verdict)
			}

			// The outside leaf agrees, but the re-read differs elsewhere:
			// no whole-case agreement, nothing admitted.
			elsewhere := cloneSnapshot(t, baseline)
			setKeyedValue(t, elsewhere, opts, keys[0], "value", candidateValue)
			other := otherMatchedKey(t, baseline, candidate, opts, keys[0])
			otherValue, _, _ := leafAt(baseline.Data, outside.Path, []string{other}, opts)
			// Lowered, not raised: the route's own declaration (a
			// baseline strictly above the candidate) would admit a raise.
			setKeyedValue(t, elsewhere, opts, other, "value", addToNumber(t, otherValue, -7))
			if verdict, _, _ = classifyForTest(result, baseline, candidate, elsewhere, opts); verdict != WriteSkewStands {
				t.Fatalf("difference elsewhere on the re-read: verdict=%s, want stands", verdict)
			}
		})
	}
}

// otherMatchedKey returns a key, other than skip, that both legs carry with
// equal values.
func otherMatchedKey(t *testing.T, baseline, candidate Snapshot, opts Options, skip string) string {
	t.Helper()
	decl, _ := findOrderInsensitiveList(opts, "data")
	values := map[string]any{}
	for _, element := range baseline.Data.([]any) {
		if k, ok := orderInsensitiveKey(element, decl.KeyFields); ok {
			values[k] = element.(map[string]any)["value"]
		}
	}
	for _, element := range candidate.Data.([]any) {
		k, ok := orderInsensitiveKey(element, decl.KeyFields)
		if ok && k != skip && leafValuesEqual(values[k], element.(map[string]any)["value"]) {
			return k
		}
	}
	t.Fatal("no other matched key")
	return ""
}

// TestWriteSkewRereadNeeded_OnlyOutsideValueLeaves pins which first
// comparisons trigger a re-read at all.
func TestWriteSkewRereadNeeded_OnlyOutsideValueLeaves(t *testing.T) {
	for _, cell := range []struct {
		name                string
		baseline, candidate string
		opts                Options
		want                bool
	}{
		{"match", `{"data":{"a":1}}`, `{"data":{"a":1}}`, Options{}, false},
		{"outside value leaf", `{"data":{"a":1}}`, `{"data":{"a":2}}`, Options{}, true},
		{"outside presence difference", `{"data":{"a":1}}`, `{"data":{"a":1,"b":2}}`, Options{}, false},
		{"outside value and presence", `{"data":{"a":1}}`, `{"data":{"a":2,"b":2}}`, Options{}, false},
		{"outside type difference", `{"data":{"a":1}}`, `{"data":{"a":"1"}}`, Options{}, false},
		{"outside null", `{"data":{"a":1}}`, `{"data":{"a":null}}`, Options{}, false},
		{"covered value leaf only", `{"data":{"a":2}}`, `{"data":{"a":1}}`, Options{BaselineDefects: []BaselineDefect{{Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.a"}}}}, false},
		{"structural refusal", `{"data":{"items":[{"id":"ABC-1"}]}}`, `{"data":{"items":[{"id":"ABC-2"}]}}`, Options{}, false},
	} {
		t.Run(cell.name, func(t *testing.T) {
			result := Compare(snapshotFromJSON(t, cell.baseline), snapshotFromJSON(t, cell.candidate), cell.opts)
			if got := WriteSkewRereadNeeded(result); got != cell.want {
				t.Fatalf("reread=%v, want %v (outside=%d refusal=%q shapes=%v)", got, cell.want, result.DifferencesOutsideBaselineDefect, result.StructuralRefusal, result.OutsideByShape)
			}
		})
	}
}

// TestLeafAt_LocatesEveryPathFormCompareBuilds covers the locator on the
// path forms compareJSON emits: nested fields, a field name containing a
// dot, a positional list and a declared keyed list.
func TestLeafAt_LocatesEveryPathFormCompareBuilds(t *testing.T) {
	keyed := Options{OrderInsensitiveLists: []OrderInsensitiveList{{Path: "data.rows", KeyFields: []string{"id"}, Reason: "test fixture", Ticket: "ABC-123"}}}
	for _, cell := range []struct {
		name, baseline, candidate string
		opts                      Options
		want                      string
	}{
		{"nested field", `{"data":{"a":{"b":1}}}`, `{"data":{"a":{"b":2}}}`, Options{}, "1"},
		{"dotted field name", `{"data":{"m":{"quality.bugfix":1}}}`, `{"data":{"m":{"quality.bugfix":2}}}`, Options{}, "1"},
		{"positional list", `{"data":{"xs":[1,5]}}`, `{"data":{"xs":[1,6]}}`, Options{}, "5"},
		{"keyed list, reordered", `{"data":{"rows":[{"id":"ABC-1","v":1},{"id":"ABC-2","v":3}]}}`, `{"data":{"rows":[{"id":"ABC-2","v":4},{"id":"ABC-1","v":1}]}}`, keyed, "3"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			baseline := snapshotFromJSON(t, cell.baseline)
			result := Compare(baseline, snapshotFromJSON(t, cell.candidate), cell.opts)
			if len(result.outsideFindings) != 1 {
				t.Fatalf("outside findings = %v (%v), want one", result.outsideFindings, result.Findings)
			}
			f := result.Findings[result.outsideFindings[0]]
			got, ok, ambiguous := leafAt(baseline.Data, f.Path, findingKeys(f.Detail), cell.opts)
			if !ok || ambiguous || !leafValuesEqual(got, json.Number(cell.want)) {
				t.Fatalf("leafAt(%s, %q) = %v, %v; want %v", f.Path, strings.Join(findingKeys(f.Detail), ","), got, ok, cell.want)
			}
		})
	}
}

// addToNumber returns n + delta as a JSON number, exactly.
func addToNumber(t *testing.T, n any, delta int64) json.Number {
	t.Helper()
	r, ok := exactNumber(n)
	if !ok {
		t.Fatalf("%v (%T) is not a number", n, n)
	}
	r.Add(r, new(big.Rat).SetInt64(delta))
	return json.Number(r.FloatString(10))
}

// TestLeafValuesEqual_IsExactNumericEquality pins the equality the
// bracket admits on: two spellings of one number are equal, two numbers
// one ulp apart are not, a number never equals a string of its digits.
func TestLeafValuesEqual_IsExactNumericEquality(t *testing.T) {
	for _, cell := range []struct {
		a, b any
		want bool
	}{
		{json.Number("1"), json.Number("1.0"), true},
		{json.Number("1e3"), json.Number("1000"), true},
		{json.Number("167682.7945792228"), json.Number("167682.7945792228"), true},
		{json.Number("167682.7945792228"), json.Number("167682.79457922282"), false},
		{json.Number("0.1"), float64(0.1), false},
		{json.Number("1"), "1", false},
		{"ABC-123", "ABC-123", true},
		{true, false, false},
		{nil, nil, true},
		{nil, json.Number("0"), false},
	} {
		if got := leafValuesEqual(cell.a, cell.b); got != cell.want {
			t.Errorf("leafValuesEqual(%#v, %#v) = %v, want %v", cell.a, cell.b, got, cell.want)
		}
	}
}

// TestElementByKey_RequiresExactlyOneElement pins the keyed lookup: a key
// no element carries, or one two elements share, locates nothing.
func TestElementByKey_RequiresExactlyOneElement(t *testing.T) {
	list := []any{
		map[string]any{"id": "ABC-1", "v": json.Number("1")},
		map[string]any{"id": "ABC-2", "v": json.Number("2")},
		map[string]any{"id": "ABC-2", "v": json.Number("3")},
	}
	if e, ok := elementByKey(list, []string{"id"}, "ABC-1"); !ok || e.(map[string]any)["v"] != json.Number("1") {
		t.Fatalf("ABC-1: %v %v, want the one element", e, ok)
	}
	if _, ok := elementByKey(list, []string{"id"}, "ABC-2"); ok {
		t.Fatal("ABC-2 is carried by two elements, want not found")
	}
	if _, ok := elementByKey(list, []string{"id"}, "ABC-9"); ok {
		t.Fatal("ABC-9 is carried by no element, want not found")
	}
}

// TestLeafAtRefusesAPathThatDenotesMoreThanOneLocation: a display path
// is one location only when no literal key spells a nested path. With a
// field "q" (an object or a list) beside a literal key "q.b" or "q[0]",
// the path names two leaves, and leafAt says so rather than picking one.
func TestLeafAtRefusesAPathThatDenotesMoreThanOneLocation(t *testing.T) {
	for _, cell := range []struct {
		name, body, path string
		want             string
		ambiguous        bool
	}{
		{"nested field and literal dotted key", `{"q":{"b":1},"q.b":2}`, "$.data.q.b", "", true},
		{"list element and literal bracketed key", `{"q":[1],"q[0]":2}`, "$.data.q[0]", "", true},
		{"deeper collision", `{"a":{"b":{"c":1},"b.c":2}}`, "$.data.a.b.c", "", true},
		{"literal dotted key alone", `{"q.b":2}`, "$.data.q.b", "2", false},
		{"nested field alone", `{"q":{"b":1}}`, "$.data.q.b", "1", false},
		{"a longer field that only shares a prefix", `{"q":{"b":1},"qq":{"b":2}}`, "$.data.q.b", "1", false},
		{"sibling path unaffected", `{"q":{"b":1,"c":3},"q.b":2}`, "$.data.q.c", "3", false},
		{"absent", `{"q":{}}`, "$.data.q.b", "", false},
	} {
		body := snapshotFromJSON(t, `{"data":`+cell.body+`}`)
		got, present, ambiguous := leafAt(body.Data, cell.path, nil, Options{})
		if ambiguous != cell.ambiguous {
			t.Errorf("%s: ambiguous = %v, want %v", cell.name, ambiguous, cell.ambiguous)
			continue
		}
		if cell.want == "" {
			if present {
				t.Errorf("%s: present with %v, want no single location", cell.name, got)
			}
			continue
		}
		if !present || !leafValuesEqual(got, json.Number(cell.want)) {
			t.Errorf("%s: %v, %v; want %s", cell.name, got, present, cell.want)
		}
	}
}

// TestAnAmbiguousOutsideLeafIsNeverWitnessed reproduces a nested leaf and
// a literal key spelling the same path: the nested leaf moved to a third
// value while the literal key moved to the candidate's. Neither may stand
// for the other, so the case is refused by name.
func TestAnAmbiguousOutsideLeafIsNeverWitnessed(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"s":{"mean":0.25},"s.mean":0.25}}`)
	candidate := snapshotFromJSON(t, `{"data":{"s":{"mean":0.5},"s.mean":0.5}}`)
	second := snapshotFromJSON(t, `{"data":{"s":{"mean":0.50000000001},"s.mean":0.5}}`)
	first := Compare(baseline, candidate, Options{})
	decision := ClassifyWriteSkew(first, baseline, candidate, second, Options{})
	if decision.Verdict != WriteSkewRefused || decision.Refusal != RESTRefusalRereadLeafAmbiguous {
		t.Fatalf("verdict %s refusal %q (%s), want refused %q", decision.Verdict, decision.Refusal, decision.Detail, RESTRefusalRereadLeafAmbiguous)
	}
}

// TestClassifyWriteSkew_SecondReadRefusedStructurallyStands pins the
// whole-case check against a structural refusal: the second baseline read
// carries the candidate's value at the outside leaf, but its body is over
// three times the candidate's size, so comparing the two is refused before
// any value is compared. The reference changed shape between its two
// reads, so the case is refused by name (R3), with no receipt.
func TestClassifyWriteSkew_SecondReadRefusedStructurallyIsRefused(t *testing.T) {
	pad := strings.Repeat("x", 300)
	decode := func(body string) Snapshot {
		s, err := DecodeRESTSnapshot([]byte(body))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return s
	}
	baseline := decode(`{"a":1,"b":2,"pad":"` + pad + `"}`)
	candidate := decode(`{"a":1,"b":3,"pad":"` + pad + `"}`)
	second := decode(`{"a":1,"b":3,"pad":"` + strings.Repeat("y", 1200) + `"}`)
	result := Compare(baseline, candidate, Options{})
	if Compare(second, candidate, Options{}).StructuralRefusal == "" {
		t.Fatal("fixture: the second baseline read must be refused structurally against the candidate")
	}
	decision := ClassifyWriteSkew(result, baseline, candidate, second, Options{})
	if decision.Verdict != WriteSkewRefused || decision.Refusal != RESTRefusalRereadStructural {
		t.Fatalf("verdict = %s refusal = %q (%s), want refused %q", decision.Verdict, decision.Refusal, decision.Detail, RESTRefusalRereadStructural)
	}
}

// TestClassifyWriteSkew_UnwitnessedLeafStandsWhateverTheSecondComparisonCovers
// pins that the witness is required at EVERY outside leaf, and that no
// declaration evaluated against the second baseline read can stand in for
// it. The first comparison has two outside leaves; the second baseline
// read moves one to the candidate's value and leaves the other where it
// was. Classified under declarations that cover the unmoved leaf in the
// second comparison, the case still stands.
func TestClassifyWriteSkew_UnwitnessedLeafStandsWhateverTheSecondComparisonCovers(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"b":1,"c":1}}`)
	candidate := snapshotFromJSON(t, `{"data":{"b":2,"c":2}}`)
	second := snapshotFromJSON(t, `{"data":{"b":1,"c":2}}`)
	first := Compare(baseline, candidate, Options{})
	if len(first.outsideFindings) != 2 {
		t.Fatalf("fixture: want two outside leaves, got %d", len(first.outsideFindings))
	}
	for _, cell := range []struct {
		name string
		opts Options
	}{
		{"no declaration", Options{}},
		{"a declaration covering the unmoved leaf", Options{BaselineDefects: []BaselineDefect{{Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.b"}}}}},
		{"a scalar-direction shape on the unmoved leaf", Options{BaselineDefects: []BaselineDefect{{
			Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.b"},
			ScalarDirectionShape: &ScalarDirectionShape{Path: "data.b", ContestedPaths: []string{"data.b"}},
		}}}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			verdict, leaves, detail := classifyForTest(first, baseline, candidate, second, cell.opts)
			t.Logf("%s -> %s (%s)", cell.name, verdict, detail)
			if verdict != WriteSkewStands || len(leaves) != 2 {
				t.Fatalf("verdict = %s, leaves = %+v; want stands with both leaves recorded", verdict, leaves)
			}
		})
	}
}

// TestWriteSkewLeaf_ReportsTheSecondBaselineReadAlways pins the report
// contract: the second baseline value is always serialised, and a JSON
// null is told apart from a leaf the second read did not carry.
func TestWriteSkewLeaf_ReportsTheSecondBaselineReadAlways(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"b":1}}`)
	candidate := snapshotFromJSON(t, `{"data":{"b":2}}`)
	first := Compare(baseline, candidate, Options{})
	for _, cell := range []struct {
		name, second, want string
	}{
		{"null", `{"data":{"b":null}}`, `"second_baseline":null,"second_baseline_present":true`},
		{"missing", `{"data":{}}`, `"second_baseline":null,"second_baseline_present":false`},
		{"a value", `{"data":{"b":3}}`, `"second_baseline":3,"second_baseline_present":true`},
	} {
		t.Run(cell.name, func(t *testing.T) {
			_, leaves, _ := classifyForTest(first, baseline, candidate, snapshotFromJSON(t, cell.second), Options{})
			raw, err := json.Marshal(leaves)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if !strings.Contains(string(raw), cell.want) {
				t.Fatalf("report %s does not carry %s", raw, cell.want)
			}
		})
	}
}

// classifyForTest is ClassifyWriteSkew's decision as (verdict, leaves,
// detail).
func classifyForTest(first Result, firstBaseline, candidate, secondBaseline Snapshot, opts Options) (WriteSkewVerdict, []WriteSkewLeaf, string) {
	decision := ClassifyWriteSkew(first, firstBaseline, candidate, secondBaseline, opts)
	return decision.Verdict, decision.Leaves, decision.Detail
}

// TestWatermarkGatedComparisonIsNeverReRead (row F6): a comparison that
// ends unsupported because a required watermark is absent has no outside
// value finding, so no re-read is made and the classifier reports
// not_applicable.
func TestWatermarkGatedComparisonIsNeverReRead(t *testing.T) {
	baseline := Snapshot{Data: map[string]any{"v": json.Number("1")}, DataPresent: true}
	candidate := Snapshot{Data: map[string]any{"v": json.Number("2")}, DataPresent: true}
	result := Compare(baseline, candidate, Options{RequireWatermark: true})
	if result.TerminalState != TerminalStateUnsupported {
		t.Fatalf("fixture: terminal state %q, want %q", result.TerminalState, TerminalStateUnsupported)
	}
	if WriteSkewRereadNeeded(result) {
		t.Fatal("an unsupported comparison asked for a re-read")
	}
	if got := ClassifyWriteSkew(result, baseline, candidate, candidate, Options{RequireWatermark: true}); got.Verdict != WriteSkewNotApplicable {
		t.Fatalf("verdict %q, want not_applicable", got.Verdict)
	}
}
