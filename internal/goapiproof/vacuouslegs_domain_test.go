package goapiproof

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// This file holds the whole input domain of Compare's vacuity exit: leg A
// x leg B over every zero-leaf and non-empty shape of one field, under
// every Parity relaxation. A vacuous refusal means "both planes gave the
// same empty answer, so nothing was compared". It is correct only when
// the two decoded legs are equal; two zero-leaf legs that differ
// (null vs [], absent vs [], [] vs {}) are a real difference between the
// planes and must reach the comparison, never be refused as vacuous.

var vacuityShapes = []struct {
	name     string
	json     string // "" means the key is absent
	zeroLeaf bool
}{
	{"absent", "", true},
	{"null", `null`, true},
	{"[]", `[]`, true},
	{"{}", `{}`, true},
	{"leafless nested", `{"tags":[]}`, true},
	{"non-empty", `[{"id":"ABC-123"}]`, false},
}

func vacuityBody(fieldJSON string) string {
	if fieldJSON == "" {
		return `{"data":{"widget":{}}}`
	}
	return `{"data":{"widget":{"items":` + fieldJSON + `}}}`
}

var vacuityRelaxations = []struct {
	name       string
	opts       Options
	armsVacuum bool
}{
	{"none", Options{}, false},
	{"BaselineDefects", Options{BaselineDefects: []BaselineDefect{{
		Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.widget.items.unused"},
	}}}, true},
	{"OrderInsensitiveLists", Options{OrderInsensitiveLists: []OrderInsensitiveList{{
		Path: "data.widget.items", KeyFields: []string{"id"}, Reason: "test fixture", Ticket: "ABC-123",
	}}}, true},
	{"VolatileFields", Options{VolatileFields: map[string]string{"data.widget.items": "test fixture"}}, false},
}

// TestCompareVacuityInputDomain runs every cell in one pass and prints the
// executed table: leg A x leg B x relaxation -> verdict.
func TestCompareVacuityInputDomain(t *testing.T) {
	var rows []string
	for _, relax := range vacuityRelaxations {
		for _, a := range vacuityShapes {
			for _, b := range vacuityShapes {
				baseline := snapshotFromJSON(t, vacuityBody(a.json))
				candidate := snapshotFromJSON(t, vacuityBody(b.json))
				result := Compare(baseline, candidate, relax.opts)
				verdict := string(result.TerminalState)
				if result.StructuralRefusal != "" {
					verdict = "refused:" + result.StructuralRefusal
				}
				rows = append(rows, fmt.Sprintf("%-22s %-16s x %-16s -> %s", relax.name, a.name, b.name, verdict))

				equal := reflect.DeepEqual(baseline.Data, candidate.Data)
				wantVacuous := relax.armsVacuum && a.zeroLeaf && b.zeroLeaf && equal
				isVacuous := result.StructuralRefusal == RefusalVacuousEmptyLegs
				if isVacuous != wantVacuous {
					t.Errorf("%s: %s x %s: vacuous=%v, want %v (verdict %s)", relax.name, a.name, b.name, isVacuous, wantVacuous, verdict)
				}
				// Two legs that differ must never read as a clean
				// match, except where the relaxation itself declares
				// the field volatile (its values are not compared).
				if !equal && relax.name != "VolatileFields" && result.StructuralRefusal == "" && result.TerminalState == TerminalStateMatch {
					t.Errorf("%s: %s x %s: differing legs read as a clean match", relax.name, a.name, b.name)
				}
			}
		}
	}
	sort.Strings(rows)
	t.Logf("vacuity domain (%d cells):\n%s", len(rows), strings.Join(rows, "\n"))
}

// TestCompareEarlyExitsNeverTurnADifferenceIntoAPass is the sibling sweep:
// every exit Compare takes before its value comparison, each executed
// with two legs that DIFFER. None of them may return a clean match, and
// none may return the vacuous refusal (the one exit that means "nothing
// to compare", which a bounded candidate search treats as no data).
func TestCompareEarlyExitsNeverTurnADifferenceIntoAPass(t *testing.T) {
	relax := Options{BaselineDefects: []BaselineDefect{{Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.widget.items.unused"}}}}
	big := func(id string, n int) string {
		items := make([]string, n)
		for i := range items {
			items[i] = fmt.Sprintf(`{"id":"%s-%d","label":"ABC-123"}`, id, i)
		}
		return `{"data":{"widget":{"items":[` + strings.Join(items, ",") + `]}}}`
	}
	for _, cell := range []struct {
		name                string
		baseline, candidate Snapshot
		opts                Options
		wantRefusal         string
		wantTerminal        string
	}{
		{
			name:      "required watermark missing",
			baseline:  withWatermark(snapshotFromJSON(t, vacuityBody(`[]`)), ""),
			candidate: withWatermark(snapshotFromJSON(t, vacuityBody(`null`)), "w1"),
			opts:      Options{RequireWatermark: true}, wantTerminal: TerminalStateUnsupported,
		},
		{
			name:      "watermark drift",
			baseline:  withWatermark(snapshotFromJSON(t, vacuityBody(`[]`)), "w1"),
			candidate: withWatermark(snapshotFromJSON(t, vacuityBody(`null`)), "w2"),
			opts:      relax, wantTerminal: TerminalStateUnsupported,
		},
		{
			name:      "vacuity with differing zero-leaf legs",
			baseline:  snapshotFromJSON(t, vacuityBody(`[]`)),
			candidate: snapshotFromJSON(t, vacuityBody(`null`)),
			opts:      relax, wantTerminal: TerminalStateMismatch,
		},
		{
			name:      "structural agreement: no shared ids",
			baseline:  snapshotFromJSON(t, `{"data":{"widget":{"items":[{"id":"ABC-1"}]}}}`),
			candidate: snapshotFromJSON(t, `{"data":{"widget":{"items":[{"id":"ABC-2"}]}}}`),
			opts:      relax, wantRefusal: RefusalLegsDoNotOverlap,
		},
		{
			name:      "body size ratio",
			baseline:  withBodyBytes(snapshotFromJSON(t, big("ABC", 8))),
			candidate: withBodyBytes(snapshotFromJSON(t, big("ABC", 40))),
			opts:      Options{}, wantRefusal: RefusalLegsDoNotOverlap,
		},
	} {
		t.Run(cell.name, func(t *testing.T) {
			if reflect.DeepEqual(cell.baseline.Data, cell.candidate.Data) {
				t.Fatal("cell legs are equal; every cell here must differ")
			}
			result := Compare(cell.baseline, cell.candidate, cell.opts)
			t.Logf("%s -> terminal=%q refusal=%q findings=%d", cell.name, result.TerminalState, result.StructuralRefusal, len(result.Findings))
			if result.StructuralRefusal == RefusalVacuousEmptyLegs {
				t.Fatalf("differing legs refused as vacuous")
			}
			if result.StructuralRefusal == "" && result.TerminalState == TerminalStateMatch {
				t.Fatalf("differing legs read as a clean match")
			}
			if cell.wantRefusal != "" && result.StructuralRefusal != cell.wantRefusal {
				t.Fatalf("refusal = %q, want %q", result.StructuralRefusal, cell.wantRefusal)
			}
			if cell.wantTerminal != "" && result.TerminalState != cell.wantTerminal {
				t.Fatalf("terminal = %q, want %q", result.TerminalState, cell.wantTerminal)
			}
		})
	}
}

// withBodyBytes sets BodyBytes from the decoded body's own JSON size, the
// way the REST decoder records the raw response size.
func withBodyBytes(s Snapshot) Snapshot {
	raw, _ := json.Marshal(map[string]any{"data": s.Data})
	s.BodyBytes = len(raw)
	return s
}

func withWatermark(s Snapshot, watermark string) Snapshot {
	s.Watermark = watermark
	return s
}

// TestCompareVacuityCountsOnlyThePlanesOwnLeaves pins that the synthetic
// dedup key the REST prover injects before Compare (RESTDedupKeyField,
// built from the row's own key fields) is not response evidence: rows
// whose every field is null are still vacuous after injection, while a
// row with any real non-null leaf is not.
func TestCompareVacuityCountsOnlyThePlanesOwnLeaves(t *testing.T) {
	relax := Options{BaselineDefects: []BaselineDefect{{Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.items.unused"}}}}
	for _, cell := range []struct {
		name        string
		body        string
		wantVacuous bool
	}{
		{"all-null rows", `{"items":[{"repo_id":null,"number":null}]}`, true},
		{"all-null rows, key fields absent", `{"items":[{"title":null}]}`, true},
		{"null keys, a real leaf elsewhere", `{"items":[{"repo_id":null,"number":null,"title":"ABC-123"}]}`, false},
		{"real keys", `{"items":[{"repo_id":"ABC-123","number":7}]}`, false},
	} {
		t.Run(cell.name, func(t *testing.T) {
			leg := func() Snapshot {
				s := snapshotFromJSON(t, `{"data":`+cell.body+`}`)
				s.Data = InjectRESTDedupKeys(s.Data, "items", []string{"repo_id", "number"})
				return s
			}
			result := Compare(leg(), leg(), relax)
			got := result.StructuralRefusal == RefusalVacuousEmptyLegs
			t.Logf("%s -> terminal=%q refusal=%q", cell.name, result.TerminalState, result.StructuralRefusal)
			if got != cell.wantVacuous {
				t.Fatalf("vacuous = %v, want %v", got, cell.wantVacuous)
			}
		})
	}
}
