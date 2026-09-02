package pythonparity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const edgeShapesGoldenPath = "../../tests/fixtures/evidence_json_edge_shapes_python_golden.json"

type edgeShapeRow struct {
	TeamID      string `json:"team_id"`
	MetricTable string `json:"metric_table"`
	WindowStart string `json:"window_start"`
	WindowEnd   string `json:"window_end"`
	Field       string `json:"field"`
	ValueBits   string `json:"value_bits"`
}

type edgeShapeCase struct {
	Name         string         `json:"name"`
	Why          string         `json:"why"`
	Rows         []edgeShapeRow `json:"rows"`
	EvidenceJSON string         `json:"evidence_json"`
}

type edgeShapesGolden struct {
	Cases []edgeShapeCase `json:"cases"`
}

func loadEdgeShapesGolden(t *testing.T) edgeShapesGolden {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(edgeShapesGoldenPath))
	if err != nil {
		t.Fatalf("read golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_evidence_json_edge_shapes_golden.py)", err)
	}
	var golden edgeShapesGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(golden.Cases) == 0 {
		t.Fatal("golden contains no cases")
	}
	return golden
}

// TestEdgeShapesMatchLiveProducerBytes covers the spellings of this column that
// the REAL corpus cannot reach.
//
// lane-3092 compared 302 real evidence shapes against this encoder and found
// them byte-identical -- but those rows carry zero -Infinity and every string
// in them is ASCII. A corpus that only samples what production happened to emit
// is silent on the rest, and the silence is invisible: the encoder returns
// plausible bytes either way.
//
// So these eleven are synthetic ON PURPOSE, and each one is a real spelling of
// the same column: the third allow_nan token, ensure_ascii over non-ASCII, CJK,
// emoji and astral text, the short escapes, C0 controls including NUL, signed
// zero, and the empty list.
func TestEdgeShapesMatchLiveProducerBytes(t *testing.T) {
	golden := loadEdgeShapesGolden(t)

	for _, testCase := range golden.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			rows := make([]OrderedObject, 0, len(testCase.Rows))
			for _, r := range testCase.Rows {
				rows = append(rows, OrderedObject{
					{Key: "team_id", Value: r.TeamID},
					{Key: "metric_table", Value: r.MetricTable},
					{Key: "window_start", Value: r.WindowStart},
					{Key: "window_end", Value: r.WindowEnd},
					{Key: "field", Value: r.Field},
					{Key: "value", Value: floatFromBits(t, r.ValueBits)},
				})
			}

			encoded, err := MarshalPythonJSONInsertionOrder(rows)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if got := string(encoded); got != testCase.EvidenceJSON {
				t.Errorf("bytes differ from the live producer\n  why: %s\n"+
					"  python: %s\n  go:     %s",
					testCase.Why, testCase.EvidenceJSON, got)
			}
			for offset, b := range encoded {
				if b >= 0x80 {
					t.Fatalf("byte %d is 0x%02x; ensure_ascii output must be pure "+
						"ASCII, so a literal non-ASCII byte means the escape path "+
						"was skipped: %q", offset, b, encoded)
				}
			}
		})
	}
}

// TestEdgeShapesActuallyCoverWhatTheyClaim is the anti-erosion check.
//
// The value of this fixture is entirely in WHICH shapes it contains. A case
// deleted, or a value quietly changed to something ordinary, leaves a green
// suite and a corpus that no longer covers the thing it was added for. Naming
// the required tokens here means erosion fails loudly instead of silently.
//
// It also caught a real mislabel: the case named `all-three-tokens` was
// contributed carrying only TWO of them. The name promised coverage the case
// did not have, which is the exact failure this test exists to prevent.
func TestEdgeShapesActuallyCoverWhatTheyClaim(t *testing.T) {
	golden := loadEdgeShapesGolden(t)

	all := ""
	byName := make(map[string]string, len(golden.Cases))
	for _, testCase := range golden.Cases {
		all += testCase.EvidenceJSON
		byName[testCase.Name] = testCase.EvidenceJSON
	}

	for _, required := range []struct{ token, why string }{
		// Matched in VALUE POSITION (": " prefix) and with NO trailing quote.
		//
		// Both halves are load-bearing. Without the prefix, `"Infinity"` is a
		// SUBSTRING of `"-Infinity"`, so a fixture carrying only -Infinity
		// satisfies the +Infinity check -- this test passed on a planted case
		// holding NaN, -Infinity, -Infinity, which is the exact two-distinct-
		// token mislabel it was written to catch. And a trailing quote would
		// match NOTHING, because these tokens are BARE: they are followed by
		// `}` or `,`, never `"`. That version fails on a CORRECT fixture.
		//
		// `": Infinity"` cannot be satisfied by -Infinity, because there the
		// colon-space is followed by `-`.
		{": NaN", "the NaN token"},
		{": Infinity", "the +Infinity token"},
		{": -Infinity", "the -Infinity token, which no real corpus row carries"},
		// Searched as SIX-CHARACTER TEXT, not as the characters themselves.
		// ensure_ascii output contains the escape SEQUENCE, so the literal rune
		// never appears -- searching for it would fail on a CORRECT fixture. It
		// would also put a raw non-ASCII byte (and, for NUL, a byte the Go
		// compiler rejects outright) into a file where it renders as nothing.
		{`\u00e9`, "a non-ASCII escape, proving ensure_ascii fires"},
		{`\ud83d\ude00`, "an astral surrogate PAIR, not a literal 4-byte rune"},
		{`\u0000`, "NUL, which has no short escape"},
		{`\t`, "a tab short-escape"},
		{`\"`, "an escaped quote"},
		{"-0.0", "signed zero keeping its sign"},
		{"[]", "the empty evidence list"},
	} {
		if !strings.Contains(all, required.token) {
			t.Errorf("no case emits %q (%s); the corpus has lost the coverage it "+
				"was added for", required.token, required.why)
		}
	}

	// The mislabel that was actually found, pinned so it cannot come back.
	if got := byName["all-three-tokens"]; got != "" {
		for _, token := range []string{": NaN", ": Infinity", ": -Infinity"} {
			if !strings.Contains(got, token) {
				t.Errorf("case `all-three-tokens` does not contain %q; a case whose "+
					"NAME promises coverage must actually carry it, or the next "+
					"reader trusts the label instead of the bytes", token)
			}
		}
	}
}
