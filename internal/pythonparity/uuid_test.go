package pythonparity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

type acceptSet struct {
	Schema   string `json:"schema"`
	Measured string `json:"measured_on"`
	Cases    []struct {
		Input   string `json:"input"`
		Verdict string `json:"verdict"`
		UUID    string `json:"uuid"`
		Error   string `json:"error"`
	} `json:"cases"`
}

func loadAcceptSet(t *testing.T) acceptSet {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "uuid_accept_set.json"))
	if err != nil {
		t.Fatalf("read the measured accept set: %v", err)
	}
	var set acceptSet
	if err := json.Unmarshal(raw, &set); err != nil {
		t.Fatalf("decode the measured accept set: %v", err)
	}
	if set.Schema != "python_uuid_accept_set.v1" || len(set.Cases) == 0 {
		t.Fatalf("unexpected accept set: schema=%q cases=%d", set.Schema, len(set.Cases))
	}
	return set
}

// TestParseUUIDMatchesTheMeasuredPythonAcceptSet asserts BOTH directions
// against the live interpreter's measured behaviour.
//
// Accepting what the reference rejects is the direction that matters: the
// callers of this function run BEFORE the reference validates, and write rows.
// A value they accept and the reference then rejects leaves those rows behind
// for a request that never legitimately ran.
func TestParseUUIDMatchesTheMeasuredPythonAcceptSet(t *testing.T) {
	for _, testCase := range loadAcceptSet(t).Cases {
		t.Run(testCase.Input, func(t *testing.T) {
			parsed, err := ParseUUID(testCase.Input)

			if testCase.Verdict == "REJECT" {
				if err == nil {
					t.Fatalf(
						"ParseUUID(%q) = %s, but CPython raises %s. Accepting what the reference "+
							"rejects is never acceptable here: the caller writes rows before the "+
							"reference validates.",
						testCase.Input, parsed, testCase.Error,
					)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseUUID(%q) refused a value CPython accepts as %s: %v",
					testCase.Input, testCase.UUID, err)
			}
			if parsed.String() != testCase.UUID {
				t.Fatalf("ParseUUID(%q) = %s, CPython = %s", testCase.Input, parsed, testCase.UUID)
			}
		})
	}
}

// TestParseUUIDDivergesFromTheGeneralPurposeParser documents WHY this function
// exists, by asserting the difference rather than describing it.
//
// google/uuid.Parse dispatches on LENGTH: at 38 characters it assumes the
// braced form and strips the first and last character without checking they are
// braces. If a future version fixed that, this test fails and the package's
// justification should be re-read — a guard that silently stops being needed is
// as misleading as one that silently stops working.
func TestParseUUIDDivergesFromTheGeneralPurposeParser(t *testing.T) {
	divergent := []string{
		"X7b9583ee-4d24-2be7-4d09-34f815bebdd7X",
		"[7b9583ee-4d24-2be7-4d09-34f815bebdd7]",
		"!7b9583ee-4d24-2be7-4d09-34f815bebdd7?",
		" 7b9583ee-4d24-2be7-4d09-34f815bebdd7 ",
		"URN:UUID:7b9583ee-4d24-2be7-4d09-34f815bebdd7",
	}
	for _, input := range divergent {
		t.Run(input, func(t *testing.T) {
			if _, err := uuid.Parse(input); err != nil {
				t.Skipf("google/uuid now rejects %q too; this divergence has closed", input)
			}
			if _, err := ParseUUID(input); err == nil {
				t.Fatalf("ParseUUID(%q) accepted a value CPython rejects", input)
			}
		})
	}
}
