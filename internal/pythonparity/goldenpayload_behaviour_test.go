package pythonparity

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPayloadComparisonIgnoresProvenanceAndCatchesData is the pair team-lead
// asked for, and it is the whole justification for comparing a payload rather
// than a document.
//
// Both halves are load-bearing and neither is sufficient alone. A guard that
// ignored provenance but also ignored data would pass this test's first half
// and be worthless; a guard that caught data changes but tripped on the
// interpreter version is the one that produced a false "has ROTTED" pointing at
// recommendations/loader.py.
func TestPayloadComparisonIgnoresProvenanceAndCatchesData(t *testing.T) {
	const frozen = `{
  "environment": {"python_version": "3.14.7", "float_repr_style": "short"},
  "distinct_input_values": 2,
  "cases": [{"name": "a", "evidence_json": "[{\"value\": 1.5}]"}]
}`

	t.Run("a bumped interpreter version PASSES", func(t *testing.T) {
		// The incident, in miniature. The interpreter is unpinned, so this
		// happens on CPython's release schedule with nobody deciding anything.
		rendered := strings.Replace(frozen, `"3.14.7"`, `"3.14.8"`, 1)
		if err := comparePayloadFields([]byte(frozen), []byte(rendered), []string{"cases", "distinct_input_values"}); err != nil {
			t.Errorf("a provenance-only difference must not fail the guard; got %v", err)
		}
	})

	t.Run("a flipped case FAILS", func(t *testing.T) {
		rendered := strings.Replace(frozen, `\"value\": 1.5`, `\"value\": 1.6`, 1)
		if rendered == frozen {
			t.Fatal("the mutation did not apply; this test would pass vacuously")
		}
		err := comparePayloadFields([]byte(frozen), []byte(rendered), []string{"cases", "distinct_input_values"})
		if err == nil {
			t.Error("a changed case must fail the guard -- this is the drift the " +
				"fixture exists to detect")
		} else if !strings.Contains(err.Error(), "cases") {
			t.Errorf("the failure should name the field that differs; got %v", err)
		}
	})

	t.Run("a changed count FAILS", func(t *testing.T) {
		// distinct_input_values is payload, not provenance: it is derived from
		// the data and a change means the corpus changed.
		rendered := strings.Replace(frozen, `"distinct_input_values": 2`, `"distinct_input_values": 3`, 1)
		if err := comparePayloadFields([]byte(frozen), []byte(rendered), []string{"cases", "distinct_input_values"}); err == nil {
			t.Error("a changed distinct_input_values must fail; it is derived from the corpus")
		}
	})

	t.Run("a RENAMED payload field FAILS rather than passing vacuously", func(t *testing.T) {
		// Without this, renaming `cases` would leave the field absent on BOTH
		// sides, every named field would compare equal by not existing, and the
		// guard would go green over a document it no longer understands.
		renamed := strings.Replace(frozen, `"cases"`, `"rows"`, 1)
		err := comparePayloadFields([]byte(renamed), []byte(renamed), []string{"cases", "distinct_input_values"})
		if err == nil {
			t.Error("a missing payload field must fail; a guard must not pass by " +
				"failing to look")
		} else if !strings.Contains(err.Error(), "missing") {
			t.Errorf("the failure should say the field is missing; got %v", err)
		}
	})
}

// TestShippedFixturesExposeThePayloadFieldsTheGuardsCompare ties the unit test
// above to the real files.
//
// comparePayload is only as good as the fields in the registry entry it is
// given. This asserts every shipped fixture actually carries the fields its
// entry names -- so a fixture reshaped in the generator cannot leave a guard
// silently comparing nothing. (The fields were string literals in three
// separate guards until CHAOS-4914; they are registry entries now, which is
// what stops this test and the guards disagreeing.)
//
// It has already earned its place: restoring provenance to the band generator
// accidentally deleted `distinct_input_values`, and the guard caught it on the
// first run rather than at review.
func TestShippedFixturesExposeThePayloadFieldsTheGuardsCompare(t *testing.T) {
	// Iterates the SAME registry the guards call comparePayload with, rather than
	// a second copy of the list. When this test carried its own table, a guard
	// that changed its compared fields left this asserting the OLD ones -- green,
	// and describing a comparison that no longer happened.
	if len(allPayloadGuards) == 0 {
		t.Fatal("the guard registry is empty; this test would pass by iterating " +
			"nothing, which is the vacuous-green shape it exists to prevent")
	}
	for _, testCase := range allPayloadGuards {
		t.Run(testCase.fixture, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Clean(
				filepath.Join("../../tests/fixtures", testCase.fixture)))
			if err != nil {
				t.Fatal(err)
			}
			var document map[string]json.RawMessage
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatal(err)
			}
			for _, field := range testCase.fields {
				if _, present := document[field]; !present {
					t.Errorf("fixture does not carry %q, but its rot guard names that "+
						"field -- the guard would fail closed, which is correct, but the "+
						"generator and the guard have drifted apart", field)
				}
			}
			// And the provenance field must NOT be in the compared set, or the
			// tripwire is back.
			for _, field := range testCase.fields {
				if field == "environment" {
					t.Error("`environment` is provenance and must never be compared; " +
						"it records an interpreter that drifts without a decision")
				}
			}
		})
	}
}

// TestPayloadMismatchIsAttributed covers the half that excluding provenance does
// not give you: USING it when something fails.
//
// The original incident is the argument. The guard reported "has ROTTED" and
// sent the reader to recommendations/loader.py, while the actual difference was
// a macOS build string against a Linux one -- and the guard had both
// interpreters in hand and printed neither. Excluding provenance stops the false
// alarm; attributing it is what makes a REAL alarm diagnosable.
//
// Adopted from floattext_rot_guard_test.go, which had the pattern first.
func TestPayloadMismatchIsAttributed(t *testing.T) {
	const base = `{
  "environment": {"python_version": "%s", "float_repr_style": "short"},
  "cases": [{"name": "a", "value": %s}]
}`
	frozen := fmt.Sprintf(base, "3.14.7", "1.5")

	t.Run("same interpreter says the DATA moved", func(t *testing.T) {
		rendered := fmt.Sprintf(base, "3.14.7", "1.6")
		err := comparePayloadFields([]byte(frozen), []byte(rendered), []string{"cases"})
		if err == nil {
			t.Fatal("a changed case must fail")
		}
		if !strings.Contains(err.Error(), "real data change") {
			t.Errorf("with an identical interpreter the failure should say the data "+
				"moved; got %v", err)
		}
		if !strings.Contains(err.Error(), "CPython 3.14.7") {
			t.Errorf("the failure must name the interpreter it was produced by; got %v", err)
		}
	})

	t.Run("different interpreter says to check that first", func(t *testing.T) {
		rendered := fmt.Sprintf(base, "3.14.8", "1.6")
		err := comparePayloadFields([]byte(frozen), []byte(rendered), []string{"cases"})
		if err == nil {
			t.Fatal("a changed case must fail")
		}
		if !strings.Contains(err.Error(), "interpreter ALSO differs") {
			t.Errorf("when both moved the reader must be told to check the "+
				"interpreter before porting; got %v", err)
		}
		for _, want := range []string{"3.14.7", "3.14.8"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the failure must name BOTH interpreters; %q missing from %v", want, err)
			}
		}
	})

	t.Run("a document with no interpreter is FATAL, not tolerated", func(t *testing.T) {
		// This is what stops the provenance being deleted again -- which is
		// exactly what I did one commit earlier.
		stripped := `{"cases": [{"name": "a", "value": 9.9}]}`
		err := comparePayloadFields([]byte(frozen), []byte(stripped), []string{"cases"})
		if err == nil || !strings.Contains(err.Error(), "records no interpreter") {
			t.Errorf("a document without provenance cannot be attributed and must "+
				"fail saying so; got %v", err)
		}
	})
}
