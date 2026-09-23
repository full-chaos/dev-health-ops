package venueoracle

import (
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// JSONRowSeparator joins stored JSON texts in a string_agg query: an ASCII
// record separator, which neither JSON text nor Go's indentation contains
// unescaped.
const JSONRowSeparator = "\x1e"

// Reporter is the part of *testing.T the checks here report through.
type Reporter interface {
	Helper()
	Errorf(format string, args ...any)
}

// CompareJSONSpacingGap checks one named, ticketed difference between the
// planes' stored JSON text: Go stores the same JSON values, keys and key
// order as Python, but not json.dumps' text (", " and ": " separators,
// ensure_ascii). python and goTexts are one stored text per row, in the
// same row order ("<null>" for SQL NULL). The check fails if
//   - no row was compared;
//   - the row counts differ;
//   - every row's text is already equal (the gap is closed, so the
//     expected difference must be removed and the column compared
//     directly);
//   - a Go text, re-rendered by pyjson.Dumps with its key order kept, is
//     not exactly Python's text (a difference beyond the named gap).
func CompareJSONSpacingGap(t Reporter, name string, python, goTexts []string) {
	t.Helper()
	if len(python) == 0 {
		t.Errorf("%s: no rows compared; the check measured nothing", name)
		return
	}
	if len(python) != len(goTexts) {
		t.Errorf("%s: %d rows on Python, %d on Go", name, len(python), len(goTexts))
		return
	}
	gap := false
	for index, text := range goTexts {
		if text != python[index] {
			gap = true
		}
		rendered := text
		if text != "<null>" {
			value, err := pyjson.DecodeString(text)
			if err != nil {
				t.Errorf("%s row %d: Go stored text that is not JSON %q: %v", name, index, text, err)
				continue
			}
			if rendered, err = pyjson.Dumps(value); err != nil {
				t.Errorf("%s row %d: %v", name, index, err)
				continue
			}
		}
		if rendered != python[index] {
			t.Errorf("%s row %d differs beyond the named spacing gap:\n python %s\n go     %s", name, index, python[index], text)
		}
	}
	if !gap {
		t.Errorf("%s: Go now stores Python's JSON text on every row; the named spacing gap is closed, so remove this expected difference and compare the column directly", name)
	}
}
