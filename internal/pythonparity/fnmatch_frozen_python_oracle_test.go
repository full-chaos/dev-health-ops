package pythonparity_test

import (
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// fnMatchOracleProgram is the oracle script, run as an inline program: its
// text is part of the request the golden is keyed on.
//
//go:embed testdata/python_fnmatch_oracle.py
var fnMatchOracleProgram string

// TestFnMatchMatchesFrozenPython is the oracle: it compares both the match
// outcome AND the translated expression against what CPython answered,
// executed once on the pinned build and frozen.
func TestFnMatchMatchesFrozenPython(t *testing.T) {
	// JSON rather than a line-based protocol: two cases carry a newline INSIDE
	// the name, and a line-based encoding silently drops them. The first
	// version of this harness did exactly that and reported "checked 28 of 30"
	// -- the count guard caught it, which is why the guard is there.
	pairs := pythonparity.FnMatchOracleCases()
	encoded, err := json.Marshal(pairs)
	if err != nil {
		t.Fatalf("encode cases: %v", err)
	}

	output := frozenPython(t, "fnmatch.golden.json",
		programoracle.Program{Name: "fnmatch", Text: fnMatchOracleProgram, Stdin: encoded})[0]

	var got []struct {
		Pattern    string `json:"pattern"`
		Name       string `json:"name"`
		Match      bool   `json:"match"`
		MatchCase  bool   `json:"matchcase"`
		Translated string `json:"translated"`
	}
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("parse oracle output: %v\n%s", err, output)
	}
	if len(got) != len(pairs) {
		t.Fatalf("oracle returned %d results for %d cases -- every comparison "+
			"below would be checking a subset it chose for itself",
			len(got), len(pairs))
	}

	checked := 0
	for i, pair := range pairs {
		pattern, name := pair[0], pair[1]
		want := got[i]
		if want.Pattern != pattern || want.Name != name {
			t.Fatalf("oracle result %d is for (%q,%q), expected (%q,%q) -- "+
				"the results are not aligned with the inputs",
				i, want.Pattern, want.Name, pattern, name)
		}
		checked++
		if g := pythonparity.FnMatch(name, pattern); g != want.Match {
			t.Errorf("FnMatch(%q, %q) = %v, python %v (translated %q)",
				name, pattern, g, want.Match, want.Translated)
		}
		// fnmatch vs fnmatchcase differ only under a case-folding normcase,
		// which POSIX does not have. If these ever disagree the platform
		// assumption in fnmatch.go is wrong and the helper needs revisiting.
		if want.Match != want.MatchCase {
			t.Errorf("python fnmatch and fnmatchcase disagree on (%q,%q): %v vs %v -- "+
				"the POSIX identity-normcase assumption does not hold here",
				pattern, name, want.Match, want.MatchCase)
		}
	}
	if checked != len(pairs) {
		t.Fatalf("checked %d of %d cases", checked, len(pairs))
	}
}
