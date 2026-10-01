package textrefs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

// TestEveryRuneMatchesLivePythonCharacterClasses is the guard that makes the
// package doc's substitution table an assertion rather than a claim.
//
// It derives Python's `\s`, `\w` and `\d` sets from the LIVE interpreter over
// all 0x110000 code points and compares each against this package's substitute
// predicate. Two different assertions, because the two directions mean
// different things:
//
//   - **python-only must be ZERO, always.** A rune Python treats as a member
//     and Go does not makes the Go side MISS a match Python finds -- a silently
//     dropped edge. There is no acceptable non-zero value here, so this fails
//     hard rather than reporting a count.
//
//   - **go-only must be a SUBSET OF THE UNASSIGNED SET.** These are runes
//     assigned in Go's Unicode tables and not yet existing in CPython's UCD.
//     Asserting the Cn property rather than a count is deliberate: a count
//     fails on any table upgrade and gets "fixed" by editing the number, which
//     would silently absorb a real semantic divergence arriving in the same
//     release. The property stays true across upgrades and fails only on the
//     thing that matters -- a rune Python KNOWS and still excludes, where Go
//     matches and Python does not.
//
// Measured on CPython 3.14.7 / UCD 16.0.0 against Go 1.24, the residue is 0 for
// \s, 4657 for \w and 10 for \d, all Cn. When CPython adopts UCD 17 it shrinks
// toward zero on its own and this test keeps passing.
//
// The marker records BOTH UCD versions, so the parity claim in CI carries the
// pair it was established against rather than being undated.
func TestEveryRuneMatchesLivePythonCharacterClasses(t *testing.T) {
	// Range-encoded so the transfer stays small: \w alone is ~143k code points,
	// and a naive list would dominate the test's runtime for no benefit.
	const derive = `
import json, re, sys, unicodedata

def ranges(pred):
    out, start, prev = [], None, None
    for cp in range(0x110000):
        if pred(chr(cp)):
            if start is None:
                start = cp
            prev = cp
        elif start is not None:
            out.append([start, prev]); start = None
    if start is not None:
        out.append([start, prev])
    return out

print(json.dumps({
    "space": ranges(lambda c: bool(re.match(r"\s", c))),
    "word":  ranges(lambda c: bool(re.match(r"\w", c))),
    "digit": ranges(lambda c: bool(re.match(r"\d", c))),
    # The UNASSIGNED set. The go-only residue must be a SUBSET of this: a rune
    # Go accepts and Python does not is tolerable ONLY when Python has no such
    # code point yet. If Python knows the rune and still excludes it from the
    # class, that is a semantic disagreement and a defect.
    "unassigned": ranges(lambda c: unicodedata.category(c) == "Cn"),
    "unicode": unicodedata.unidata_version,
    "python": sys.version.split()[0],
}))
`
	output := []byte(textrefsProgram(t, "charclass_allrunes", "TestEveryRuneMatchesLivePythonCharacterClasses", "character classes", derive))

	var derived struct {
		Space      [][2]rune `json:"space"`
		Word       [][2]rune `json:"word"`
		Digit      [][2]rune `json:"digit"`
		Unassigned [][2]rune `json:"unassigned"`
		Unicode    string    `json:"unicode"`
		Python     string    `json:"python"`
	}
	if err := json.Unmarshal(output, &derived); err != nil {
		t.Fatalf("decode derived classes: %v", err)
	}
	if len(derived.Space) == 0 || len(derived.Word) == 0 || len(derived.Digit) == 0 {
		// An empty set would make every comparison below trivially pass. This is
		// the vacuity guard: the oracle must have produced something.
		t.Fatalf("live python produced an empty class: space=%d word=%d digit=%d",
			len(derived.Space), len(derived.Word), len(derived.Digit))
	}
	t.Logf("oracle: CPython %s, UCD %s", derived.Python, derived.Unicode)

	expand := func(rs [][2]rune) map[rune]bool {
		set := make(map[rune]bool)
		for _, r := range rs {
			for cp := r[0]; cp <= r[1]; cp++ {
				set[cp] = true
			}
		}
		return set
	}

	unassigned := expand(derived.Unassigned)
	if len(unassigned) == 0 {
		t.Fatal("live python reported no unassigned code points; the Cn subset " +
			"assertion below would be vacuous")
	}

	for _, class := range []struct {
		name        string
		pythonSet   map[rune]bool
		goPredicate func(rune) bool
	}{
		{"\\s", expand(derived.Space), pythonIsSpace},
		{"\\w", expand(derived.Word), pythonIsWord},
		{"\\d", expand(derived.Digit), pythonIsDigit},
	} {
		var pythonOnly, goOnly []rune
		for cp := rune(0); cp <= 0x10FFFF; cp++ {
			inPython := class.pythonSet[cp]
			inGo := class.goPredicate(cp)
			switch {
			case inPython && !inGo:
				pythonOnly = append(pythonOnly, cp)
			case inGo && !inPython:
				goOnly = append(goOnly, cp)
			}
		}

		// Direction 1: a defect. Go must never miss what Python accepts.
		if len(pythonOnly) != 0 {
			sample := pythonOnly
			if len(sample) > 12 {
				sample = sample[:12]
			}
			t.Errorf("%s: %d rune(s) accepted by live Python and REJECTED by the Go "+
				"substitution -- the Go side would MISS a match Python finds, "+
				"silently dropping an edge. First: %U",
				class.name, len(pythonOnly), sample)
		}

		// Direction 2: version skew, asserted as a PROPERTY rather than a count.
		//
		// Every go-only rune must be unassigned (Cn) in the interpreter's UCD.
		// That is the claim the package doc actually makes, and it is the one that
		// stays true across table upgrades: when CPython adopts a newer UCD the
		// residue shrinks toward zero on its own and this still passes, whereas a
		// pinned count would fail and be "fixed" by editing the number -- which
		// would also silently absorb a real semantic divergence arriving in the
		// same release.
		var assignedButExcluded []rune
		for _, r := range goOnly {
			if !unassigned[r] {
				assignedButExcluded = append(assignedButExcluded, r)
			}
		}
		if len(assignedButExcluded) != 0 {
			sample := assignedButExcluded
			if len(sample) > 12 {
				sample = sample[:12]
			}
			t.Errorf("%s: %d rune(s) accepted by the Go substitution that live Python "+
				"KNOWS (assigned in UCD %s) and still excludes from the class. This is "+
				"a semantic disagreement, not version skew: Go would match where Python "+
				"does not. First: %U",
				class.name, len(assignedButExcluded), derived.Unicode, sample)
		}
		t.Logf("%s: go-only residue %d rune(s), all unassigned in UCD %s",
			class.name, len(goOnly), derived.Unicode)
	}
}

// textrefsRepositoryRoot walks up to the module root, for callers that need
// it only to locate the checked-out virtualenv.
func textrefsRepositoryRoot(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find repository root (no go.mod found)")
		}
		directory = parent
	}
}

// textrefsProgram returns the stdout of the inline Python program `text` (run as `python3 -c`). The
// derivations of this package were executed once on the last build that carried the Python sources and
// are frozen in testdata/golden/<slug>.json (recipe in the golden's spec); a frozen run reads the recorded
// stdout and starts no Python. The program's text is part of the request, so a changed derivation is
// refused until it is recorded again.
func textrefsProgram(t *testing.T, slug, test, name, text string) string {
	t.Helper()
	spec := rotguard.Spec("testdata/golden/"+slug+".json", textrefsGoldenPins[slug], "./internal/jobs/workgraph/textrefs/", "^"+test+"$")
	answers := programoracle.Run(t, spec, textrefsRepositoryRoot(t), []programoracle.Program{{Name: name, Text: text}})
	if answers[0].ExitCode != 0 {
		t.Fatalf("the %s program exited %d (stdout %q)", name, answers[0].ExitCode, answers[0].Stdout)
	}
	return answers[0].Stdout
}

// textrefsGoldenPins holds the digest each golden of this package is pinned to (a placeholder until the
// goldenrecord verb records the golden and replaces it).
var textrefsGoldenPins = map[string]string{
	"charclass_allrunes": "PIN:charclass_allrunes",
	"number_allrunes":    "PIN:number_allrunes",
}
