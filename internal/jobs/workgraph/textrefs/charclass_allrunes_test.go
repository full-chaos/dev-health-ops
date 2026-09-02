package textrefs

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"unicode"
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
//
// Proof marker: workgraph-textrefs-charclass-allrunes
func TestEveryRuneMatchesLivePythonCharacterClasses(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	python := os.Getenv("PYTHON")
	if python == "" {
		resolved, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required: %v", err)
		}
		python = resolved
	}

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
	output, err := exec.Command(python, "-c", derive).Output()
	if err != nil {
		t.Fatalf("derive character classes from live python: %v", err)
	}

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

	if err := os.WriteFile(
		filepath.Join(proofDirectory, "workgraph-textrefs-charclass-allrunes"),
		[]byte(fmt.Sprintf("executed ucd_python=%s ucd_go=%s",
			derived.Unicode, unicode.Version)), 0o644,
	); err != nil {
		t.Fatalf("write proof marker: %v", err)
	}
}
