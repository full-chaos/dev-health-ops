package textrefs

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
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
//   - **go-only is PINNED to a measured number.** These are runes assigned in
//     Go's Unicode tables and unassigned in CPython's UCD, so the count is a
//     property of the version skew between the two, not of this code. A change
//     means one side upgraded its tables; the test says which way and by how
//     much rather than failing mysteriously.
//
// The pinned counts were measured on CPython 3.14.7 / UCD 16.0.0 against Go
// 1.24. When CPython adopts UCD 17 they go to zero without any change here,
// and this test is what will say so.
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
    "unicode": unicodedata.unidata_version,
    "python": sys.version.split()[0],
}))
`
	output, err := exec.Command(python, "-c", derive).Output()
	if err != nil {
		t.Fatalf("derive character classes from live python: %v", err)
	}

	var derived struct {
		Space   [][2]rune `json:"space"`
		Word    [][2]rune `json:"word"`
		Digit   [][2]rune `json:"digit"`
		Unicode string    `json:"unicode"`
		Python  string    `json:"python"`
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

	for _, class := range []struct {
		name        string
		pythonSet   map[rune]bool
		goPredicate func(rune) bool
		// Pinned go-only count. See the doc comment: this is version skew, not
		// a defect, and it is pinned so a table bump is loud.
		pinnedGoOnly int
	}{
		{"\\s", expand(derived.Space), pythonIsSpace, 0},
		{"\\w", expand(derived.Word), pythonIsWord, 4657},
		{"\\d", expand(derived.Digit), pythonIsDigit, 10},
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

		// Direction 2: version skew. Pinned so a table upgrade is announced.
		if len(goOnly) != class.pinnedGoOnly {
			sample := goOnly
			if len(sample) > 12 {
				sample = sample[:12]
			}
			t.Errorf("%s: go-only rune count is %d, pinned at %d. This is Unicode "+
				"VERSION skew, not a defect: these runes are assigned in Go's "+
				"tables and unassigned in CPython's UCD %s. A change means one "+
				"side upgraded. If CPython adopted UCD 17 the count should now "+
				"be 0 and the pin should be updated to 0. First: %U",
				class.name, len(goOnly), class.pinnedGoOnly, derived.Unicode, sample)
		}
	}

	if err := os.WriteFile(
		filepath.Join(proofDirectory, "workgraph-textrefs-charclass-allrunes"),
		[]byte("executed"), 0o644,
	); err != nil {
		t.Fatalf("write proof marker: %v", err)
	}
}
