package pythonparity

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// TestLowerAndUpperMatchLivePythonOnEveryMultiRuneMapping derives, from the
// live interpreter and over all 1,114,112 code points, EVERY rune whose
// str.lower() or str.upper() is longer than one rune -- then asserts Lower and
// Upper reproduce each one.
//
// Enumerating rather than sampling is the point: when the oracle is a Unicode
// property, a sample-based test is structurally blind (it can only find the
// divergences someone already thought of), and a hard-coded list is exactly
// the constant that rots when a Unicode revision adds a mapping. This is the
// same derivation the unexported copy in internal/jobs/workgraph/edges used;
// promoting the helper must not weaken it, so it is carried over and WIDENED
// to cover Upper, which had no such guard before.
func TestLowerAndUpperMatchLivePythonOnEveryMultiRuneMapping(t *testing.T) {
	python := requireLivePython(t)

	const derive = `
import json, sys
out = {"lower": {}, "upper": {}}
for cp in range(0x110000):
    c = chr(cp)
    if len(c.lower()) > 1:
        out["lower"][cp] = c.lower()
    if len(c.upper()) > 1:
        out["upper"][cp] = c.upper()
json.dump(out, sys.stdout)
`
	command := exec.Command(python, "-c", derive)
	rendered, err := command.Output()
	if err != nil {
		var stderr []byte
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			stderr = exitErr.Stderr
		}
		t.Fatalf("derive multi-rune case mappings: %v: %s", err, stderr)
	}
	var derived struct {
		Lower map[string]string `json:"lower"`
		Upper map[string]string `json:"upper"`
	}
	if err := json.Unmarshal(rendered, &derived); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Non-vacuity: a broken derivation that returned {} would otherwise pass
	// this whole test by asserting nothing at all.
	if len(derived.Lower) == 0 {
		t.Fatal("live Python reports no multi-rune LOWERCASE mappings; the derivation is broken")
	}
	if len(derived.Upper) == 0 {
		t.Fatal("live Python reports no multi-rune UPPERCASE mappings; the derivation is broken")
	}

	for _, spec := range []struct {
		name    string
		mapping map[string]string
		fn      func(string) string
	}{
		{"Lower", derived.Lower, Lower},
		{"Upper", derived.Upper, Upper},
	} {
		for codePoint, expected := range spec.mapping {
			parsed, err := strconv.ParseInt(codePoint, 10, 32)
			if err != nil {
				t.Fatalf("bad code point %q: %v", codePoint, err)
			}
			value := rune(parsed)
			if got := spec.fn(string(value)); got != expected {
				t.Errorf("%s: U+%04X maps to %q in Python but %q here", spec.name, value, expected, got)
			}
		}
	}
}

// TestLowerHandlesContextSensitiveFinalSigma pins the one divergence that is
// NOT a length change, so the enumeration above cannot see it: a sigma's
// mapping depends on its POSITION, and strings.ToLower gets it wrong while
// producing a same-length, plausible-looking result.
func TestLowerHandlesContextSensitiveFinalSigma(t *testing.T) {
	for _, testCase := range []struct{ input, want string }{
		{"ΟΔΟΣ", "οδος"},   // final position -> final sigma
		{"ΣΟΦΟΣ", "σοφος"}, // both positions in one word
		{"Σ", "σ"},         // lone sigma is NOT final
	} {
		if got := Lower(testCase.input); got != testCase.want {
			t.Errorf("Lower(%q) = %q, want %q", testCase.input, got, testCase.want)
		}
	}
}

// TestLowerAndUpperAreSafeUnderConcurrency is a REGRESSION guard on the
// pooling, not a demonstration that sharing is unsafe.
//
// x/text documents that a Caser may be stateful and must not be shared
// between goroutines (cases.go:35-36; only cases.Fold is exempt, :87), and
// Caser.String calls transform.String, which begins with t.Reset() -- a
// mutation. A package-level shared Caser is therefore a latent race even
// though a 64-goroutine probe over final-sigma inputs did not flag one. Run
// under -race, this asserts the pooled implementation stays correct when
// hammered concurrently; if someone later "simplifies" the pool back into a
// package-level var, this is the test that has a chance of catching it.
func TestLowerAndUpperAreSafeUnderConcurrency(t *testing.T) {
	inputs := []string{"ΟΔΟΣ", "ΣΟΦΟΣ", "İstanbul", "straße", "AI-Assisted", "CHANGES_REQUESTED"}
	wantLower := make([]string, len(inputs))
	wantUpper := make([]string, len(inputs))
	for index, input := range inputs {
		wantLower[index] = Lower(input)
		wantUpper[index] = Upper(input)
	}

	var waitGroup sync.WaitGroup
	failures := make(chan string, 64)
	for goroutine := 0; goroutine < 64; goroutine++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for iteration := 0; iteration < 200; iteration++ {
				for index, input := range inputs {
					if got := Lower(input); got != wantLower[index] {
						failures <- "Lower(" + input + ") = " + got
						return
					}
					if got := Upper(input); got != wantUpper[index] {
						failures <- "Upper(" + input + ") = " + got
						return
					}
				}
			}
		}()
	}
	waitGroup.Wait()
	close(failures)
	for failure := range failures {
		t.Fatalf("concurrent casing produced a wrong result: %s", failure)
	}
}

// TestFinalSigmaLookaheadBoundaryIsWhereWeMeasuredIt pins the ONE known
// divergence between Lower and CPython's str.lower(): x/text's Final_Sigma
// lookahead is bounded at 31 case-ignorable runes, CPython's is unbounded.
//
// It is asserted, not merely documented, for two reasons. First, the
// enumeration test above structurally cannot see it -- that walks single code
// points, and this needs a 33+ rune input. Second, a divergence recorded only
// in prose silently becomes wrong when a dependency bump moves it: if x/text
// ever widens or removes the bound, THIS test fails and tells the next reader
// the doc comment needs updating, rather than leaving a stale measurement that
// reads as current.
//
// The assertion is deliberately two-sided -- n=30 must still agree with CPython
// and n=31 must still differ. A one-sided test would keep passing if the bound
// moved in the direction that made things agree, hiding the change.
func TestFinalSigmaLookaheadBoundaryIsWhereWeMeasuredIt(t *testing.T) {
	python := requireLivePython(t)

	for _, dots := range []int{30, 31} {
		input := "AΣ" + strings.Repeat(".", dots) + "B"
		command := exec.Command(python, "-c", "import sys; sys.stdout.write(sys.argv[1].lower())", input)
		rendered, err := command.Output()
		if err != nil {
			t.Fatalf("live python lower(n=%d): %v", dots, err)
		}
		cpython, got := string(rendered), Lower(input)
		agrees := cpython == got
		switch dots {
		case 30:
			if !agrees {
				t.Errorf("n=30 must still AGREE with CPython (inside x/text's lookahead bound), "+
					"but python=%q go=%q -- the measured boundary has moved", cpython, got)
			}
		case 31:
			if agrees {
				t.Errorf("n=31 now AGREES with CPython, but the recorded measurement says x/text's " +
					"Final_Sigma lookahead gives up here. x/text's behaviour has changed: re-measure " +
					"the boundary and update Lower's doc comment, which still claims 31.")
			}
		}
	}
}

func TestLowerAndUpperPassThroughTheEmptyString(t *testing.T) {
	if got := Lower(""); got != "" {
		t.Fatalf("Lower(\"\") = %q", got)
	}
	if got := Upper(""); got != "" {
		t.Fatalf("Upper(\"\") = %q", got)
	}
}

func requireLivePython(t *testing.T) string {
	t.Helper()
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	python := os.Getenv("PYTHON")
	if python == "" {
		resolved, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required: %v", err)
		}
		python = resolved
	}
	return python
}
