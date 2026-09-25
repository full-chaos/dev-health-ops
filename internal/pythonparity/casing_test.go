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
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
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
		t.Fatalf("derive multi-rune case mappings: %v", pyoracle.RunError(python, err, stderr))
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

// TestFinalSigmaMatchesLivePythonAtAnyDistance holds Lower to CPython's
// str.lower() on capital sigmas whose context lies at every distance around
// x/text's own 31-rune lookahead bound, which Lower no longer relies on:
// medial and final positions, dots, apostrophes and combining marks, the
// scan in both directions, several sigmas in one string, and neighbours with
// multi-rune mappings.
func TestFinalSigmaMatchesLivePythonAtAnyDistance(t *testing.T) {
	python := requireLivePython(t)
	var inputs []string
	for _, n := range []int{0, 1, 2, 30, 31, 32, 33, 64, 1000} {
		for _, pad := range []string{".", "'", "\u0301", "\u00b7", "\u02b0", ".\u0301'"} {
			run := strings.Repeat(pad, n)
			inputs = append(inputs,
				"AΣ"+run+"B",        // a cased letter follows: medial
				"AΣ"+run,            // nothing cased follows: final
				"AΣ"+run+" B",       // a space stops the scan: final
				"A"+run+"Σ",         // cased before, across the run: final
				run+"Σ",             // nothing cased before: medial
				" "+run+"ΣB",        // uncased before: medial
				"ΑΣ"+run+"ΣΟΣ",      // three sigmas, one string
				"İΣ"+run+"ß",        // multi-rune neighbours
				"ǅ"+run+"Σ"+run+"1", // titlecase before, digit after
			)
		}
	}
	encoded, err := json.Marshal(inputs)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-c", "import json, sys; json.dump([s.lower() for s in json.load(sys.stdin)], sys.stdout)")
	command.Stdin = strings.NewReader(string(encoded))
	rendered, err := command.Output()
	if err != nil {
		t.Fatalf("live python lower: %v", pyoracle.RunError(python, err, nil))
	}
	var want []string
	if err := json.Unmarshal(rendered, &want); err != nil || len(want) != len(inputs) {
		t.Fatalf("decode: %v (%d of %d)", err, len(want), len(inputs))
	}
	for index, input := range inputs {
		if got := Lower(input); got != want[index] {
			t.Errorf("Lower(%q) = %q, python %q", input, got, want[index])
		}
	}
	t.Logf("%d inputs compared", len(inputs))
}

// TestSigmaPropertiesMatchLivePythonOnEveryCodePoint derives, for every code
// point c, which sigma CPython writes in three contexts -- c before a sigma,
// c between a cased letter and a sigma, c between a sigma and a cased letter
// -- which together fix c's Case_Ignorable property and, for a character that
// is not case-ignorable, its Cased property. It requires Lower to write the
// same sigma in each context.
//
// Named exclusion, derived rather than listed: Go's unicode tables may be a
// newer Unicode version than the running CPython's, so a code point whose
// general category differs between the two (unassigned in CPython's version,
// or re-categorised since) is skipped and counted.
func TestSigmaPropertiesMatchLivePythonOnEveryCodePoint(t *testing.T) {
	python := requireLivePython(t)
	const derive = `
import sys, unicodedata
bits, cats = [], []
for cp in range(0x110000):
    c = chr(cp)
    b = 0
    if (c + "Σ").lower()[-1] == "ς": b |= 1
    if ("A" + c + "Σ").lower()[-1] == "ς": b |= 2
    if ("AΣ" + c + "B").lower()[1] == "ς": b |= 4
    bits.append(str(b))
    cats.append(unicodedata.category(c))
sys.stdout.write(unicodedata.unidata_version + "\n" + "".join(bits) + "\n" + "".join(cats))
`
	rendered, err := exec.Command(python, "-c", derive).Output()
	if err != nil {
		t.Fatalf("derive sigma properties: %v", pyoracle.RunError(python, err, nil))
	}
	parts := strings.SplitN(string(rendered), "\n", 3)
	if len(parts) != 3 || len(parts[1]) != 0x110000 || len(parts[2]) != 2*0x110000 {
		t.Fatalf("derivation output malformed (%d parts)", len(parts))
	}
	bits, categories := parts[1], parts[2]
	compared, excluded := 0, 0
	for cp := rune(0); cp < 0x110000; cp++ {
		if cp == capitalSigma || (cp >= 0xd800 && cp <= 0xdfff) {
			continue
		}
		if categories[2*cp:2*cp+2] != generalCategory(cp) {
			excluded++
			continue
		}
		c := string(cp)
		got := 0
		if strings.HasSuffix(Lower(c+"Σ"), "ς") {
			got |= 1
		}
		if strings.HasSuffix(Lower("A"+c+"Σ"), "ς") {
			got |= 2
		}
		if strings.HasPrefix(strings.TrimPrefix(Lower("AΣ"+c+"B"), "a"), "ς") {
			got |= 4
		}
		if want := int(bits[cp] - '0'); got != want {
			t.Errorf("U+%04X: go contexts %03b, python %03b", cp, got, want)
		}
		compared++
	}
	if compared < 0x100000 {
		t.Fatalf("only %d code points compared", compared)
	}
	t.Logf("python unicode %s, go unicode %s: %d code points compared, %d skipped for a differing general category",
		parts[0], unicode.Version, compared, excluded)
}

// generalCategory is cp's two-letter general category in Go's tables ("Cn"
// when unassigned), for the exclusion above. "LC" (Lu+Ll+Lt) is a group,
// not a category.
func generalCategory(cp rune) string {
	for name, table := range unicode.Categories {
		if len(name) == 2 && name != "LC" && unicode.Is(table, cp) {
			return name
		}
	}
	return "Cn"
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
	return pyoracle.Resolve(t, parityRepositoryRoot(t))
}
