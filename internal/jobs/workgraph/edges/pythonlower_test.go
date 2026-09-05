package edges

import (
	"encoding/json"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestPythonLowerMatchesStrLowerOnTheKnownDivergences pins the two cases where
// Go's simple case mapping and Python's full case mapping disagree, in opposite
// directions. Both change which branch of CanonicalDependency a row takes.
func TestPythonLowerMatchesStrLowerOnTheKnownDivergences(t *testing.T) {
	// Measured against the live interpreter; the rot guard below re-derives.
	cases := map[string]struct{ input, python string }{
		"ascii":            {"BLOCKS", "blocks"},
		"turkish dotted I": {"İS_BLOCKED_BY", "i̇s_blocked_by"},
		"kelvin sign":      {"BLOCKS", "blocks"},
		"already lower":    {"is_blocked_by", "is_blocked_by"},
		"mixed":            {"Is_Blocked_By", "is_blocked_by"},
	}
	for name, testCase := range cases {
		if got := pythonparity.Lower(testCase.input); got != testCase.python {
			t.Errorf("%s: pythonparity.Lower(%q) = %q, python gives %q",
				name, testCase.input, got, testCase.python)
		}
	}

	// The point of the exercise: blocker-set membership must agree, because that
	// is what decides the branch.
	for name, testCase := range cases {
		_, goSays := blockerTypes[pythonparity.Lower(testCase.input)]
		_, pythonSays := blockerTypes[testCase.python]
		if goSays != pythonSays {
			t.Errorf("%s: blocker membership differs (go=%v python=%v) for %q",
				name, goSays, pythonSays, testCase.input)
		}
	}

	// And the two naive alternatives must each be WRONG on one of them, or this
	// function is solving a problem that does not exist.
	if strings.ToLower("İS_BLOCKED_BY") == "i̇s_blocked_by" {
		t.Error("strings.ToLower now matches Python on U+0130; pythonLower may be unnecessary")
	}
	if _, wrong := blockerTypes[strings.ToLower("İS_BLOCKED_BY")]; !wrong {
		t.Error("strings.ToLower no longer folds U+0130 into the blocker set; the premise changed")
	}
}

// TestPythonLowerMatchesLivePython re-derives the set of runes whose lowercase
// mapping is longer than one rune — the entire difference between Go's simple
// mapping and Python's full mapping — and requires pythonLower to handle exactly
// those.
//
// Hard-coding U+0130 is correct today and is the kind of constant that rots: a
// Unicode revision adding another multi-char lowercase mapping would silently
// reintroduce the divergence in whichever direction the new rune falls.
func TestPythonLowerMatchesLivePython(t *testing.T) {
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
	const derive = `
import json, sys
out = {}
for cp in range(0x110000):
    c = chr(cp)
    low = c.lower()
    if len(low) > 1:
        out[cp] = low
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
		t.Fatalf("derive multi-rune lowercase mappings: %v: %s", err, stderr)
	}
	var multi map[string]string
	if err := json.Unmarshal(rendered, &multi); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(multi) == 0 {
		t.Fatal("live Python reports no multi-rune lowercase mappings; the derivation is broken")
	}

	// Every one of them must round-trip through pythonLower to what Python gives.
	// A rune this function does not special-case will fail here rather than in
	// production, which is the whole point.
	for codePoint, expected := range multi {
		var value rune
		if _, err := parseCodePoint(codePoint, &value); err != nil {
			t.Fatalf("bad code point %q: %v", codePoint, err)
		}
		if got := pythonparity.Lower(string(value)); got != expected {
			t.Errorf(
				"U+%04X lowercases to %q in Python but %q here; pythonLower does not handle it, "+
					"so a relationship_type containing it would take a different branch than Python",
				value, expected, got,
			)
		}
	}
	if err := os.WriteFile(
		filepath.Join(proofDirectory, "workgraph-python-lower"), []byte("executed"), 0o644,
	); err != nil {
		t.Fatalf("write proof: %v", err)
	}
	t.Logf("pythonLower matches live str.lower() on all %d multi-rune mappings", len(multi))
}

func parseCodePoint(value string, into *rune) (rune, error) {
	number := 0
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, errors.New("not a decimal code point")
		}
		number = number*10 + int(r-'0')
	}
	*into = rune(number)
	return *into, nil
}

// TestPythonLowerHandlesContextSensitiveFinalSigma covers the axis the derived
// rot guard above cannot reach.
//
// That guard enumerates every MULTI-RUNE lowercase mapping from the live
// interpreter — properly derived, not hand-written — and is still blind here,
// because final sigma is not a multi-rune mapping. It is a single rune whose
// mapping depends on its POSITION in the string. The guard varied the rune
// exhaustively and held position constant.
//
// Deriving a corpus from the reference removes the transcription risk, not the
// axis risk: a generator can only enumerate along axes its author iterated.
//
// Positions covered: final, medial, both-in-one-string, and final-followed-by-a
// case-ignorable (a trailing '.' or apostrophe still leaves the sigma
// word-final to Unicode's rule).
func TestPythonLowerHandlesContextSensitiveFinalSigma(t *testing.T) {
	for _, testCase := range []struct{ input, python, position string }{
		{"ΟΔΟΣ", "οδος", "final"},
		{"ΣΟΦΟΣ", "σοφος", "initial and final in one string"},
		{"ΑΣΒ", "ασβ", "medial — NOT final, stays σ"},
		{"ΑΣ", "ας", "final, two runes"},
		{"ΑΣ.", "ας.", "final followed by a case-ignorable period"},
		{"ΑΣ'", "ας'", "final followed by a case-ignorable apostrophe"},
	} {
		if got := pythonparity.Lower(testCase.input); got != testCase.python {
			t.Errorf("%s: pythonparity.Lower(%q) = %q, python .lower() gives %q",
				testCase.position, testCase.input, got, testCase.python)
		}
	}
}

// TestTheReadPathCarriesAnInstantNotAString pins the property CHAOS-4819 was
// closed on.
//
// That ticket was closed as unreachable-by-construction on the argument "the
// production path carries a time.Time, so no ISO parse exists". Nothing
// enforced it: zero tests pinned the type. If LastSynced becomes a string
// again, a parser comes back with it and the whole fromisoformat accept-set
// problem returns — silently, because nothing else in this package would fail.
//
// A compile-time pin rather than a behavioural test: the string field is what
// FORCES a parse, so pinning the type closes the reintroduction path at its
// root instead of testing for parser symptoms.
func TestTheReadPathCarriesAnInstantNotAString(t *testing.T) {
	var row DependencyRow
	var _ time.Time = row.LastSynced // a string here stops the build
}

// unicodeVersionSkewRunes is the EXACT set of code points where
// `cases.Lower(language.Und)` disagrees with the deployed CPython's
// `str.lower()`, derived by comparing both planes over all 0x110000.
//
// They are Unicode 17 additions that x/text lowercases and CPython's UCD 16
// treats as unassigned and therefore leaves alone: three Latin Extended-D
// letters and the twenty-five Old Uyghur letters.
//
// This is divergence #4 in Divergences (edges.go). It exists because the fix
// for the previous Unicode defect swapped one oracle for another: replacing
// `strings.ToLower` with `cases.Lower` moved authority from Go's stdlib table
// to x/text's, and x/text is a different Unicode version from the interpreter
// this port must match.
var unicodeVersionSkewRunes = []rune{
	0xA7CE, 0xA7D2, 0xA7D4,
	0x16EA0, 0x16EA1, 0x16EA2, 0x16EA3, 0x16EA4, 0x16EA5, 0x16EA6, 0x16EA7,
	0x16EA8, 0x16EA9, 0x16EAA, 0x16EAB, 0x16EAC, 0x16EAD, 0x16EAE, 0x16EAF,
	0x16EB0, 0x16EB1, 0x16EB2, 0x16EB3, 0x16EB4, 0x16EB5, 0x16EB6, 0x16EB7,
	0x16EB8,
}

// TestEveryRuneLowercasesLikeLivePython compares BOTH planes over EVERY code
// point and pins the disagreement set exactly.
//
// # Why this replaces a multi-rune-only guard
//
// The previous guard enumerated only multi-rune lowercase mappings. That is a
// real property derived from the interpreter, and it was still blind twice: to
// context-sensitive final sigma (a one-rune mapping that depends on position),
// and to Unicode VERSION SKEW (a one-rune mapping that exists in one plane and
// not the other). Both slipped through because the guard varied the thing it
// knew to vary.
//
// Enumerating every code point removes the axis question entirely: there is no
// sampling decision left to get wrong.
//
// A change in EITHER direction fails. More disagreements means the port drifted
// or x/text moved; fewer means CPython caught up, and then divergence #4 should
// be deleted rather than quietly shrinking.
//
// Proof marker: workgraph-python-lower-allrunes
func TestEveryRuneLowercasesLikeLivePython(t *testing.T) {
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

	const derive = `
import json, sys, unicodedata
mapping = {}
for cp in range(0x110000):
    c = chr(cp)
    low = c.lower()
    if low != c:
        mapping[cp] = [ord(x) for x in low]
print(json.dumps({"mapping": mapping, "unicode": unicodedata.unidata_version}))
`
	output, err := exec.Command(python, "-c", derive).Output()
	if err != nil {
		t.Fatalf("derive full lower mapping from live python: %v", err)
	}
	var live struct {
		Mapping map[string][]int `json:"mapping"`
		Unicode string           `json:"unicode"`
	}
	if err := json.Unmarshal(output, &live); err != nil {
		t.Fatalf("decode derivation: %v", err)
	}
	if len(live.Mapping) == 0 {
		t.Fatal("derivation produced no mappings; a silently empty result would make this vacuous")
	}
	t.Logf("python unicode %s: %d runes have a non-identity lower()", live.Unicode, len(live.Mapping))

	expected := make(map[rune]struct{}, len(unicodeVersionSkewRunes))
	for _, r := range unicodeVersionSkewRunes {
		expected[r] = struct{}{}
	}

	var unexpected, missing []rune
	for codePoint := rune(0); codePoint <= 0x10FFFF; codePoint++ {
		want := string(codePoint)
		if mapped, present := live.Mapping[strconv.Itoa(int(codePoint))]; present {
			runes := make([]rune, 0, len(mapped))
			for _, value := range mapped {
				runes = append(runes, rune(value))
			}
			want = string(runes)
		}
		_, isKnown := expected[codePoint]
		if pythonparity.Lower(string(codePoint)) != want {
			if !isKnown {
				unexpected = append(unexpected, codePoint)
			}
		} else if isKnown {
			missing = append(missing, codePoint)
		}
	}

	for _, codePoint := range unexpected {
		t.Errorf("U+%04X: pythonLower gives %q, live python maps it to %v — a NEW disagreement, so "+
			"either this port drifted or x/text's Unicode table moved. It is not in the pinned "+
			"skew set and must not be added without measuring why",
			codePoint, pythonparity.Lower(string(codePoint)), live.Mapping[strconv.Itoa(int(codePoint))])
	}
	for _, codePoint := range missing {
		t.Errorf("U+%04X is pinned as a version-skew divergence but the two planes now AGREE — "+
			"CPython has caught up, so divergence #4 should shrink or be deleted rather than "+
			"silently carrying a stale entry", codePoint)
	}

	if err := os.WriteFile(
		filepath.Join(proofDirectory, "workgraph-python-lower-allrunes"), []byte("executed"), 0o644,
	); err != nil {
		t.Fatalf("write proof marker: %v", err)
	}
}
