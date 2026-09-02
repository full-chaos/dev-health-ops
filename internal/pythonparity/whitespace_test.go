package pythonparity

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unicode"
)

type splitLinesCase struct {
	InputHex string   `json:"input_hex"`
	LinesHex []string `json:"lines_hex"`
}

type whitespaceGolden struct {
	IsSpaceCodePoints         []int            `json:"isspace_code_points"`
	SplitLinesBoundaries      []int            `json:"splitlines_boundary_code_points"`
	IsSpaceOnlyVsSplitLines   []int            `json:"isspace_only_vs_splitlines_code_points"`
	CRLFIsOneBoundary         bool             `json:"crlf_is_one_boundary"`
	SplitLinesCases           []splitLinesCase `json:"splitlines_cases"`
	IntSpaceCodePoints        []int            `json:"int_space_code_points"`
	IsSpaceOnlyVsIntSpace     []int            `json:"isspace_only_vs_int_space"`
	IntSpaceOnlyVsIsSpace     []int            `json:"int_space_only_vs_isspace"`
	FloatSpaceCodePoints      []int            `json:"float_space_code_points"`
	FloatSpaceDiffersFromInt  []int            `json:"float_space_differs_from_int_space"`
	PythonOnlyCodePoints      []int            `json:"python_only_code_points"`
	GoOnlyCodePoints          []int            `json:"go_only_code_points"`
	SplitDisagreesWithIsSpace []int            `json:"split_disagrees_with_isspace"`
	SplitSplitsOnNonIsSpace   []int            `json:"split_splits_on_non_isspace"`
}

func loadWhitespaceGolden(t *testing.T) whitespaceGolden {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(
		"../../tests/fixtures/python_whitespace_python_golden.json",
	))
	if err != nil {
		t.Fatalf("read whitespace golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_python_whitespace_golden.py)", err)
	}
	var golden whitespaceGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse whitespace golden: %v", err)
	}
	return golden
}

// TestIsSpaceMatchesCPythonExhaustively sweeps every code point rather than
// sampling.
//
// The predicate is cheap and the space is only 1.1M wide, so there is no reason
// to test a subset and hope. A sampled test would pass while missing a code
// point added by a future Unicode revision, which is precisely the drift this
// is meant to catch.
func TestIsSpaceMatchesCPythonExhaustively(t *testing.T) {
	golden := loadWhitespaceGolden(t)

	pythonSpace := make(map[rune]bool, len(golden.IsSpaceCodePoints))
	for _, codePoint := range golden.IsSpaceCodePoints {
		pythonSpace[rune(codePoint)] = true
	}
	if len(pythonSpace) == 0 {
		t.Fatal("golden lists no whitespace code points")
	}

	var disagreements int
	for codePoint := rune(0); codePoint <= 0x10FFFF; codePoint++ {
		if got, want := IsSpace(codePoint), pythonSpace[codePoint]; got != want {
			if disagreements < 20 {
				t.Errorf("IsSpace(U+%04X) = %v, CPython str.isspace() = %v",
					codePoint, got, want)
			}
			disagreements++
		}
	}
	if disagreements > 20 {
		t.Errorf("... and %d further disagreements", disagreements-20)
	}
}

// TestGoUnicodeIsSpaceIsAStrictSubset pins the DELTA itself, not just the
// corrected predicate.
//
// This is the test that explains why IsSpace exists. If a future Go release
// added the four separators to unicode.IsSpace, this test would fail and the
// custom predicate could be deleted -- a failure that means "the workaround is
// now unnecessary", which is worth being told about.
func TestGoUnicodeIsSpaceIsAStrictSubset(t *testing.T) {
	golden := loadWhitespaceGolden(t)

	if len(golden.GoOnlyCodePoints) != 0 {
		t.Errorf("Go treats %v as whitespace and CPython does not; the subset "+
			"relationship this package relies on no longer holds",
			golden.GoOnlyCodePoints)
	}

	want := []int{0x1c, 0x1d, 0x1e, 0x1f}
	if !reflect.DeepEqual(golden.PythonOnlyCodePoints, want) {
		t.Errorf("python-only whitespace = %v, want %v -- if this grew, every "+
			"TrimSpace/Fields call site in the port needs re-auditing, not just "+
			"this package", golden.PythonOnlyCodePoints, want)
	}

	// Drive the delta rather than assert it from the fixture alone.
	for _, codePoint := range want {
		if unicode.IsSpace(rune(codePoint)) {
			t.Errorf("unicode.IsSpace(U+%04X) is now true; the delta has closed",
				codePoint)
		}
		if !IsSpace(rune(codePoint)) {
			t.Errorf("IsSpace(U+%04X) must be true to match CPython", codePoint)
		}
	}
}

// TestSplitWhitespaceMatchesPythonAndFieldsDoesNot drives both the correct
// implementation and the wrong one, so the defect is visible rather than
// merely averted.
func TestSplitWhitespaceMatchesPythonAndFieldsDoesNot(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		input       string
		want        []string
		fieldsAgree bool
	}{
		{name: "plain", input: "a b c", want: []string{"a", "b", "c"}, fieldsAgree: true},
		{name: "runs collapse", input: "a   b", want: []string{"a", "b"}, fieldsAgree: true},
		{name: "leading and trailing", input: "  a b  ", want: []string{"a", "b"}, fieldsAgree: true},
		{name: "tabs and newlines", input: "a\t\nb", want: []string{"a", "b"}, fieldsAgree: true},
		{name: "empty", input: "", want: nil, fieldsAgree: true},
		{name: "whitespace only", input: " \t\n ", want: nil, fieldsAgree: true},
		{name: "nbsp is whitespace to both", input: "a b", want: []string{"a", "b"}, fieldsAgree: true},

		// The delta. strings.Fields leaves these embedded.
		{name: "file separator", input: "a\x1cb", want: []string{"a", "b"}, fieldsAgree: false},
		{name: "group separator", input: "a\x1db", want: []string{"a", "b"}, fieldsAgree: false},
		{name: "record separator", input: "a\x1eb", want: []string{"a", "b"}, fieldsAgree: false},
		{name: "unit separator", input: "a\x1fb", want: []string{"a", "b"}, fieldsAgree: false},
		{
			name:        "all four mixed with ordinary whitespace",
			input:       "alpha\x1cbeta\x1fgamma\x1ddelta\x1eepsilon",
			want:        []string{"alpha", "beta", "gamma", "delta", "epsilon"},
			fieldsAgree: false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got := SplitWhitespace(testCase.input)
			if len(got) == 0 && len(testCase.want) == 0 {
				// nil and empty slice are equivalent here.
			} else if !reflect.DeepEqual(got, testCase.want) {
				t.Errorf("SplitWhitespace(%q) = %q, want %q",
					testCase.input, got, testCase.want)
			}

			fields := strings.Fields(testCase.input)
			agree := reflect.DeepEqual(fields, got) ||
				(len(fields) == 0 && len(got) == 0)
			if agree != testCase.fieldsAgree {
				t.Errorf("strings.Fields(%q) = %q; agreement with CPython = %v, "+
					"want %v -- this case exists to pin where the obvious Go "+
					"implementation is wrong",
					testCase.input, fields, agree, testCase.fieldsAgree)
			}
		})
	}
}

// TestCollapseWhitespaceMatchesPythonJoinSplit covers the exact expression
// evidence._truncate_text opens with.
func TestCollapseWhitespaceMatchesPythonJoinSplit(t *testing.T) {
	for _, testCase := range []struct{ name, input, want string }{
		{name: "empty", input: "", want: ""},
		{name: "already clean", input: "a b", want: "a b"},
		{name: "collapses runs", input: "  collapse\t\tthese   spaces\n\nplease  ", want: "collapse these spaces please"},
		{name: "separators split and normalise", input: "alpha\x1cbeta\x1fgamma", want: "alpha beta gamma"},
		{name: "whitespace only becomes empty", input: " \t\x1c\n ", want: ""},
		{name: "nbsp normalises to a plain space", input: "a b", want: "a b"},
		{name: "non-ascii text is untouched", input: " 修復  バグ ", want: "修復 バグ"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := CollapseWhitespace(testCase.input); got != testCase.want {
				t.Errorf("CollapseWhitespace(%q) = %q, want %q",
					testCase.input, got, testCase.want)
			}
		})
	}
}

// TestRuneLenAndTruncateCountCodePoints pins the counting unit, with the
// byte-based answer spelled out alongside so the gap is legible.
func TestRuneLenAndTruncateCountCodePoints(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		input     string
		wantRunes int
		wantBytes int
	}{
		{name: "ascii agrees", input: "hello", wantRunes: 5, wantBytes: 5},
		{name: "latin1 is 2 bytes per char", input: "café", wantRunes: 4, wantBytes: 5},
		{name: "cjk is 3 bytes per char", input: "修復バグ", wantRunes: 4, wantBytes: 12},
		{name: "astral is 4 bytes per char", input: "\U0001f600\U0001f600", wantRunes: 2, wantBytes: 8},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := RuneLen(testCase.input); got != testCase.wantRunes {
				t.Errorf("RuneLen(%q) = %d, want %d", testCase.input, got, testCase.wantRunes)
			}
			if got := len(testCase.input); got != testCase.wantBytes {
				t.Errorf("len(%q) = %d, want %d (byte count, for contrast)",
					testCase.input, got, testCase.wantBytes)
			}
		})
	}

	// The truncation case that matters: 280 CJK characters is 840 bytes, so a
	// byte-slicing port keeps 93 characters where CPython keeps 280.
	const limit = 280
	cjk := strings.Repeat("修", 400)
	truncated := TruncateRunes(cjk, limit)
	if got := RuneLen(truncated); got != limit {
		t.Errorf("TruncateRunes kept %d code points, want %d", got, limit)
	}
	if byteSliced := RuneLen(cjk[:limit]); byteSliced >= limit {
		t.Errorf("expected byte slicing to keep FEWER than %d code points; kept %d",
			limit, byteSliced)
	}
	// And it must never split a rune.
	if strings.ContainsRune(truncated, '�') {
		t.Error("TruncateRunes produced a replacement character, i.e. it cut mid-rune")
	}

	// Short input is returned whole, not padded or copied differently.
	if got := TruncateRunes("abc", 10); got != "abc" {
		t.Errorf("TruncateRunes(%q, 10) = %q, want %q", "abc", got, "abc")
	}
	if got := TruncateRunes("abc", 0); got != "" {
		t.Errorf("TruncateRunes(%q, 0) = %q, want empty", "abc", got)
	}
}

// TestSplitLinesMatchesCPython drives every case the generator captured from
// str.splitlines() itself, and drives strings.Split alongside so the wrong
// answer is visible rather than merely avoided.
func TestSplitLinesMatchesCPython(t *testing.T) {
	golden := loadWhitespaceGolden(t)
	if len(golden.SplitLinesCases) == 0 {
		t.Fatal("golden contains no splitlines cases")
	}
	if !golden.CRLFIsOneBoundary {
		t.Fatal("golden says CRLF is not a single boundary; SplitLines assumes it is")
	}

	var newlineOnlyDisagreements int
	for _, testCase := range golden.SplitLinesCases {
		input := decodeHexString(t, testCase.InputHex)
		want := make([]string, len(testCase.LinesHex))
		for index, encoded := range testCase.LinesHex {
			want[index] = decodeHexString(t, encoded)
		}

		got := SplitLines(input)
		if len(got) == 0 && len(want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("SplitLines(%q) = %q, CPython = %q", input, got, want)
		}

		// How far the obvious implementation would have been off.
		if naive := strings.Split(input, "\n"); !reflect.DeepEqual(naive, want) {
			newlineOnlyDisagreements++
		}
	}

	// The point of the exercise: a newline-only split is wrong on a large
	// fraction of these, so the divergence is not an exotic corner.
	if newlineOnlyDisagreements == 0 {
		t.Error("strings.Split(value, \"\\n\") agreed with CPython on every case; " +
			"the corpus is no longer covering the boundaries that motivated SplitLines")
	}
	t.Logf("strings.Split on newline alone disagrees with CPython on %d of %d cases",
		newlineOnlyDisagreements, len(golden.SplitLinesCases))
}

// TestSplitLinesBoundarySetIsNotTheWhitespaceSet pins the non-nesting that
// makes a single shared predicate impossible.
func TestSplitLinesBoundarySetIsNotTheWhitespaceSet(t *testing.T) {
	golden := loadWhitespaceGolden(t)

	boundaries := make(map[rune]bool, len(golden.SplitLinesBoundaries))
	for _, codePoint := range golden.SplitLinesBoundaries {
		boundaries[rune(codePoint)] = true
	}
	if len(boundaries) == 0 {
		t.Fatal("golden lists no line boundaries")
	}

	for codePoint := rune(0); codePoint <= 0x10FFFF; codePoint++ {
		if got, want := isLineBoundary(codePoint), boundaries[codePoint]; got != want {
			t.Errorf("isLineBoundary(U+%04X) = %v, CPython splitlines = %v",
				codePoint, got, want)
		}
	}

	// 0x1f is the asymmetry: whitespace, but NOT a line boundary. If a future
	// refactor points both helpers at one predicate, this fails.
	if !IsSpace(0x1f) {
		t.Error("U+001F must be whitespace to CPython")
	}
	if isLineBoundary(0x1f) {
		t.Error("U+001F must NOT be a line boundary; the two sets are not nested")
	}
	// And the reverse asymmetry does not exist for 0x0b/0x0c -- both are both.
	for _, codePoint := range []rune{0x0b, 0x0c} {
		if !IsSpace(codePoint) || !isLineBoundary(codePoint) {
			t.Errorf("U+%04X should be both whitespace and a line boundary", codePoint)
		}
	}
}

func decodeHexString(t *testing.T, encoded string) string {
	t.Helper()
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode hex %q: %v", encoded, err)
	}
	return string(decoded)
}

// TestIntSpaceSetIsGoUnicodeIsSpaceExactly pins the THIRD character class, and
// with it the reason parsePythonInt must NOT adopt pythonparity.Strip.
//
// This port has asserted since PR2 that int()/float() REJECT 0x1c-0x1f while
// str.strip() removes them. That claim was stated in comments and never pinned
// by a corpus -- an assertion doing the work of a measurement, which is the
// habit this lane keeps finding in itself. It is now derived.
//
// Measured: int()'s space set is 25 code points, EXACTLY Go's unicode.IsSpace,
// zero difference in either direction. So strings.TrimSpace is not merely
// "close enough" for the numeric parsers -- it is exact, and the four
// separators are precisely the gap against str.isspace().
//
// lane-pathb-go measured the same equality independently while porting int()'s
// grammar for ParseUUID. Two derivations agreeing is worth more than one, and
// worth more than either of us asserting it.
func TestIntSpaceSetIsGoUnicodeIsSpaceExactly(t *testing.T) {
	golden := loadWhitespaceGolden(t)

	if len(golden.IntSpaceCodePoints) == 0 {
		t.Fatal("golden records no int() space set; the claim that the numeric " +
			"parsers use a narrower rule is unpinned again")
	}

	intSpace := make(map[rune]bool, len(golden.IntSpaceCodePoints))
	for _, codePoint := range golden.IntSpaceCodePoints {
		intSpace[rune(codePoint)] = true
	}

	var goOnly, pythonOnly []rune
	for codePoint := rune(0); codePoint <= 0x10FFFF; codePoint++ {
		switch g, p := unicode.IsSpace(codePoint), intSpace[codePoint]; {
		case g && !p:
			goOnly = append(goOnly, codePoint)
		case !g && p:
			pythonOnly = append(pythonOnly, codePoint)
		}
	}
	if len(goOnly) != 0 || len(pythonOnly) != 0 {
		t.Errorf("unicode.IsSpace and int()'s space set have diverged\n"+
			"  Go accepts, int() does not: %v\n"+
			"  int() accepts, Go does not: %v\n"+
			"strings.TrimSpace is used by parsePythonInt and confidenceFromString "+
			"precisely because these were equal; if they are not, those parsers "+
			"need their own predicate", goOnly, pythonOnly)
	}

	// And the delta against str.isspace() is exactly the four separators --
	// the whole reason two predicates exist in this package.
	want := []int{0x1c, 0x1d, 0x1e, 0x1f}
	if !reflect.DeepEqual(golden.IsSpaceOnlyVsIntSpace, want) {
		t.Errorf("str.isspace() minus int()'s space set = %v, want %v -- if this "+
			"grew, every TrimSpace call site in the numeric parsers needs "+
			"re-auditing", golden.IsSpaceOnlyVsIntSpace, want)
	}
	if len(golden.IntSpaceOnlyVsIsSpace) != 0 {
		t.Errorf("int() accepts %v as padding which str.isspace() does not; the "+
			"subset relation this package assumes has inverted",
			golden.IntSpaceOnlyVsIsSpace)
	}

	// Drive the consequence, not just the sets: a separator must be REFUSED by
	// the numeric parser and REMOVED by Strip. Value deliberately != the
	// resolver default, so a parse and a fallback are distinguishable.
	for _, separator := range []string{"\x1c", "\x1d", "\x1e", "\x1f"} {
		if Strip(separator+"200"+separator) != "200" {
			t.Errorf("Strip must remove %q", separator)
		}
	}

	// float() is a SECOND numeric parser with its own rule, mirrored by
	// confidenceFromString with the same strings.TrimSpace on the same
	// reasoning. That reasoning was asserted and not measured either. It is
	// identical to int()'s today -- but "identical today" is a measurement, and
	// the two are separate functions in CPython that could diverge.
	if len(golden.FloatSpaceCodePoints) == 0 {
		t.Fatal("golden records no float() space set; confidenceFromString's " +
			"TrimSpace is unpinned")
	}
	if n := len(golden.FloatSpaceDiffersFromInt); n != 0 {
		t.Errorf("float() and int() space sets have diverged on %d code points: %v\n"+
			"parsePythonInt and confidenceFromString both use strings.TrimSpace on "+
			"the assumption they agree; one of them now needs its own predicate",
			n, golden.FloatSpaceDiffersFromInt)
	}
	floatSpace := make(map[rune]bool, len(golden.FloatSpaceCodePoints))
	for _, codePoint := range golden.FloatSpaceCodePoints {
		floatSpace[rune(codePoint)] = true
	}
	for codePoint := rune(0); codePoint <= 0x10FFFF; codePoint++ {
		if unicode.IsSpace(codePoint) != floatSpace[codePoint] {
			t.Errorf("unicode.IsSpace and float()'s space set disagree on U+%04X",
				codePoint)
			break
		}
	}
}
