package units

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// floatGoldenStringCase is one entry of "string_cases" in
// python_float_python_golden.json: a code-point sequence fed to CPython's
// float(str), the ValueError verdict, and (when it did not raise) the
// resulting bits.
//
// ResultBits is "0000000000000000" -- the bits of 0.0, not a sentinel -- on a
// ValueError row too, because _float_value's except-clause defaults to 0.0.
// FloatRaisesValueError is the field that carries the verdict; ResultBits must
// never be read without checking it first.
type floatGoldenStringCase struct {
	InputCodePoints       []int  `json:"input_codepoints"`
	FloatRaisesValueError bool   `json:"float_raises_value_error"`
	ResultBits            string `json:"result_bits"`
	ResultRepr            string `json:"result_repr"`
}

type floatGolden struct {
	StringCases []floatGoldenStringCase `json:"string_cases"`
	// TypedCases covers non-string inputs handled elsewhere in the port and
	// is deliberately not read here.
}

func loadFloatGolden(t *testing.T) floatGolden {
	t.Helper()
	path := filepath.Join(
		repositoryRootPath(t), "tests", "fixtures",
		"python_float_python_golden.json",
	)
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read float golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_python_float_golden.py)", err)
	}
	var golden floatGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse float golden: %v", err)
	}
	if len(golden.StringCases) == 0 {
		t.Fatal("golden lists no string cases")
	}
	return golden
}

// TestParsePythonFloatMatchesTheInterpreter is the bit-exact parity check
// against measured CPython float(str) output.
//
// Comparison is by math.Float64bits, never by ==: two of the golden's own
// cases would silently pass a "==" comparison for the wrong reason -- NaN != NaN
// under ==, so a test that used == could not tell float("nan") apart from a
// bug that returned any other NaN, and -0.0 == 0.0 under == would hide a
// dropped sign bit on "-0.0" / "-1e-400". Bits make both observable.
func TestParsePythonFloatMatchesTheInterpreter(t *testing.T) {
	golden := loadFloatGolden(t)

	var mismatches int
	for _, tc := range golden.StringCases {
		input := make([]rune, len(tc.InputCodePoints))
		for i, cp := range tc.InputCodePoints {
			input[i] = rune(cp)
		}
		s := string(input)

		got, ok := ParsePythonFloat(s)

		wantOK := !tc.FloatRaisesValueError
		if ok != wantOK {
			if mismatches < 20 {
				t.Errorf("ParsePythonFloat(%q): ok = %v, want %v (repr %s)",
					s, ok, wantOK, tc.ResultRepr)
			}
			mismatches++
			continue
		}
		if !wantOK {
			// CPython raised; _float_value's caller substitutes 0.0 itself,
			// not this function's job to reproduce -- nothing further to
			// compare for this row.
			continue
		}

		wantBits, err := strconv.ParseUint(tc.ResultBits, 16, 64)
		if err != nil {
			t.Fatalf("golden result_bits %q for %q is not 16 hex chars: %v",
				tc.ResultBits, s, err)
		}
		if gotBits := math.Float64bits(got); gotBits != wantBits {
			if mismatches < 20 {
				t.Errorf("ParsePythonFloat(%q) = %v (bits %016x), want bits %016x (repr %s)",
					s, got, gotBits, wantBits, tc.ResultRepr)
			}
			mismatches++
		}
	}
	if mismatches > 20 {
		t.Errorf("... and %d further mismatches", mismatches-20)
	}
}

// TestAsciiFoldingEquivalenceIsSpecificToThisKeywordSet turns the reasoning in
// asciiLower's doc comment into an executable claim.
//
// The comment says strings.EqualFold happens to agree with ASCII folding for
// inf/infinity/nan, because the only ASCII letters reachable by a non-ASCII
// simple fold are 'k' (U+212A) and 's' (U+017F) and neither appears in those
// words. That is true today and it is a property of the WORDS, not of the
// folding — so it stops holding the moment someone adds a keyword containing an
// s or a k.
//
// Prose cannot notice that. This test can, and it is the difference between a
// comment that ages into a lie and one that fails.
func TestAsciiFoldingEquivalenceIsSpecificToThisKeywordSet(t *testing.T) {
	keywords := []string{"inf", "infinity", "nan"}

	// The two ASCII letters a non-ASCII simple fold can produce. Derived here
	// rather than hard-coded, so a Unicode table change is caught rather than
	// silently invalidating the argument.
	reachable := map[rune]rune{}
	for codePoint := rune(0x80); codePoint <= 0x10FFFF; codePoint++ {
		if codePoint >= 0xD800 && codePoint <= 0xDFFF {
			continue
		}
		for folded := unicode.SimpleFold(codePoint); folded != codePoint; folded = unicode.SimpleFold(folded) {
			if folded >= 'a' && folded <= 'z' {
				reachable[folded] = codePoint
			}
		}
	}

	if len(reachable) == 0 {
		t.Fatal("no ASCII letter is reachable by a non-ASCII simple fold; the " +
			"derivation has broken and this test would pass vacuously")
	}

	// The claim: no keyword letter is reachable, therefore EqualFold cannot
	// differ from ASCII folding on these words.
	for _, keyword := range keywords {
		for _, letter := range keyword {
			if source, ok := reachable[letter]; ok {
				t.Errorf(
					"keyword %q contains %q, which U+%04X simple-folds into. "+
						"strings.EqualFold now over-accepts for this keyword set, so "+
						"asciiLower's doc comment is WRONG and the two relations must "+
						"be distinguished explicitly rather than described as equivalent",
					keyword, string(letter), source,
				)
			}
		}
	}

	// And the control: the equivalence really is set-specific. A keyword
	// containing 's' IS over-accepted by EqualFold, which is why asciiLower is
	// used rather than EqualFold even though they agree today.
	if !strings.EqualFold("falſe", "false") {
		t.Error("expected strings.EqualFold to accept \"fal\\u017fe\" as \"false\"; " +
			"if it no longer does, Go's folding has changed and the whole argument " +
			"in asciiLower's comment needs re-deriving")
	}
	if asciiLower("falſe") == "false" {
		t.Error("asciiLower must NOT fold U+017F to 's' -- it folds ASCII only")
	}
}
