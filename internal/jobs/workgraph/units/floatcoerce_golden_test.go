package units

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
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
