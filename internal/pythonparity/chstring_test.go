package pythonparity

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"unicode/utf8"
)

type decodeCase struct {
	Label       string `json:"label"`
	RawHex      string `json:"raw_hex"`
	PythonValid bool   `json:"python_valid_utf8"`
	DecodedHex  string `json:"decoded_hex"`
}

func loadDecodeGolden(t *testing.T) []decodeCase {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(
		"../../tests/fixtures/clickhouse_string_decode_python_golden.json",
	))
	if err != nil {
		t.Fatalf("read decode golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_clickhouse_string_decode_golden.py)", err)
	}
	var golden struct {
		Cases []decodeCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse decode golden: %v", err)
	}
	if len(golden.Cases) == 0 {
		t.Fatal("golden contains no cases")
	}
	return golden.Cases
}

// TestDecodeClickHouseStringMatchesPythonDriver drives every corpus case.
func TestDecodeClickHouseStringMatchesPythonDriver(t *testing.T) {
	cases := loadDecodeGolden(t)

	var substituted int
	for _, testCase := range cases {
		rawBytes, err := hex.DecodeString(testCase.RawHex)
		if err != nil {
			t.Fatalf("%s: decode raw hex: %v", testCase.Label, err)
		}
		wantBytes, err := hex.DecodeString(testCase.DecodedHex)
		if err != nil {
			t.Fatalf("%s: decode expected hex: %v", testCase.Label, err)
		}
		want := string(wantBytes)

		if got := DecodeClickHouseString(rawBytes); got != want {
			t.Errorf("%s: DecodeClickHouseString(%x) = %q, python = %q",
				testCase.Label, rawBytes, got, want)
		}
		// The string-valued form the fetchers actually use must agree.
		if got := DecodeClickHouseStringValue(string(rawBytes)); got != want {
			t.Errorf("%s: DecodeClickHouseStringValue = %q, python = %q",
				testCase.Label, got, want)
		}
		if !testCase.PythonValid {
			substituted++
		}
	}

	if substituted < 100 {
		t.Errorf("only %d invalid-UTF-8 cases exercised the substitution path; "+
			"the corpus should carry far more", substituted)
	}
}

// TestGoAndPythonAgreeOnWhatIsValidUTF8 is the assumption this port rests on,
// checked rather than asserted.
//
// Given the measured policy, the Go implementation reduces to "is this valid
// UTF-8, and if not, hex it". That is correct ONLY if utf8.Valid accepts
// exactly the byte sequences Python's bytes.decode("utf-8") accepts. Both
// describe themselves as strict UTF-8, but "both are strict" is precisely the
// sort of generalisation that has already been wrong twice in this lane -- for
// str.isspace() and for the json.dumps escape table -- so the two acceptance
// sets are compared sequence by sequence.
//
// The corpus deliberately includes the shapes where implementations differ in
// practice: overlong encodings, UTF-8-encoded surrogates (WTF-8), CESU-8
// surrogate pairs, code points above U+10FFFF, and five-byte sequences.
func TestGoAndPythonAgreeOnWhatIsValidUTF8(t *testing.T) {
	cases := loadDecodeGolden(t)

	var disagreements int
	for _, testCase := range cases {
		rawBytes, err := hex.DecodeString(testCase.RawHex)
		if err != nil {
			t.Fatalf("%s: decode raw hex: %v", testCase.Label, err)
		}
		if goValid := utf8.Valid(rawBytes); goValid != testCase.PythonValid {
			t.Errorf("%s (%x): utf8.Valid = %v, python decode succeeds = %v -- "+
				"the acceptance sets differ, so DecodeClickHouseString cannot be "+
				"written as a validity check plus hex",
				testCase.Label, rawBytes, goValid, testCase.PythonValid)
			disagreements++
		}
	}
	if disagreements > 0 {
		t.Errorf("%d sequences disagree; the port needs Python's decoder, not Go's "+
			"validity check", disagreements)
	}
}

// TestSubstitutionIsWholeValueNotPerByte pins the detail most likely to be
// "improved" into a bug: a mixed value is hexed ENTIRELY, including the parts
// that were valid.
func TestSubstitutionIsWholeValueNotPerByte(t *testing.T) {
	got := DecodeClickHouseString([]byte("a\xffb"))
	if got != "61ff62" {
		t.Errorf("DecodeClickHouseString(a\\xffb) = %q, want %q -- the whole value "+
			"is hexed, not just the invalid byte", got, "61ff62")
	}
	// And the output is pure ASCII, which is why this belongs at the reader
	// rather than in MarshalPythonJSON.
	for index := 0; index < len(got); index++ {
		if got[index] >= 0x80 {
			t.Fatalf("substituted value must be pure ASCII; byte %d is 0x%02x",
				index, got[index])
		}
	}
	// A valid value is returned untouched, not re-encoded.
	if got := DecodeClickHouseString([]byte("修")); got != "修" {
		t.Errorf("valid UTF-8 must pass through unchanged, got %q", got)
	}
}
