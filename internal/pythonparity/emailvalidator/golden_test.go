package emailvalidator

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// goldenPath holds a slice of pydantic's own answers, cut from the live
// oracle run (TestValidateEmailMatchesLivePydantic checks it is still
// exactly what the live interpreter says), so the ordinary test run pins
// the port without Python.
const goldenPath = "testdata/validate_email_golden.jsonl"

type goldenCase struct {
	Input  []rune `json:"input"`
	OK     bool   `json:"ok"`
	Email  []rune `json:"email,omitempty"`
	Reason string `json:"reason,omitempty"`
}

func TestValidateEmailMatchesGolden(t *testing.T) {
	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var c goldenCase
		if err := json.Unmarshal(line, &c); err != nil {
			t.Fatal(err)
		}
		cases = append(cases, c)
	}
	if len(cases) < 1000 {
		t.Fatalf("golden holds %d cases; it was cut short", len(cases))
	}
	failures := 0
	for _, c := range cases {
		email, reason, ok := ValidateEmail(c.Input)
		if ok != c.OK || reason != c.Reason || !equalRunes(email, c.Email) {
			failures++
			if failures <= 10 {
				t.Errorf("%s: go ok=%v %q %s, python ok=%v %q %s", codepoints(c.Input), ok, reason, codepoints(email), c.OK, c.Reason, codepoints(c.Email))
			}
		}
	}
	if failures > 0 {
		t.Fatalf("%d of %d golden cases differ", failures, len(cases))
	}
}
