package pyunicodedata

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// nfcGoldenPath holds a slice of unicodedata.normalize("NFC", ...)'s own
// answers, cut from the live oracle run (TestNFCMatchesLivePython checks
// it is still exactly what the interpreter says), so the ordinary test run
// pins NFC without Python.
const nfcGoldenPath = "testdata/nfc_golden.jsonl"

type nfcCase struct {
	Input []rune `json:"input"`
	Want  []rune `json:"want"`
}

func TestNFCMatchesGolden(t *testing.T) {
	cases := readJSONLines[nfcCase](t, nfcGoldenPath)
	if len(cases) < 1000 {
		t.Fatalf("golden holds %d cases; it was cut short", len(cases))
	}
	failures := 0
	for _, c := range cases {
		if got := NFC(c.Input); !equal(got, c.Want) {
			failures++
			if failures <= 10 {
				t.Errorf("NFC(%U) = %U, python %U", c.Input, got, c.Want)
			}
		}
	}
	if failures > 0 {
		t.Fatalf("%d of %d golden cases differ", failures, len(cases))
	}
}

// TestLookupsPinned pins answers the email validator depends on, one per
// function, from unicodedata 16.0.0.
func TestLookupsPinned(t *testing.T) {
	checks := []struct {
		name      string
		got, want any
	}{
		{"Category(a)", Category('a'), "Ll"},
		{"Category(U+0301)", Category(0x0301), "Mn"},
		{"Category(U+00A0)", Category(0x00a0), "Zs"},
		{"Category(U+D800)", Category(0xd800), "Cs"},
		{"Category(U+0378)", Category(0x0378), "Cn"},
		{"Category(U+10FFFF)", Category(0x10ffff), "Cn"},
		{"Bidirectional(U+05D0)", Bidirectional(0x05d0), "R"},
		{"Bidirectional(U+0378)", Bidirectional(0x0378), ""},
		{"Combining(U+094D)", Combining(0x094d), 9},
		{"Combining(a)", Combining('a'), 0},
		{"HasName(a)", HasName('a'), true},
		{"HasName(U+0000)", HasName(0), false},
		{"DecompositionHasFullStop(U+2488)", DecompositionHasFullStop(0x2488), true},
		{"DecompositionHasFullStop(a)", DecompositionHasFullStop('a'), false},
		{"IsWord(_)", IsWord('_'), true},
		{"IsWord(-)", IsWord('-'), false},
	}
	name, ok := Name(0x00a0)
	checks = append(checks, struct {
		name      string
		got, want any
	}{"Name(U+00A0)", name + "/" + map[bool]string{true: "ok", false: "none"}[ok], "NO-BREAK SPACE/ok"})
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %v, want %v", check.name, check.got, check.want)
		}
	}
}

func readJSONLines[T any](t *testing.T, path string) []T {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []T
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var value T
		if err := json.Unmarshal(line, &value); err != nil {
			t.Fatal(err)
		}
		out = append(out, value)
	}
	return out
}

// checkJSONLines compares the committed golden at path with values, or
// rewrites it when regenerate is set.
func checkJSONLines[T any](t *testing.T, path string, values []T, regenerate bool) {
	t.Helper()
	var rendered []byte
	for _, value := range values {
		line, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		rendered = append(append(rendered, line...), '\n')
	}
	if regenerate {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, rendered, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	committed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(committed, rendered) {
		t.Fatalf("%s differs from the live answers; regenerate with DEV_HEALTH_REGENERATE_TABLES=1", path)
	}
}

// TestNFCPinnedAgainstXText pins the two ways golang.org/x/text's
// normalizer departs from Python, both answered by the live oracle: a
// supplementary-plane base does not compose with a following mark as its
// low-16-bit BMP twin would (U+0057 U+0301 is U+1E82; U+10057 U+0301 stays),
// and a run of more than 30 marks gets no U+034F.
func TestNFCPinnedAgainstXText(t *testing.T) {
	supplementary := []rune{'e', 0x10057, 0x0301}
	if got := NFC(supplementary); !equal(got, supplementary) {
		t.Errorf("NFC(%U) = %U, python leaves it unchanged", supplementary, got)
	}
	long := []rune{'a'}
	want := []rune{0x00e1}
	for i := 0; i < 40; i++ {
		long = append(long, 0x0301)
		if i > 0 {
			want = append(want, 0x0301)
		}
	}
	long, want = append(long, 'b'), append(want, 'b')
	if got := NFC(long); !equal(got, want) {
		t.Errorf("NFC(a + 40 x U+0301 + b) = %U, python %U", got, want)
	}
}
