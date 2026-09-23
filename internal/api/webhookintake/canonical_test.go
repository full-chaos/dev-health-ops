package webhookintake

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// TestCanonicalJSONSortsKeysAndEscapesNonASCII pins the two flags that
// distinguish this from pyjson.Marshal: sort_keys=True (Python's dict
// insertion order is NOT preserved) and the DEFAULT ensure_ascii=True
// (non-ASCII becomes \uXXXX, not raw UTF-8).
func TestCanonicalJSONSortsKeysAndEscapesNonASCII(t *testing.T) {
	value, err := pyjson.DecodeString(`{"b":1,"a":"café","c":[3,2,1]}`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := canonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":"caf` + uEscape("00e9") + `","b":1,"c":[3,2,1]}`
	if string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// uEscape builds a \uXXXX escape sequence at runtime (never as a literal
// backslash-u in this file's own source text, which a tool-call's JSON
// encoding would otherwise decode before it ever reaches the compiler).
func uEscape(hex string) string {
	return string(rune(0x5c)) + "u" + hex
}

// TestCanonicalJSONMatchesPythonAstralSurrogatePair pins the astral-rune
// escaping the sibling externalingest package's escapeNonASCII documents:
// json.dumps(..., ensure_ascii=True) emits a UTF-16 surrogate pair for a
// rune above the BMP, never a single \u escape.
func TestCanonicalJSONMatchesPythonAstralSurrogatePair(t *testing.T) {
	value, err := pyjson.DecodeString(`{"emoji":"😀"}`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := canonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"emoji":"` + uEscape("d83d") + uEscape("de00") + `"}`
	if string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestCanonicalJSONScalarsAndEmptyContainers(t *testing.T) {
	cases := []struct {
		name string
		json string
		want string
	}{
		{"null", `null`, `null`},
		{"true", `true`, `true`},
		{"false", `false`, `false`},
		{"empty object", `{}`, `{}`},
		{"empty array", `[]`, `[]`},
		{"nested empty", `{"x":{}}`, `{"x":{}}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			value, err := pyjson.DecodeString(c.json)
			if err != nil {
				t.Fatal(err)
			}
			got, err := canonicalJSON(value)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}
