package pybody_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// stringCorpus is JSON text a client can send for a str field: lone high
// and low surrogates alone, among other text and past the bound, an
// escaped surrogate pair (one astral code point), BMP and ASCII text at and
// around the bounds, and non-strings.
var stringCorpus = []string{
	`"\ud800"`, `"\udfff"`, `"a\udc00b"`, `"\ud800\ud800"`, `"\udc00\ud800"`, `"\ud800abcdefgh"`, `""`,
	`"\ud83d\ude00"`, `"😀"`, `"é"`, `"ab"`, `"abcde"`, `"abcdef"`, `"\u0000"`, `"abc\udfffdefgh"`,
	`null`, `5`, `true`, `[]`, `{}`,
}

// TestStringMatchesFrozenPydantic compares the str helpers with pydantic 2
// for every constraint shape the helpers take: no bound, a minimum, a
// maximum, and an optional field with a maximum.
func TestStringMatchesFrozenPydantic(t *testing.T) {
	input, _ := json.Marshal(stringCorpus)
	output := frozenPython(t, "string.golden.json",
		programoracle.Program{Name: "string", Stdin: []byte(string(input))})[0]
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []map[string]struct{ OK, Type, Msg, Input string }
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(stringCorpus) {
		t.Fatalf("python answered %d of %d", len(want), len(stringCorpus))
	}
	render := func(value string, ok, present bool, errs pybody.Errors) string {
		if len(errs) > 0 {
			echoed, err := pyjson.Dumps(errs[0].Input)
			if err != nil {
				t.Fatalf("encode echoed input: %v", err)
			}
			return errs[0].Type + "|" + errs[0].Msg + "|" + echoed
		}
		if !present {
			return "ok:None"
		}
		encoded, err := pyjson.Dumps(value)
		if err != nil {
			t.Fatalf("encode value: %v", err)
		}
		return "ok:" + encoded
	}
	for index, text := range stringCorpus {
		value, err := pyjson.DecodeString(text)
		if err != nil {
			t.Fatalf("corpus entry %q does not decode: %v", text, err)
		}
		object := pyjson.NewObject()
		object.Set("v", value)
		got := map[string]string{}
		for name, bounds := range map[string][2]int{"plain": {0, 0}, "min": {1, 0}, "max": {0, 5}} {
			var errs pybody.Errors
			result, ok := errs.RequiredString(object, "v", bounds[0], bounds[1])
			got[name] = render(result, ok, true, errs)
		}
		var errs pybody.Errors
		result, present := errs.OptionalString(object, "v", 0, 5)
		got["optional_max"] = render(result, present, present, errs)
		for name, gotText := range got {
			expected := want[index][name]
			wantText := "ok:" + expected.OK
			if expected.OK == "" {
				wantText = expected.Type + "|" + expected.Msg + "|" + expected.Input
			}
			if gotText != wantText {
				t.Errorf("%s %s: go %q, python %q", name, text, gotText, wantText)
			}
		}
	}
}
