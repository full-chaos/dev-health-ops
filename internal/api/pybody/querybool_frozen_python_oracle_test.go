package pybody_test

import (
	"encoding/json"
	"math/rand"
	"net/url"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

var queryBoolCorpus = []string{
	"", "true", "false", "True", "FALSE", "tRuE", "t", "f", "T", "F", "yes", "no", "YES", "y", "n", "Y", "on", "off", "ON",
	"1", "0", "01", "00", "2", "-1", "1.0", "0.0", " true", "true ", "\ttrue", " 1", "maybe", "nope", "yess", "tru",
	" true", "ｔｒｕｅ", "１", "on\n", "None", "null",
}

const pythonQueryBoolProgram = `
import json, sys
from pydantic import TypeAdapter, ValidationError
ta = TypeAdapter(bool)
out = []
for text in json.loads(sys.stdin.read()):
    try:
        out.append({"ok": str(ta.validate_python(text))})
    except ValidationError as exc:
        err = exc.errors()[0]
        out.append({"type": err["type"], "msg": err["msg"]})
print(json.dumps(out))
`

// TestQueryBoolMatchesFrozenPydantic pins QueryBool against pydantic's lax
// str -> bool (FastAPI's bool query validation) over a corpus and
// deterministic fuzz, and LastQuery's last-value rule.
func TestQueryBoolMatchesFrozenPydantic(t *testing.T) {
	corpus := append([]string(nil), queryBoolCorpus...)
	alphabet := []rune{'t', 'r', 'u', 'e', 'f', 'a', 'l', 's', 'y', 'n', 'o', '0', '1', ' ', 'T', 'O'}
	random := rand.New(rand.NewSource(6379))
	for range 400 {
		runes := make([]rune, 1+random.Intn(5))
		for index := range runes {
			runes[index] = alphabet[random.Intn(len(alphabet))]
		}
		corpus = append(corpus, string(runes))
	}
	input, _ := json.Marshal(corpus)
	output := frozenPython(t, "query-bool.golden.json",
		programoracle.Program{Name: "query-bool", Text: pythonQueryBoolProgram, Stdin: []byte(string(input))})[0]
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []struct{ OK, Type, Msg string }
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d of %d", len(want), len(corpus))
	}
	for index, text := range corpus {
		var errs pybody.Errors
		raw := text
		value, ok := errs.QueryBool("active_only", &raw, false)
		got := ""
		if ok {
			got = map[bool]string{true: "True", false: "False"}[value]
		} else {
			got = errs[0].Type + "|" + errs[0].Msg
		}
		expected := want[index].OK
		if expected == "" {
			expected = want[index].Type + "|" + want[index].Msg
		}
		if got != expected {
			t.Errorf("%q: go %q, python %q", text, got, expected)
		}
	}
	values := url.Values{"a": {"false", "true"}, "e": {""}}
	if got := pybody.LastQuery(values, "a"); got == nil || *got != "true" {
		t.Error("the last value of a repeated key wins")
	}
	if got := pybody.LastQuery(values, "e"); got == nil || *got != "" {
		t.Error("a present empty value is \"\", not absent")
	}
	if pybody.LastQuery(values, "missing") != nil {
		t.Error("an absent key is nil")
	}
}
