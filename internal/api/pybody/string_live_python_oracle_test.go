package pybody

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
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

const pythonStringProgram = `
import json, sys
from pydantic import BaseModel, Field, ValidationError
class Plain(BaseModel):
    v: str
class Min(BaseModel):
    v: str = Field(min_length=1)
class Max(BaseModel):
    v: str = Field(max_length=5)
class OptionalMax(BaseModel):
    v: str | None = Field(default=None, max_length=5)
out = []
for text in json.loads(sys.stdin.read()):
    row = {}
    for name, model in (("plain", Plain), ("min", Min), ("max", Max), ("optional_max", OptionalMax)):
        try:
            value = model.model_validate({"v": json.loads(text)}).v
            row[name] = {"ok": "None" if value is None else json.dumps(value)}
        except ValidationError as exc:
            err = exc.errors()[0]
            row[name] = {"type": err["type"], "msg": err["msg"], "input": json.dumps(err["input"])}
    out.append(row)
print(json.dumps(out))
`

// TestStringMatchesLivePydantic compares the str helpers with pydantic 2
// for every constraint shape the helpers take: no bound, a minimum, a
// maximum, and an optional field with a maximum.
func TestStringMatchesLivePydantic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	input, _ := json.Marshal(stringCorpus)
	command := exec.Command(python, "-c", pythonStringProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live pydantic: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []map[string]struct{ OK, Type, Msg, Input string }
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(stringCorpus) {
		t.Fatalf("python answered %d of %d", len(want), len(stringCorpus))
	}
	render := func(value string, ok, present bool, errs Errors) string {
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
			var errs Errors
			result, ok := errs.RequiredString(object, "v", bounds[0], bounds[1])
			got[name] = render(result, ok, true, errs)
		}
		var errs Errors
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
	if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" {
		if err := os.WriteFile(filepath.Join(proof, "api-pybody-string"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
}
