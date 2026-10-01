package pybody_test

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// bodyIntCorpus is JSON text a client can send for an int field.
var bodyIntCorpus = []string{
	"true", "false", "0", "1", "2", "3", "-1", "-0", "1.0", "1.5", "-1.5", "2.0", "1e0", "1e2", "1E400", "0.0",
	`"1"`, `" 1 "`, `"+1"`, `"1_0"`, `"-9223372036854775809"`, `"9223372036854775808"`, `" -9223372036854775809 "`, `"1.0"`, `"abc"`, `""`, `"true"`, `"٣"`, "[]", "{}", "[1]", `{"a":1}`,
	"18446744073709551617", "-18446744073709551617", "9223372036854775807", "9223372036854775808", "2147483647",
	"2147483648", "1.8446744073709552e19", "9.2e18", "9.223372036854775807e18", "-9.223372036854775808e18", "-9.3e18", "4.0e9", "-1e300", `"18446744073709551617"`, "100000000000000000000000000000",
}

const pythonBodyIntProgram = `
import json, sys
from pydantic import BaseModel, Field, ValidationError
class Min(BaseModel):
    v: int = Field(90, ge=1)
class Bounded(BaseModel):
    v: int = Field(1, ge=0, le=2)
class Lax(BaseModel):
    v: int | None = None
out = []
for text in json.loads(sys.stdin.read()):
    row = {}
    for name, model in (("min", Min), ("bounded", Bounded), ("lax", Lax)):
        try:
            row[name] = {"ok": str(model.model_validate({"v": json.loads(text)}).v)}
        except ValidationError as exc:
            err = exc.errors()[0]
            row[name] = {"type": err["type"], "msg": err["msg"], "input": json.dumps(err["input"])}
    out.append(row)
print(json.dumps(out))
`

// TestBodyIntMatchesFrozenPydantic compares the unbounded and bounded body int
// helpers with pydantic 2's lax int on every JSON shape a client can send,
// including booleans (accepted as 0/1) and integers past the int64 range.
func TestBodyIntMatchesFrozenPydantic(t *testing.T) {
	input, _ := json.Marshal(bodyIntCorpus)
	output := frozenPython(t, "body-int.golden.json",
		programoracle.Program{Name: "body-int", Text: pythonBodyIntProgram, Stdin: []byte(string(input))})[0]
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []map[string]struct{ OK, Type, Msg, Input string }
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(bodyIntCorpus) {
		t.Fatalf("python answered %d of %d", len(want), len(bodyIntCorpus))
	}
	render := func(value string, ok bool, errs pybody.Errors) string {
		if ok {
			return "ok:" + value
		}
		echoed, err := pyjson.Dumps(errs[0].Input)
		if err != nil {
			t.Fatalf("encode echoed input: %v", err)
		}
		return errs[0].Type + "|" + errs[0].Msg + "|" + echoed
	}
	for index, text := range bodyIntCorpus {
		value, err := pyjson.DecodeString(text)
		if err != nil {
			t.Fatalf("corpus entry %q does not decode: %v", text, err)
		}
		object := pyjson.NewObject()
		object.Set("v", value)

		var minErrs pybody.Errors
		minValue, minOK := minErrs.DefaultedMinInt(object, "v", 1)
		minText := ""
		if minOK {
			minText = minValue.String()
		}
		var laxErrs pybody.Errors
		laxValue, laxOK := laxErrs.OptionalLaxInt(object, "v")
		laxText := ""
		if laxOK {
			laxText = laxValue.String()
		}
		var boundedErrs pybody.Errors
		boundedValue, boundedOK := boundedErrs.DefaultedBoundedInt(object, "v", 0, 2)
		for name, got := range map[string]string{
			"min":     render(minText, minOK, minErrs),
			"bounded": render(strconv.FormatInt(boundedValue, 10), boundedOK, boundedErrs),
			"lax":     render(laxText, laxOK, laxErrs),
		} {
			expected := want[index][name]
			wantText := "ok:" + expected.OK
			if expected.OK == "" {
				wantText = expected.Type + "|" + expected.Msg + "|" + expected.Input
			}
			if got != wantText {
				t.Errorf("%s %s: go %q, python %q", name, text, got, wantText)
			}
		}
	}
}
