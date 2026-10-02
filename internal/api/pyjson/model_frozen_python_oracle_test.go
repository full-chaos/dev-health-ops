package pyjson_test

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// pythonModelProgram answers, per decoded corpus document: its
// pydantic-core dump_json (FastAPI's response_model fast path, through a
// TypeAdapter of Any), and Python's repr(), str() and bool() of it. Then,
// per random 64-bit pattern read as a double, the dump_json of that float.
const pythonModelProgram = `
import json, struct, sys
from typing import Any
from pydantic import TypeAdapter
adapter = TypeAdapter(Any)
payload = json.loads(sys.stdin.read())
out = {"docs": [], "floats": []}
for text in payload["docs"]:
    try:
        value = json.loads(text)
    except ValueError:
        # JSONDecodeError, or an integer literal past the int digit limit.
        out["docs"].append(None)
        continue
    try:
        dumped = adapter.dump_json(value).decode()
    except Exception as exc:
        dumped = "error:" + type(exc).__name__
    try:
        shown = [repr(value), str(value)]
    except Exception as exc:
        shown = ["error", "error"]
    out["docs"].append([dumped, shown[0], shown[1], bool(value)])
for bits in payload["floats"]:
    value = struct.unpack(">d", bytes.fromhex(bits))[0]
    out["floats"].append(adapter.dump_json(value).decode())
print(json.dumps(out))
`

// TestMarshalModelAndReprMatchFrozenPydantic pins MarshalModel against
// pydantic-core's dump_json, and Repr/Str/Truthy against Python's
// repr()/str()/bool(), over the shared corpus and 20,000 random doubles
// (every exponent band, NaN and the infinities included).
func TestMarshalModelAndReprMatchFrozenPydantic(t *testing.T) {

	random := rand.New(rand.NewSource(7))
	floats := make([]float64, 0, 20005)
	for len(floats) < 20000 {
		floats = append(floats, math.Float64frombits(random.Uint64()))
	}
	floats = append(floats, math.Inf(1), math.Inf(-1), math.NaN(), 1e-5, -1.5212603486793025e-05,
		// The positional band's two edges and their neighbours, both signs.
		9.99e-6, 9.999999999999999e-06, -1e-5, -9.99e-6, 1e16, 9999999999999998.0, -1e16, 1e-4, 9.9999e-05)
	bits := make([]string, len(floats))
	for index, value := range floats {
		var buffer [8]byte
		binary.BigEndian.PutUint64(buffer[:], math.Float64bits(value))
		bits[index] = hex.EncodeToString(buffer[:])
	}
	docs := append(append([]string(nil), corpus...),
		`{"k":[1,"a'b",{"x":null}],"t":true}`, `"it's"`, `"say \"hi\""`, `["\u0000\u007f  "]`,
		`{"a":1e-5,"b":1e-7,"c":[2.5e-08,1e+16,1e22]}`, `""`, `"x"`, `0`, `7`, `0.0`, `-0.5`, `[]`, `[0]`, `{}`,
		`{"k":null}`, `false`, `null`, `"it's \"q\""`, `["it's \"q\""]`)
	input, _ := json.Marshal(map[string]any{"docs": docs, "floats": bits})
	output := frozenPython(t, "model.golden.json", programoracle.Program{Name: "pydantic dump_json and repr", Text: pythonModelProgram, Stdin: input})[0]
	var want struct {
		Docs   [][]any  `json:"docs"`
		Floats []string `json:"floats"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &want); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for index, text := range docs {
		value, err := pyjson.DecodeString(text)
		if want.Docs[index] == nil {
			var syntax *pyjson.SyntaxError
			var limit *pyjson.IntLimitError
			if !errors.As(err, &syntax) && !errors.As(err, &limit) {
				t.Errorf("%s: Go decoded, Python refused", text)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: Go refused (%v), Python decoded", text, err)
			continue
		}
		dumped := "error:PydanticSerializationError"
		if got, err := pyjson.MarshalModel(value); err == nil {
			dumped = string(got)
		}
		row := want.Docs[index]
		if dumped != row[0] {
			t.Errorf("%s dump_json:\n Go     %s\n Python %s", text, dumped, row[0])
		}
		if row[1] != "error" {
			if got := pyjson.Repr(value); got != row[1] {
				t.Errorf("%s repr:\n Go     %s\n Python %s", text, got, row[1])
			}
			// A lone surrogate cannot cross the JSON pipe from Python
			// intact, so str() of one is compared only through repr().
			if got := pyjson.Str(value); !pyjson.HasSurrogate(got) && got != row[2] {
				t.Errorf("%s str:\n Go     %s\n Python %s", text, got, row[2])
			}
		}
		if got := pyjson.Truthy(value); got != row[3] {
			t.Errorf("%s bool: Go %v, Python %v", text, got, row[3])
		}
		checked++
	}
	for index, value := range floats {
		got, err := pyjson.MarshalModel(pyjson.Float(value))
		if err != nil || string(got) != want.Floats[index] {
			t.Errorf("float %s (%v): Go %s %v, Python %s", bits[index], value, got, err, want.Floats[index])
		}
	}
	decodable := 0
	for _, row := range want.Docs {
		if row != nil {
			decodable++
		}
	}
	if checked != decodable || decodable < 20 {
		t.Fatalf("%d of %d decodable documents compared", checked, decodable)
	}
}
