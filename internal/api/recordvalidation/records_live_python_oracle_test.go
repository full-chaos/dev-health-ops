package recordvalidation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonRecordModelsProgram generates testdata/record_models.json from the
// models' pydantic core schemas.
const pythonRecordModelsProgram = `
import json
from dev_health_ops.api.external_ingest.schemas import RECORD_KIND_MODELS, OPERATIONAL_RECORD_KINDS
KEEP = {"type", "min_length", "max_length", "ge", "le", "gt", "lt", "multiple_of", "expected", "strict", "allow_inf_nan"}
def node(s):
    t = s["type"]
    out = {k: v for k, v in s.items() if k in KEEP}
    for key in ("schema", "items_schema", "values_schema", "keys_schema"):
        if key in s:
            out[key] = node(s[key])
    if t == "union":
        out["choices"] = [node(c if isinstance(c, dict) else c[0]) for c in s["choices"]]
        out["mode"] = s.get("mode", "smart")
    return out
def model(cls):
    core = cls.__pydantic_core_schema__
    config = core.get("config", {})
    fields = []
    for name, f in core["schema"]["fields"].items():
        schema = f["schema"]
        has_default = schema["type"] == "default"
        if has_default:
            schema = schema["schema"]
        fields.append({"name": name, "alias": f.get("validation_alias"), "has_default": has_default, "schema": node(schema)})
    return {"extra": config.get("extra_fields_behavior", "ignore"),
            "populate_by_name": bool(config.get("validate_by_name", config.get("populate_by_name", False))),
            "fields": fields}
print(json.dumps({"kinds": {kind: model(cls) for kind, cls in RECORD_KIND_MODELS.items()},
                  "operational": sorted(OPERATIONAL_RECORD_KINDS)}, indent=1))
`

// pythonValidateRecordsProgram runs validate_records on each case (a list
// of [kind, payload JSON text]) as one record of its own batch.
const pythonValidateRecordsProgram = `
import json, sys
from dev_health_ops.api.external_ingest.schemas import RecordEnvelope
from dev_health_ops.external_ingest.validate import validate_records
out = []
for kind, payload in json.loads(sys.stdin.read()):
    record = RecordEnvelope.model_validate({"kind": kind, "externalId": "x", "payload": json.loads(payload)})
    out.append([item.model_dump() for item in validate_records([record])])
print(json.dumps(out))
`

func oracleRoot(t *testing.T) (string, string) {
	t.Helper()
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	return root, pyoracle.Resolve(t, root)
}

func runPython(t *testing.T, root, python, program string, stdin []byte) []byte {
	t.Helper()
	command := exec.Command(python, "-c", program)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = bytes.NewReader(stdin)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	return output
}

func writeOracleProof(t *testing.T, name string) {
	t.Helper()
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, name), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestRecordModelsGoldenMatchesLivePython regenerates the model spec from
// the live models and compares it, key order included, with the embedded
// golden the validator runs on.
func TestRecordModelsGoldenMatchesLivePython(t *testing.T) {
	root, python := oracleRoot(t)
	live, err := pyjson.Decode(runPython(t, root, python, pythonRecordModelsProgram, nil))
	if err != nil {
		t.Fatal(err)
	}
	golden, err := pyjson.Decode(recordModelsGolden)
	if err != nil {
		t.Fatal(err)
	}
	liveText, _ := pyjson.Marshal(live)
	goldenText, _ := pyjson.Marshal(golden)
	if !bytes.Equal(liveText, goldenText) {
		t.Fatal("testdata/record_models.json is stale: regenerate it with pythonRecordModelsProgram")
	}
	writeOracleProof(t, "externalingest-record-models")
}

// valuePool is JSON texts that exercise every lax-mode branch of the node
// types the models use.
var valuePool = []string{
	`null`, `true`, `false`, `0`, `1`, `2`, `-1`, `100`, `101`, `1.0`, `1.5`, `-0.5`, `0.5`, `2.0`, `1e300`, `-1e300`,
	`NaN`, `Infinity`, `-Infinity`, `123456789012345678901234567890`, `""`, `" "`, `"x"`, `"é"`, `"yes"`, `"off"`, `"TRUE"`,
	`" 3 "`, `"3.0"`, `"0x1"`, `"1_000"`, `"1e3"`, `" 0.25 "`, `"inf"`, `"-Infinity"`, `"nan"`, `".5"`, `"5."`, `"+1"`,
	`"2026-01-01"`, `"2026-01-01T00:00:00Z"`, `"2026-01-01T00:00:00"`, `"2026-01-01T25:00:00Z"`, `"1700000000"`,
	`1700000000`, `1700000000123`, `"yesterday"`, `[]`, `["a"]`, `[1]`, `["a", 2, "b", null]`, `{}`, `{"a": 1}`,
	`{"a": [1]}`, `{"a": null, "b": "x", "c": 1.5, "d": true, "e": {}}`, `{"gh": ["a", 2], "x": "s"}`,
	`".5"`, `"5."`, `"1e5"`, `"+5"`, `"1.5e3"`, `"12345678901234567890"`, `"1_000.5"`, `"_1"`, `9.3e18`, `9.2e18`, `-9.3e18`, `1e19`,
	`"story"`, `"github"`, `"open"`, `"APPROVED"`, `"blocks"`, `"critical"`, `"active"`, `"done"`, `"issue"`,
}

// boundaryStrings is str values at and around a node's length bounds, in
// ASCII and in a two-byte character (lengths count code points).
func boundaryStrings(node *schemaNode) []string {
	var out []string
	add := func(n int) {
		if n >= 0 {
			out = append(out, `"`+strings.Repeat("a", n)+`"`, `"`+strings.Repeat("é", n)+`"`)
		}
	}
	if node.MinLength != nil {
		add(*node.MinLength - 1)
		add(*node.MinLength)
	}
	if node.MaxLength != nil {
		add(*node.MaxLength)
		add(*node.MaxLength + 1)
	}
	return out
}

// validText is a JSON text node accepts.
func validText(node *schemaNode) string {
	switch node.Type {
	case "str":
		n := 1
		if node.MinLength != nil {
			n = *node.MinLength
		}
		return `"` + strings.Repeat("v", n) + `"`
	case "literal":
		quoted, _ := json.Marshal(node.Expected[0])
		return string(quoted)
	case "int":
		if node.GE != nil {
			return constraintText(node.GE)
		}
		return "1"
	case "float":
		return "0.5"
	case "bool":
		return "true"
	case "datetime":
		return `"2026-01-01T00:00:00Z"`
	case "nullable":
		return validText(node.Inner)
	case "list":
		return `[]`
	case "dict":
		return `{}`
	case "union":
		return validText(node.Choices[0])
	}
	return `null`
}

// floatValues is integer literals around float64's range for a float field
// (or a nullable one): pydantic converts an int to the nearest float and
// refuses it (float_type, "Input should be a valid number") when that is
// infinite, including the exact halfway point above MaxFloat64 that rounds
// to even (2^1024 - 2^970), which is refused too; 2^1024 - 2^970 - 1 and
// 10^308 are accepted.
func floatValues(node *schemaNode) []string {
	inner := node
	if inner.Type == "nullable" {
		inner = inner.Inner
	}
	if inner.Type != "float" {
		return nil
	}
	two := big.NewInt(2)
	top := new(big.Int).Exp(two, big.NewInt(1024), nil)
	half := new(big.Int).Exp(two, big.NewInt(970), nil)
	nearMax := new(big.Int).Sub(top, half)
	values := []*big.Int{
		new(big.Int).Sub(top, big.NewInt(1)), nearMax, new(big.Int).Sub(nearMax, big.NewInt(1)), top,
		new(big.Int).Neg(top), new(big.Int).Neg(nearMax), new(big.Int).Neg(new(big.Int).Sub(nearMax, big.NewInt(1))),
		new(big.Int).Exp(big.NewInt(10), big.NewInt(308), nil), new(big.Int).Exp(big.NewInt(10), big.NewInt(309), nil),
		new(big.Int).Exp(big.NewInt(10), big.NewInt(400), nil), new(big.Int).Neg(new(big.Int).Exp(big.NewInt(10), big.NewInt(400), nil)),
	}
	out := make([]string, len(values))
	for index, value := range values {
		out[index] = value.String()
	}
	return out
}

// listValues is list texts at and past a list's max length, with valid and
// invalid items.
func listValues(node *schemaNode) []string {
	inner := node
	if inner.Type == "nullable" {
		inner = inner.Inner
	}
	if inner.Type != "list" || inner.MaxLength == nil {
		return nil
	}
	item := validText(inner.Items)
	repeat := func(n int, text string) string {
		items := make([]string, n)
		for index := range items {
			items[index] = text
		}
		return "[" + strings.Join(items, ", ") + "]"
	}
	return []string{repeat(*inner.MaxLength, item), repeat(*inner.MaxLength+1, item), repeat(*inner.MaxLength+1, "1"),
		repeat(*inner.MaxLength, "1")}
}

type payloadField struct{ key, value string }

func payloadText(fields []payloadField) string {
	parts := make([]string, len(fields))
	for index, field := range fields {
		key, _ := json.Marshal(field.key)
		parts[index] = string(key) + ": " + field.value
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func wireKey(field modelField) string {
	if field.Alias != "" {
		return field.Alias
	}
	return field.Name
}

// recordCorpus is, per kind: a valid payload; each field set to every pool
// value, boundary string and list size; each field keyed by its Python name,
// by both names, and left out; unknown keys; and seeded random payloads
// mixing all of it. Plus an unknown kind.
func recordCorpus() [][2]string {
	var corpus [][2]string
	kinds := append([]string{}, recordKinds...)
	for _, kind := range kinds {
		model := recordModels[kind]
		base := make([]payloadField, len(model.Fields))
		for index, field := range model.Fields {
			base[index] = payloadField{wireKey(field), validText(field.Schema)}
		}
		with := func(index int, change func([]payloadField) []payloadField) {
			fields := append([]payloadField{}, base...)
			corpus = append(corpus, [2]string{kind, payloadText(change(fields))})
			_ = index
		}
		corpus = append(corpus, [2]string{kind, payloadText(base)}, [2]string{kind, `{}`})
		for index, field := range model.Fields {
			node := field.Schema
			values := append(append(append([]string{}, valuePool...), boundaryStrings(node)...), listValues(node)...)
			values = append(values, floatValues(node)...)
			if node.Type == "nullable" {
				values = append(values, boundaryStrings(node.Inner)...)
			}
			for _, value := range values {
				with(index, func(fields []payloadField) []payloadField { fields[index].value = value; return fields })
			}
			with(index, func(fields []payloadField) []payloadField { return append(fields[:index], fields[index+1:]...) })
			if field.Alias != "" {
				with(index, func(fields []payloadField) []payloadField { fields[index].key = field.Name; return fields })
				with(index, func(fields []payloadField) []payloadField {
					fields[index].key = field.Name
					fields[index].value = `1`
					return fields
				})
				with(index, func(fields []payloadField) []payloadField {
					return append(fields, payloadField{field.Name, fields[index].value})
				})
				with(index, func(fields []payloadField) []payloadField {
					return append([]payloadField{{field.Name, `1`}}, fields...)
				})
			}
		}
		with(0, func(fields []payloadField) []payloadField {
			return append([]payloadField{{"zzz", `1`}}, append(fields, payloadField{"aaa", `null`})...)
		})
		random := rand.New(rand.NewSource(int64(len(kind)) * 6341))
		for range 150 {
			var fields []payloadField
			for _, field := range model.Fields {
				switch random.Intn(6) {
				case 0:
					continue
				case 1:
					fields = append(fields, payloadField{wireKey(field), valuePool[random.Intn(len(valuePool))]})
				default:
					fields = append(fields, payloadField{wireKey(field), validText(field.Schema)})
				}
			}
			if random.Intn(4) == 0 {
				fields = append(fields, payloadField{"extra", `1`})
			}
			random.Shuffle(len(fields), func(a, b int) { fields[a], fields[b] = fields[b], fields[a] })
			corpus = append(corpus, [2]string{kind, payloadText(fields)})
		}
	}
	corpus = append(corpus, [2]string{"nope.v1", `{}`}, [2]string{"it's \"q\" é", `{}`}, [2]string{"", `{}`})
	return corpus
}

// TestRecordValidationMatchesLivePython compares ValidateRecords with
// validate_records on recordCorpus: every item's index, kind, code, message
// and path, in order.
func TestRecordValidationMatchesLivePython(t *testing.T) {
	root, python := oracleRoot(t)
	corpus := recordCorpus()
	input, _ := json.Marshal(corpus)
	output := runPython(t, root, python, pythonValidateRecordsProgram, input)
	lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
	var want []json.RawMessage
	if err := json.Unmarshal(lines[len(lines)-1], &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d cases", len(want), len(corpus))
	}
	mismatches, items := 0, 0
	for index, item := range corpus {
		decoded, err := pyjson.Decode([]byte(item[1]))
		if err != nil {
			t.Fatalf("case %d: %v", index, err)
		}
		got := ValidateRecords([]RecordInput{{Kind: item[0], Payload: decoded.(*pyjson.Object)}})
		var pyItems []map[string]any
		_ = json.Unmarshal(want[index], &pyItems)
		items += len(pyItems)
		gotText := renderItems(got)
		wantText := renderPythonItems(pyItems)
		if gotText != wantText {
			mismatches++
			if mismatches <= 15 {
				t.Errorf("%s %s:\n  go     %s\n  python %s", item[0], item[1], gotText, wantText)
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d cases differ", mismatches, len(corpus))
	}
	writeOracleProof(t, "externalingest-record-validation")
	t.Logf("%d records over %d kinds compared (%d Python items); 0 mismatches", len(corpus), len(recordKinds), items)
}

func renderItems(items []ValidationErrorItem) string {
	var out []string
	for _, item := range items {
		out = append(out, fmt.Sprintf("%d|%s|%s|%s|%s", item.Index, item.Kind, item.Code, item.Message, item.Path))
	}
	return strings.Join(out, " ;; ")
}

func renderPythonItems(items []map[string]any) string {
	var out []string
	for _, item := range items {
		path, _ := item["path"].(string)
		out = append(out, fmt.Sprintf("%v|%v|%v|%v|%s", item["index"], item["kind"], item["code"], item["message"], path))
	}
	return strings.Join(out, " ;; ")
}
