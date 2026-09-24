package pybody

import (
	"encoding/json"
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

const canonicalUUID = "0f8fad5b-d9cb-469f-a165-70867728950e"

// uuidCorpus is text a client can put in a uuid.UUID query parameter: every
// accepted form, and each of the uuid crate's refusal classes.
var uuidCorpus = []string{
	canonicalUUID, strings.ToUpper(canonicalUUID), strings.ReplaceAll(canonicalUUID, "-", ""),
	"{" + canonicalUUID + "}", "urn:uuid:" + canonicalUUID, "URN:UUID:" + canonicalUUID, "urn:uuid:" + strings.ReplaceAll(canonicalUUID, "-", ""),
	"{" + strings.ReplaceAll(canonicalUUID, "-", "") + "}", "{" + canonicalUUID, canonicalUUID + "}", "[" + canonicalUUID + "]",
	" " + canonicalUUID, canonicalUUID + " ", "", " ", "x", "0", "{}", "urn:uuid:", "-", "----", "-----", "------",
	canonicalUUID[:35], canonicalUUID + "0", canonicalUUID[:8] + canonicalUUID[9:], canonicalUUID[:9] + "-" + canonicalUUID[9:],
	"0f8fad5-bd9cb-469f-a165-70867728950e", "0f8fad5bd-9cb-469f-a165-70867728950e", "0f8fad5b-d9cb-469f-a165-70867728950e-",
	"0f8fad5b-d9cb-469f-a16570867728950e", "0f8fad5b-d9cb-469fa165-70867728950e0", "g" + canonicalUUID[1:], canonicalUUID[:35] + "g",
	"é" + canonicalUUID[1:], canonicalUUID[:20] + "İ" + canonicalUUID[21:], "１" + canonicalUUID[1:], strings.Repeat("0", 31),
	strings.Repeat("0", 33), strings.Repeat("0", 32), strings.Repeat("F", 32), strings.Repeat("0", 36), strings.Repeat("0", 38),
	"{" + strings.Repeat("0", 36) + "}", "urn:uuid:" + strings.Repeat("0", 36), "0x" + strings.Repeat("0", 30), "+" + strings.Repeat("0", 31),
	strings.Repeat("0", 8) + "_" + strings.Repeat("0", 23), "{{" + canonicalUUID + "}}", "urn:uuid:{" + canonicalUUID + "}",
}

const pythonUUIDProgram = `
import json, sys, uuid
from pydantic import TypeAdapter, ValidationError
ta = TypeAdapter(uuid.UUID)
out = []
for text in json.loads(sys.stdin.read()):
    try:
        out.append({"ok": str(ta.validate_python(text))})
    except ValidationError as exc:
        err = exc.errors()[0]
        out.append({"type": err["type"], "msg": err["msg"], "ctx": json.dumps(err.get("ctx"))})
print(json.dumps(out))
`

// fuzzUUIDs mutates the canonical forms over the characters the grammar
// cares about, so a rule the corpus does not name still meets pydantic.
func fuzzUUIDs() []string {
	alphabet := []rune{'0', 'a', 'F', '-', '{', '}', 'u', ':', 'g', ' ', 'é'}
	seeds := []string{canonicalUUID, strings.ReplaceAll(canonicalUUID, "-", ""), "{" + canonicalUUID + "}", "urn:uuid:" + canonicalUUID}
	random := rand.New(rand.NewSource(6256))
	out := make([]string, 0, 600)
	for range 600 {
		runes := []rune(seeds[random.Intn(len(seeds))])
		for edits := 1 + random.Intn(3); edits > 0; edits-- {
			position := random.Intn(len(runes))
			switch random.Intn(3) {
			case 0:
				runes[position] = alphabet[random.Intn(len(alphabet))]
			case 1:
				runes = append(runes[:position], runes[position+1:]...)
			default:
				runes = append(runes[:position], append([]rune{alphabet[random.Intn(len(alphabet))]}, runes[position:]...)...)
			}
			if len(runes) == 0 {
				break
			}
		}
		out = append(out, string(runes))
	}
	return out
}

func TestQueryUUIDMatchesLivePydantic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := append(append([]string(nil), uuidCorpus...), fuzzUUIDs()...)
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonUUIDProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live pydantic: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []struct{ OK, Type, Msg, Ctx string }
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d of %d", len(want), len(corpus))
	}
	accepted, mismatches := 0, 0
	for index, text := range corpus {
		var errs Errors
		raw := text
		value, ok := errs.QueryUUID("org_id", &raw)
		got := ""
		if ok {
			got = "ok:" + value.String()
			accepted++
		} else {
			ctx, err := pyjson.Dumps(errs[0].Ctx)
			if err != nil {
				t.Fatalf("%q: ctx: %v", text, err)
			}
			got = errs[0].Type + "|" + errs[0].Msg + "|" + ctx
		}
		expected := "ok:" + want[index].OK
		if want[index].OK == "" {
			expected = want[index].Type + "|" + want[index].Msg + "|" + want[index].Ctx
		}
		if got != expected {
			mismatches++
			t.Errorf("%q: go %q, python %q", text, got, expected)
		}
	}
	if accepted == 0 || accepted == len(corpus) {
		t.Fatalf("corpus is one-sided: %d of %d accepted", accepted, len(corpus))
	}
	if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" {
		if err := os.WriteFile(filepath.Join(proof, "api-pybody-queryuuid"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	t.Logf("%d uuids compared (%d accepted), %d mismatches", len(corpus), accepted, mismatches)
}
