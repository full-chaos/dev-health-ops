package pybody

import (
	"encoding/json"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

const pythonUUIDProgram = `
import json, sys, uuid
from pydantic import TypeAdapter, ValidationError
ta = TypeAdapter(uuid.UUID)
out = []
for text in json.loads(sys.stdin.read()):
    try:
        out.append(str(ta.validate_python(text)))
    except ValidationError as exc:
        err = exc.errors()[0]
        out.append("E:%s|%s|%s" % (err["type"], err["msg"], err.get("ctx", {}).get("error")))
print(json.dumps(out))
`

// uuidCorpus is every string of up to five characters over the characters
// that decide the parse, one-character edits of each accepted form at every
// position, and seeded random near-misses (forms of the 30-50 byte lengths,
// and hyphen groups of random sizes).
func uuidCorpus() []string {
	seen := map[string]bool{"": true}
	add := func(text string) { seen[text] = true }
	frontier := []string{""}
	for range 5 {
		var next []string
		for _, prefix := range frontier {
			for _, character := range "0a-{}: g" {
				next = append(next, prefix+string(character))
			}
		}
		for _, text := range next {
			add(text)
		}
		frontier = next
	}
	hyphenated := "01234567-89ab-cdef-0123-456789abcdef"
	simple := strings.ReplaceAll(hyphenated, "-", "")
	for _, base := range []string{hyphenated, simple, "{" + hyphenated + "}", "{" + simple + "}", "urn:uuid:" + hyphenated, "urn:uuid:" + simple, strings.ToUpper(hyphenated)} {
		add(base)
		for index := 0; index <= len(base); index++ {
			add(base[:index] + base[min(index+1, len(base)):])
			for _, character := range "0-g{} :é" {
				add(base[:index] + string(character) + base[index:])
				if index < len(base) {
					add(base[:index] + string(character) + base[index+1:])
				}
			}
		}
	}
	random := rand.New(rand.NewSource(6429))
	symbols := []rune("0123456789abcdef-{}:")
	others := []rune("0123456789abcdefABCDEF-{}: gxuéurnid_")
	lengths := []int{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 44, 45, 46, 47, 50}
	for range 20000 {
		length := lengths[random.Intn(len(lengths))]
		runes := make([]rune, length)
		for index := range runes {
			if random.Float64() < 0.9 {
				runes[index] = symbols[random.Intn(len(symbols))]
			} else {
				runes[index] = others[random.Intn(len(others))]
			}
		}
		add(string(runes))
	}
	for range 20000 {
		groups := make([]string, 1+random.Intn(7))
		for index := range groups {
			size := make([]rune, random.Intn(11))
			for position := range size {
				size[position] = []rune("0123456789abcdefg")[random.Intn(17)]
			}
			groups[index] = string(size)
		}
		add(strings.Join(groups, "-"))
	}
	out := make([]string, 0, len(seen))
	for text := range seen {
		out = append(out, text)
	}
	sort.Strings(out)
	return out
}

func TestParsePydanticUUIDMatchesLivePydantic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := uuidCorpus()
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonUUIDProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live pydantic: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil || len(want) != len(corpus) {
		t.Fatalf("decode: %v (%d of %d)", err, len(want), len(corpus))
	}
	mismatches := 0
	for index, text := range corpus {
		value, failure := ParsePydanticUUID(text)
		got := value.String()
		if failure != nil {
			ctx, _ := failure.Ctx.Get("error")
			got = "E:" + failure.Type + "|" + failure.Msg + "|" + ctx.(string)
		}
		if got != want[index] {
			mismatches++
			if mismatches <= 20 {
				t.Errorf("%q: go %q, python %q", text, got, want[index])
			}
		}
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "api-pybody-uuid"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d uuid strings compared, %d mismatches", len(corpus), mismatches)
}
