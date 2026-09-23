package pybody

import (
	"encoding/json"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// queryIntCorpus is text a client can put in an int query parameter.
var queryIntCorpus = []string{
	"5", " 5 ", "+5", "-0", "05", "1_0", "5.0", "5.00", "5.5", "5.", ".5", "1e2", "", "  ", "abc", "٣", "１２", "0x10",
	"0b1", "201", "200", "1", "0", "-5", "+0", "00", "_1", "1__0", "1_", "+-1", "- 1", "5.0_0", "5_.0", "5._0",
	"1.000000000000000000001", "\t7\n", "\u00a05", "\u20035", "\x0b5", "\x1c5", "\x1f5", "\u00855", "\u200b5",
	"\u30005", "\u202f5", "\u180e5", "99999999999999999999999.0", "-0.0", "0-5", "0-0", "00-5", strings.Repeat("9", 30),
	strings.Repeat("1", 4300), strings.Repeat("1", 4301), " " + strings.Repeat("1", 4300) + " ", "1_000", "5 ",
}

const pythonQueryIntProgram = `
import json, sys
from typing import Annotated
from annotated_types import Ge, Le
from pydantic import TypeAdapter, ValidationError
ta = TypeAdapter(Annotated[int, Ge(1), Le(200)])
out = []
for text in json.loads(sys.stdin.read()):
    try:
        out.append({"ok": str(ta.validate_python(text))})
    except ValidationError as exc:
        err = exc.errors()[0]
        out.append({"type": err["type"], "msg": err["msg"], "ctx": json.dumps(err.get("ctx"))})
print(json.dumps(out))
`

// fuzzQueryInts adds deterministic random strings over the characters the
// grammar cares about, so a rule the corpus does not name still meets
// pydantic.
func fuzzQueryInts() []string {
	alphabet := []rune{'0', '1', '5', '9', '_', '.', '+', '-', ' ', '\t', 'e', 'x', '\u00a0', '\x1c', '٣'}
	random := rand.New(rand.NewSource(6248))
	out := make([]string, 0, 400)
	for range 400 {
		length := 1 + random.Intn(7)
		runes := make([]rune, length)
		for index := range runes {
			runes[index] = alphabet[random.Intn(len(alphabet))]
		}
		out = append(out, string(runes))
	}
	return out
}

func TestQueryIntMatchesLivePydantic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := append(append([]string(nil), queryIntCorpus...), fuzzQueryInts()...)
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonQueryIntProgram)
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
	ge, le := int64(1), int64(200)
	mismatches, limited := 0, 0
	for index, text := range corpus {
		// Named limit: pydantic-core strips a run of leading zeros that is
		// followed by a minus sign ("0-5" is -5, "0-0" is 0, while "0-05"
		// and "0-00" are refused). Go refuses the whole family; these
		// inputs are counted, not compared.
		if leadingZerosThenMinus(text) {
			limited++
			continue
		}
		var errs Errors
		raw := text
		value, ok := errs.QueryInt("limit", &raw, 50, &ge, &le)
		got := ""
		if ok {
			got = "ok:" + strconv.FormatInt(value, 10)
		} else {
			ctx := "null"
			if errs[0].Ctx != nil {
				encoded, _ := json.Marshal(map[string]int64{errs[0].Ctx.Keys()[0]: map[bool]int64{true: ge, false: le}[errs[0].Type == "greater_than_equal"]})
				ctx = string(encoded)
			}
			got = errs[0].Type + "|" + errs[0].Msg + "|" + strings.ReplaceAll(ctx, ":", ": ")
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
	if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" {
		if err := os.WriteFile(filepath.Join(proof, "api-pybody-queryint"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	t.Logf("%d query ints compared, %d under the leading-zeros-then-minus named limit, %d mismatches", len(corpus)-limited, limited, mismatches)
}

func leadingZerosThenMinus(text string) bool {
	trimmed := strings.TrimFunc(text, unicode.IsSpace)
	rest := strings.TrimLeft(trimmed, "0_")
	return len(rest) < len(trimmed) && strings.HasPrefix(trimmed, "0") && strings.HasPrefix(rest, "-")
}
