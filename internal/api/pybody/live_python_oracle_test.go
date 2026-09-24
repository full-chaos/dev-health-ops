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

// pythonPlainIntProgram is pydantic's unbounded int validation of each text:
// the exact parsed value, or the error type. The bounded program above only
// shows which side of a bound a value fell on.
const pythonPlainIntProgram = `
import json, sys
from pydantic import TypeAdapter, ValidationError
ta = TypeAdapter(int)
out = []
for text in json.loads(sys.stdin.read()):
    try:
        out.append(str(ta.validate_python(text)))
    except ValidationError as exc:
        out.append("E:" + exc.errors()[0]["type"])
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

// exhaustiveQueryInts is every string of up to five characters over the
// characters that decide where pydantic-core's leading-zero, underscore,
// sign and fraction rules bite -- the whole boundary, not a sample of it.
func exhaustiveQueryInts() []string {
	alphabet := "015-+_."
	out := []string{""}
	frontier := []string{""}
	for range 5 {
		var next []string
		for _, prefix := range frontier {
			for _, character := range alphabet {
				next = append(next, prefix+string(character))
			}
		}
		out = append(out, next...)
		frontier = next
	}
	return out
}

// longQueryInts is the size boundary: every prefix shape (none, zeros,
// zeros and underscores, "+", "-", zeros after a sign, zeros before a minus)
// against digit runs either side of the 4300 limit, with the tails that
// decide whether the size gate or the digit limit answers (nothing, a zero
// fraction, a real fraction, junk, a trailing underscore), and digit runs
// broken by underscores.
func longQueryInts() []string {
	prefixes := []string{"", "0", strings.Repeat("0", 10), strings.Repeat("0", 5000), strings.Repeat("0_", 5), strings.Repeat("0_", 3000),
		"+", "+" + strings.Repeat("0", 10), "-", "-" + strings.Repeat("0", 10), "-" + strings.Repeat("0", 5000),
		strings.Repeat("0", 10) + "-", strings.Repeat("0", 5000) + "-", strings.Repeat("0_", 5) + "-", strings.Repeat("0_", 3000) + "-",
		"-0", "+0_", " "}
	tails := []string{"", ".0", ".5", "x", "_", "e5", "-5"}
	var out []string
	for _, prefix := range prefixes {
		for _, digits := range []int{1, 4299, 4300, 4301} {
			for _, tail := range tails {
				out = append(out, prefix+strings.Repeat("1", digits)+tail)
			}
		}
	}
	for _, digits := range []int{2150, 2151, 4300, 4301} {
		interleaved := "1" + strings.Repeat("_1", digits-1)
		for _, prefix := range []string{"", "-", "+", "0", "0_", "-0"} {
			out = append(out, prefix+interleaved)
		}
	}
	return append(out, strings.Repeat("1", 2500)+"_"+strings.Repeat("1", 2500), strings.Repeat("1", 2500)+" "+strings.Repeat("1", 2500),
		strings.Repeat("٣", 4301), "٣"+strings.Repeat("1", 4301), "."+strings.Repeat("1", 4301),
		"1."+strings.Repeat("0", 5000), "1."+strings.Repeat("0", 5000)+"1", strings.Repeat("1", 10)+"."+strings.Repeat("0", 5000))
}

func TestQueryIntMatchesLivePydantic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := append(append(append(append([]string(nil), queryIntCorpus...), fuzzQueryInts()...), exhaustiveQueryInts()...), longQueryInts()...)
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
	mismatches := 0
	for index, text := range corpus {
		var errs Errors
		raw := text
		value, ok := errs.QueryInt("limit", &raw, 50, &ge, &le)
		got := ""
		if ok {
			got = "ok:" + value.String()
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
			t.Errorf("%s: go %q, python %q", abbreviate(text), got, expected)
		}
	}
	// The exact parsed value, unbounded: a wrong value on the same side of a
	// bound would pass the bounded comparison above.
	plain := exec.Command(python, "-c", pythonPlainIntProgram)
	plain.Stdin = strings.NewReader(string(input))
	plainOutput, err := plain.CombinedOutput()
	if err != nil {
		t.Fatalf("live pydantic (plain int): %v", pyoracle.RunError(python, err, plainOutput))
	}
	plainLines := strings.Split(strings.TrimSpace(string(plainOutput)), "\n")
	var plainWant []string
	if err := json.Unmarshal([]byte(plainLines[len(plainLines)-1]), &plainWant); err != nil || len(plainWant) != len(corpus) {
		t.Fatalf("decode plain: %v (%d of %d)", err, len(plainWant), len(corpus))
	}
	for index, text := range corpus {
		value, failure := ParsePydanticInt(text)
		var got string
		if failure != nil {
			got = "E:" + failure.Type
		} else {
			got = value.String()
		}
		if got != plainWant[index] {
			mismatches++
			t.Errorf("%s: go value %q, python %q", abbreviate(text), got, plainWant[index])
		}
	}
	if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" {
		if err := os.WriteFile(filepath.Join(proof, "api-pybody-queryint"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	t.Logf("%d query ints compared, %d mismatches", len(corpus), mismatches)
}

// abbreviate shortens a long corpus value to its length and both ends, so a
// mismatch on a 4300-digit input stays readable.
func abbreviate(text string) string {
	if len(text) <= 48 {
		return strconv.Quote(text)
	}
	return strconv.Quote(text[:20]) + "..." + strconv.Quote(text[len(text)-20:]) + " (" + strconv.Itoa(len(text)) + " bytes)"
}
