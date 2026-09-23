package pytime

import (
	"encoding/json"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// datetimeCorpus is JSON text of ts values a client can send.
var datetimeCorpus = []string{
	`"2026-09-23T02:00:00.123Z"`, `"2026-09-23T02:00:00+02:00"`, `"2026-09-23 02:00"`, `"2026-09-23"`, `"2026-09-23T02"`,
	`"x"`, `""`, `"2026-13-01T00:00:00Z"`, `"202"`, `"2026x09-23"`, `"20a6-09-23"`, `"2026-0x-23"`, `"2026-09x23"`,
	`"2026-09-3x"`, `"2026-00-10"`, `"2026-02-30"`, `"2026-02-29"`, `"2024-02-29T00:00:00Z"`, `"2026-09-23T"`,
	`"2026-09-23X02:00"`, `"2026-09-23T2:00"`, `"2026-09-23T02:0"`, `"2026-09-23T02:00:6"`, `"2026-09-23T25:00"`,
	`"2026-09-23T02:00:00."`, `"2026-09-23T02:00:00.123456789"`, `"2026-09-23T02:00:00+0200"`, `"2026-09-23T02:00:00+02"`,
	`"2026-09-23T02:00:00-00:00"`, `"2026-09-23T02:00:00-05:30"`, `"2026-09-23T02:00:00+24:00"`, `"2026-09-23T02:00:00 "`,
	`"0000-01-01"`, `"9999-12-31T23:59:59Z"`, `"1e3"`, `"-1"`, `"17e8"`, `"  2026-09-23"`, `"2026-09-23_02:00"`,
	`"2026-09-23t02:00:00z"`, `"2026-09-23T02:00:00,5Z"`, `"1700000000.123456789"`, `"1700000000123"`, `"+5"`, `"5."`,
	`1700000000`, `1700000000123`, `1.5`, `-1.5`, `1e20`, `-1e20`, `0`, `true`, `null`, `[]`, `{}`,
	`123456789012345678901234567890`, `"2026-09-23T02:00:60Z"`, `"2026-09-23T24:00:00"`,
}

const pythonDatetimeProgram = `
import json, sys
from datetime import datetime
from pydantic import TypeAdapter, ValidationError
ta = TypeAdapter(datetime)
out = []
for text in json.loads(sys.stdin.read()):
    try:
        out.append({"ok": ta.dump_json(ta.validate_python(json.loads(text))).decode()[1:-1]})
    except ValidationError as exc:
        err = exc.errors()[0]
        out.append({"type": err["type"], "msg": err["msg"]})
print(json.dumps(out))
`

func TestParseDatetimeMatchesLivePydantic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	input, _ := json.Marshal(datetimeCorpus)
	command := exec.Command(python, "-c", pythonDatetimeProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live pydantic: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []struct{ OK, Type, Msg string }
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v: %s", err, output)
	}
	for index, text := range datetimeCorpus {
		var raw any
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		_ = decoder.Decode(&raw)
		if number, ok := raw.(json.Number); ok {
			if integer, ok := new(big.Int).SetString(string(number), 10); ok {
				raw = integer
			} else {
				raw, _ = number.Float64()
			}
		}
		parsed, failure := ParseDatetime(raw)
		got := ""
		if failure != nil {
			got = failure.Type + "|" + failure.Msg
		} else {
			got = Pydantic(parsed)
		}
		expected := want[index].OK
		if expected == "" {
			expected = want[index].Type + "|" + want[index].Msg
		}
		if got != expected {
			t.Errorf("%s:\n Go     %s\n Python %s", text, got, expected)
		}
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "api-pytime"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}
