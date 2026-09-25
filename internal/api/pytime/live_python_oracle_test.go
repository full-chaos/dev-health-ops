package pytime

import (
	"encoding/json"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
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
	`"0000-01-01"`, `"0001-01-01T00:00:00+05:00"`, `"0000-12-31T23:00:00-05:00"`, `"0001-01-01T00:00:00-05:00"`, `"9999-12-31T23:59:59Z"`, `"1e3"`, `"-1"`, `"17e8"`, `"  2026-09-23"`, `"2026-09-23_02:00"`,
	`"2026-09-23t02:00:00z"`, `"2026-09-23T02:00:00,5Z"`, `"1700000000.123456789"`, `"1700000000123"`, `"+5"`, `"5."`,
	`1700000000`, `1700000000123`, `1.5`, `-1.5`, `1e20`, `-1e20`, `0`, `true`, `null`, `[]`, `{}`,
	`123456789012345678901234567890`, `-62150000000`, `-1.25`, `-0.75`, `-1700000000.123456`, `-20000000001.25`, `1e308`, `-1e308`, `9223372036854775807`, `9223372036854775808`, `-9223372036854775808`, `-9223372036854775809`, `253402300799999`, `253402300800000`, `-62167219200000.5`, `-62167219200`, `-62167219201`, `-62150000000000`, `-62150000000.5`, `-62167219200001`, `253402300799.9999999`, `20000000000.5`, `2e10`, `-2.0000000001e10`, `"2026-09-23T02:00:60Z"`, `"2026-09-23T24:00:00"`,
}

// numericStringCorpus crosses signs, mantissas and exponents, including
// exponents whose value overflows or underflows an f64, and integers at
// and past the i64 bounds.
func numericStringCorpus() []string {
	var corpus []string
	for _, sign := range []string{"", "+", "-"} {
		for _, mantissa := range []string{"1.5", ".5", "5.", "0.0", "1700000000.5", "0.", "."} {
			for _, exponent := range []string{"", "e5", "E999", "e-999", "e+10", "e308", "e309", "e-400", "E+999", "e-5", "e", "e+"} {
				corpus = append(corpus, strconv.Quote(sign+mantissa+exponent))
			}
		}
		for _, integer := range []string{
			"9223372036854775807", "9223372036854775808", "99999999999999999999", "18446744073709551626", "0", "1e999",
			"20000000000", "20000000001", "20000000000999", "20000000001000", "62135596800", "62135596801", "62167219200",
			"62167219201", "62167219200000", "62167219200001", "253402300799", "253402300800", "253402300799999",
			"253402300800000", "20000000000999.0", "20000000001000.0", "170000000000000.0", "62167219200.5",
			"62135596800.5", "62167219200000.5", "253402300799.9999995", "253402300799999.9995", "0.0000005",
			"0.0000015", "1.9999999", "1.5.5", "1.e5", "5", "",
		} {
			corpus = append(corpus, strconv.Quote(sign+integer))
		}
	}
	return corpus
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
	corpus := append(append([]string{}, datetimeCorpus...), numericStringCorpus()...)
	input, _ := json.Marshal(corpus)
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
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d inputs", len(want), len(corpus))
	}
	for index, text := range corpus {
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
	t.Logf("%d values compared", len(corpus))
}

const pythonDateProgram = `
import json, sys
from datetime import date
from pydantic import TypeAdapter, ValidationError
ta = TypeAdapter(date)
out = []
for text in json.loads(sys.stdin.read()):
    try:
        out.append({"ok": ta.validate_python(json.loads(text)).isoformat()})
    except ValidationError as exc:
        err = exc.errors()[0]
        out.append({"type": err["type"], "msg": err["msg"], "reason": (err.get("ctx") or {}).get("error", "")})
print(json.dumps(out))
`

// TestParseDateMatchesLivePydantic compares ParseDate with pydantic's
// TypeAdapter(date) on the datetime corpus and the numeric-string corpus.
// A non-numeric string that is neither a date nor a datetime is not
// judged by ParseDate (its reason is speedate's datetime error, which the
// caller carries); for those, only pydantic's error type is checked.
func TestParseDateMatchesLivePydantic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := append(append([]string{}, datetimeCorpus...), numericStringCorpus()...)
	corpus = append(corpus, `86400`, `86400.0`, `"86400"`, `"86400.0"`, `0.5`, `-86400`, `"2024-01-01T00:00:00Z"`,
		`"2024-01-01T00:00:00+05:00"`, `"2024-01-01T00:00:01"`, `253402214400`, `253402300800`, `-62135596800`, `1e20`)
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonDateProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live pydantic: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []struct{ OK, Type, Msg, Reason string }
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v: %s", err, output)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d inputs", len(want), len(corpus))
	}
	judged := 0
	for index, text := range corpus {
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
		if raw == nil {
			continue
		}
		date, failure, ok := ParseDate(raw)
		expected := want[index].OK
		if expected == "" {
			expected = want[index].Type + "|" + want[index].Msg + "|" + want[index].Reason
		}
		if !ok {
			if want[index].OK != "" || want[index].Type != "date_from_datetime_parsing" {
				t.Errorf("%s: not judged, but python gives %s", text, expected)
			}
			continue
		}
		judged++
		got := ""
		if failure != nil {
			got = failure.Type + "|" + failure.Msg + "|" + failure.Reason
		} else {
			got = date.Format("2006-01-02")
		}
		if got != expected {
			t.Errorf("%s:\n Go     %s\n Python %s", text, got, expected)
		}
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "api-pytime-date"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d values compared, %d judged by ParseDate", len(corpus), judged)
}
