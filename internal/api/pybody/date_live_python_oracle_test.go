package pybody

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// jsonDateCorpus is JSON text of values a client can send for a `date` or an
// `AwareDatetime` field: every JSON type, the datetime forms, unix numbers, and
// strings that break each stage of speedate's grammar.
func jsonDateCorpus() []string {
	corpus := []string{
		`"2026-09-23"`, `"2026-09-23T00:00:00Z"`, `"2026-09-23T00:00:00+05:00"`, `"2026-09-23T00:00:01Z"`, `"2026-09-23T00:00:00"`,
		`"2026-09-23T02:00:00.123456Z"`, `"2026-09-23T02:00:00-05:30"`, `"2026-09-23 02:00"`, `"2026-02-30"`, `"2026-13-01"`,
		`"0000-01-01"`, `"9999-12-31T23:59:59Z"`, `"x"`, `""`, `"  2026-09-23"`, `"2026-09-23  "`, `"20260923"`, `"2026"`,
		`1700000000`, `1700000000.5`, `86400`, `0`, `-1`, `1e20`, `true`, `false`, `null`, `[]`, `{}`, `[1]`, `{"a": 1}`,
		`"1700000000"`, `"86400"`, `"1e3"`, `"+5"`, `"5."`, `253402300799`, `253402300800`, `-62135596800`,
		`"2026-09-23T02:00:00+24:00"`, `"2026-09-23T02:00:00+01:60"`, `"2026-09-23T25:00"`, `"2026-09-23T02:60"`, `"2026-09-23T02:00:60"`,
		`"2026-09-23T"`, `"2026-09-23T02:00:00."`, `"2026-09-23T02:00:00Zx"`, `"2026-09-23T02:00:00x"`, `"2026-09-23X02:00"`,
	}
	alphabet := []string{"0", "9", ":", ".", "+", "-", "Z", "x", " ", "T"}
	for _, stage := range []string{"2026-09-23", "2026-09-23T", "2026-09-23T02:00", "2026-09-23T02:00:00", "2026-09-23T02:00:00+02", "2026-1", "x"} {
		frontier := []string{""}
		for range 3 {
			var next []string
			for _, prefix := range frontier {
				for _, character := range alphabet {
					next = append(next, prefix+character)
				}
			}
			for _, suffix := range next {
				corpus = append(corpus, strconv.Quote(stage+suffix))
			}
			frontier = next
		}
	}
	return corpus
}

const pythonDateFieldsProgram = `
import json, sys
from datetime import date, datetime, timezone
from pydantic import AwareDatetime, TypeAdapter, ValidationError
adapters = {"date": TypeAdapter(date), "aware": TypeAdapter(AwareDatetime)}
def render(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value.isoformat()
out = {name: [] for name in adapters}
for text in json.loads(sys.stdin.read()):
    raw = json.loads(text)
    for name, ta in adapters.items():
        try:
            out[name].append({"ok": render(ta.validate_python(raw))})
        except ValidationError as exc:
            err = exc.errors()[0]
            out[name].append({"type": err["type"], "msg": err["msg"], "reason": (err.get("ctx") or {}).get("error", "")})
print(json.dumps(out))
`

func TestDateAndAwareDatetimeMatchLivePydantic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := jsonDateCorpus()
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonDateFieldsProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live pydantic: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	type result struct{ OK, Type, Msg, Reason string }
	var want map[string][]result
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	mismatches := 0
	for index, text := range corpus {
		raw, err := pyjson.DecodeString(text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		for _, field := range []string{"date", "aware"} {
			var errs Errors
			got := ""
			if field == "date" {
				if date, ok := Date(&errs, raw, nil); ok {
					got = date.Format("2006-01-02")
				}
			} else if parsed, ok := AwareDatetime(&errs, raw, nil); ok {
				got = strings.Replace(pytimeISO(parsed.Time), "Z", "+00:00", 1)
			}
			if len(errs) > 0 {
				reason := ""
				if errs[0].Ctx != nil {
					if value, present := errs[0].Ctx.Get("error"); present {
						reason, _ = value.(string)
					}
				}
				got = errs[0].Type + "|" + errs[0].Msg + "|" + reason
			}
			w := want[field][index]
			expected := w.OK
			if expected == "" {
				expected = w.Type + "|" + w.Msg + "|" + w.Reason
			}
			if got != expected {
				mismatches++
				if mismatches <= 40 {
					t.Errorf("%s %s:\n Go     %s\n Python %s", field, text, got, expected)
				}
			}
		}
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "api-pybody-date-aware"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d values x 2 fields compared, %d mismatches", len(corpus), mismatches)
}

// pytimeISO is an aware UTC instant as isoformat() spells it: microseconds only
// when non-zero.
func pytimeISO(at time.Time) string {
	at = at.UTC()
	text := at.Format("2006-01-02T15:04:05")
	if micro := at.Nanosecond() / 1000; micro != 0 {
		text += fmt.Sprintf(".%06d", micro)
	}
	return text + "Z"
}
