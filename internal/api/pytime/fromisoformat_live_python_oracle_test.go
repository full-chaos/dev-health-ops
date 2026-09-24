package pytime

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

const pythonFromISOProgram = `
import json, sys
from datetime import datetime, timezone
out = []
for text in json.loads(sys.stdin.read()):
    try:
        value = datetime.fromisoformat(text)
    except (ValueError, TypeError):
        out.append(None)
        continue
    offset = value.utcoffset()
    if offset is None:
        out.append([value.strftime("%Y-%m-%dT%H:%M:%S.%f"), None])
    else:
        seconds = offset.days * 86400 + offset.seconds
        micro = offset.microseconds
        out.append([value.replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S.%f"), [seconds, micro]])
print(json.dumps(out))
`

// fromISOCorpus holds every form the grammar reads and the near misses of
// each.
var fromISOCorpus = []string{
	"2026-01-01", "20260101", "2026-W01", "2026W01", "2026-W01-1", "2026W011", "2026-W53-1", "2020-W53-7",
	"2026-W00-1", "2026-W01-0", "2026-W01-8", "2026W01-1", "2026-W011", "2026-01", "202601", "2026-0101", "202601-01",
	"2026-01-01T00", "2026-01-01T0000", "2026-01-01T000000", "2026-01-01T00:00", "2026-01-01T00:00:00",
	"2026-01-01T00:0000", "2026-01-01T0000:00", "2026-01-01T00:00:00.5", "2026-01-01T00:00:00,5",
	"2026-01-01T000000.123456789", "2026-01-01T00:00:00.", "2026-01-01T00:00:00.x", "2026-01-01T00:00.5",
	"2026-01-01T24:00", "2026-01-01T24:00:00", "2026-01-01T24:00:00.000000", "2026-01-01T24:00:01", "2026-12-31T24:00:00",
	"2026-02-29T24:00:00", "2026-02-28T24:00:00", "2026-01-01T24", "2026-01-01T25:00",
	"2026-01-01T00:00:00Z", "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00-00:00", "2026-01-01T00:00:00+05",
	"2026-01-01T00:00:00+0530", "2026-01-01T00:00:00+05:30", "2026-01-01T00:00:00+05:30:15", "2026-01-01T00:00:00+053015",
	"2026-01-01T00:00:00+05:30:15.5", "2026-01-01T00:00:00+23:59", "2026-01-01T00:00:00+24:00", "2026-01-01T00:00:00+5",
	"2026-01-01T00:00:00+053", "2026-01-01T00:00:00+", "2026-01-01T00:00:00Z+01:00", "2026-01-01T00:00:00ZZ",
	"2026-01-01T00:00:00-05:00+01:00", "2026-01-01T00:00:00+05:00-01:00", "2026-01-01 00:00:00", "2026-01-01_00:00:00",
	"2026-01-01x00:00:00", "2026-01-01T", "2026-01-01TT00", "20260101T000000Z", "20260101T00:00:00",
	"2026-W01-1T00:00:00", "2026W011T000000", "2026-W01T00:00", "2026W01T00", "0001-01-01", "0000-01-01", "9999-12-31",
	"9999-12-31T24:00:00", "9999-W52-5", "9999-W52-6", "0001-W01-1", "2026-13-01", "2026-00-10", "2026-01-32",
	"2026-02-29", "2024-02-29", "1900-02-29", "2000-02-29", " 2026-01-01", "2026-01-01 ", "+2026-01-01", "2026-1-01",
	"2026-01-01T1:00", "2026-01-01T00:60", "2026-01-01T00:00:60", "2026-01-01T23:59:59.999999",
	"2026-01-01T00:00:00.1234567", "abcd-ef-gh", "", "2026", "2026-", "2026-W", "2026-W1", "2026-W01-",
	"2026-01-01T00:00:00.+01:00", "2026-01-01T00:00:00.123456x+01:00", "2026-01-01\u00e900:00:00", "2026-01-01\u00a000:00",
	"2026-01-01T00:00\u00e9", "2026-01-01T00Z", "2026-01-01T0Z", "2026-01-01T00:00:00+05:30Z", "2026-01-01T00:00:00-0000",
	"2026-01-01T00:00:00.5x+01:00", "2026-01-01T00:00:00,+01:00", "2026-01-01T00:00x+01:00", "2026-01-01T00x+01:00",
	"2026-01-01T0000x+01:00", "2026-01-01T00:00:00.1234567x", "2026-01-01T00:00:00.123456x", "2026-01-01T0000001",
	"2026-01-01T0000001+01:00", "2026-01-01T000000123", "2026-01-01T00:+01:00", "2026-01-01T00:00:+01:00",
	"2026\u00e901-01", "2026-01\u00e901", "2026-W01\u00e9", "2026-01-01T00:00:00+05:30:15.5", "2026-01-01T00:00:00-05:30:15.5",
	"2026-01-01T00:00:00+00:00:00.000001", "2026-01-01T00:00:00+23:59:59.999999", "2026-01-01T00:00:00+0000001",
	"2025-W53-1", "2015-W53-1",
}

// TestFromISOFormatMatchesLivePython pins FromISOFormat against Python's
// datetime.fromisoformat over a corpus of every form and near miss, and a
// deterministic fuzz of mutated strings.
func TestFromISOFormatMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := append([]string(nil), fromISOCorpus...)
	random := rand.New(rand.NewSource(6440))
	alphabet := "0123456789-:+.,TWZ tx"
	for _, seed := range fromISOCorpus {
		for range 40 {
			runes := []byte(seed)
			switch random.Intn(3) {
			case 0:
				if len(runes) > 0 {
					runes[random.Intn(len(runes))] = alphabet[random.Intn(len(alphabet))]
				}
			case 1:
				at := random.Intn(len(runes) + 1)
				runes = append(runes[:at], append([]byte{alphabet[random.Intn(len(alphabet))]}, runes[at:]...)...)
			case 2:
				if len(runes) > 0 {
					at := random.Intn(len(runes))
					runes = append(runes[:at], runes[at+1:]...)
				}
			}
			corpus = append(corpus, string(runes))
		}
	}
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonFromISOProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []*[2]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d of %d", len(want), len(corpus))
	}
	accepted := 0
	for index, text := range corpus {
		got, ok := FromISOFormat(text)
		expected := want[index]
		if expected == nil {
			if ok {
				t.Errorf("%q: go accepted %v, python refused", text, got)
			}
			continue
		}
		accepted++
		if !ok {
			t.Errorf("%q: go refused, python %v", text, *expected)
			continue
		}
		wall := got.Time.Add(timeOffset(got)).Format("2006-01-02T15:04:05.000000")
		// Python's utcoffset() as a timedelta: days*86400+seconds and
		// microseconds, normalized; compared as total microseconds.
		tz, wantTZ := "naive", "naive"
		if got.Aware {
			tz = fmt.Sprint(int64(got.Offset)*1_000_000 + int64(got.OffsetMicro))
		}
		if parts, isList := expected[1].([]any); isList {
			wantTZ = fmt.Sprint(int64(parts[0].(float64))*1_000_000 + int64(parts[1].(float64)))
		}
		if wall != expected[0] || tz != wantTZ {
			t.Errorf("%q: go %s %v, python %v", text, wall, tz, *expected)
		}
	}
	if len(corpus) != 5289 || accepted != 538 {
		t.Fatalf("compared %d strings (%d accepted by python), want 5289 (538)", len(corpus), accepted)
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "api-pytime-fromisoformat"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d strings, %d accepted by python", len(corpus), accepted)
}

func timeOffset(value DateTime) time.Duration {
	if !value.Aware {
		return 0
	}
	return time.Duration(value.Offset)*time.Second + time.Duration(value.OffsetMicro)*time.Microsecond
}

const pythonPydanticFormatProgram = `
import json, sys
from datetime import datetime
from pydantic import TypeAdapter
adapter = TypeAdapter(datetime)
out = []
for text in json.loads(sys.stdin.read()):
    try:
        value = datetime.fromisoformat(text)
    except (ValueError, TypeError):
        out.append(None)
        continue
    out.append(json.loads(adapter.dump_json(value)))
print(json.dumps(out))
`

// TestPydanticMatchesLivePydanticDumpJSON pins Pydantic, the pydantic-core
// JSON form of a datetime, on every value datetime.fromisoformat reads from
// the fromisoformat corpus: naive and aware, UTC, offsets with seconds and
// microseconds, negative and sub-minute offsets.
func TestPydanticMatchesLivePydanticDumpJSON(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := append([]string(nil), fromISOCorpus...)
	for _, offset := range []string{"+05:30:15", "+05:30:15.5", "-05:30:15", "-05:30:15.5", "+00:00:59", "-00:00:59",
		"+00:00:00.5", "-00:00:00.5", "-00:00:00.000001", "+23:59:59.999999", "-23:59:59.999999", "+00:01", "-00:01", "Z", "+00:00", "-00:00",
		"+00:00:59.5", "-00:00:59.5", "+00:00:59.499999", "+00:00:00.499999", "-00:00:00.499999", "+23:59:59.5", "+23:59:59.499999",
		"+00:01:59.5", "+00:59:59.999999"} {
		for _, at := range []string{"2026-01-01T00:00:00", "2026-06-15T12:34:56.789", "0001-01-01T12:00:00", "9999-12-31T12:00:00"} {
			corpus = append(corpus, at+offset)
		}
	}
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonPydanticFormatProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []*string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d of %d", len(want), len(corpus))
	}
	compared := 0
	for index, text := range corpus {
		if want[index] == nil {
			continue
		}
		parsed, ok := FromISOFormat(text)
		if !ok {
			t.Errorf("%q: go refused, python %s", text, *want[index])
			continue
		}
		compared++
		if got := Pydantic(parsed); got != *want[index] {
			t.Errorf("%q: go %s, python %s", text, got, *want[index])
		}
	}
	if compared != 166 {
		t.Fatalf("compared %d values, want 166", compared)
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "api-pytime-pydantic"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d values compared", compared)
}
