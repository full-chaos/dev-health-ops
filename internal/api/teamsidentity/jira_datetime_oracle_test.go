//go:build integration

package teamsidentity

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const pythonJiraDatetimeProgram = `
import json, sys
from datetime import timedelta
from dev_health_ops.api.services.configuration.jira_activity_inference import JiraActivityInferenceService

svc = JiraActivityInferenceService(None, "o")  # type: ignore[arg-type]
out = []
for text in json.loads(sys.stdin.read()):
    value = svc._parse_jira_datetime(text)
    if value is None:
        out.append(None)
    else:
        offset = value.utcoffset()
        out.append([value.isoformat(), None if offset is None else int(offset / timedelta(microseconds=1))])
print(json.dumps(out))
`

// jiraDatetimeCorpus holds the timestamp shapes Jira produces plus the
// neighbouring ISO 8601 forms datetime.fromisoformat accepts, and inputs it
// rejects. ISO week dates, whitespace before the offset ("10:00:00 +0000",
// which CPython accepts) and other exotic grammars are deliberately absent:
// parseJiraDatetime does not parse them (see its doc comment).
func jiraDatetimeCorpus() []string {
	return []string{
		"2026-09-01T10:00:00.000+0000", "2026-09-01T10:00:00.000+00:00", "2026-09-01T10:00:00Z", "2026-09-01T10:00:00.123456Z",
		"2026-09-01T10:00:00.1234567+0530", "2026-09-01T10:00:00.5-0130", "2026-09-01T10:00:00+05:30:15", "2026-09-01T10:00:00",
		"2026-09-01 10:00:00", "2026-09-01T10:00", "2026-09-01T10", "20260901T101112", "20260901", "2026-09-01", "2026-09-01T101112",
		"2026-09-01T10:11:12,25+01", "2026-09-01T10:00:00+0100", "2026-09-01x10:00:00", "2026-02-30T10:00:00", "2026-13-01T10:00:00",
		"2026-09-01T24:00:00", "2026-12-31T24:00", "2026-09-01T24:00:00.1", "2026-09-01T24:01", "2026-09-01T10:00:00-23:59:59", "2026-09-01T10:00:00-24:00", "2026-09-01T10:00:00+05:30:15.5", "2026-09-01T10:00:00+05:30:00.5", "2026-9-1T1:2:3+0000", "2026-09- 1T10:00:00+0100", "2026-09-01t10:00:00.5+0000", "2026-9-1T10:00:00.5+01:00", "2026-9-1T10:00:00.1234567+0000", "2026-9-1T1:2:3", "2026-02-30T1:2:3+0000", "2026-9-1T1:2:60+0000", "2026-9-1T1:2:3+2400", "2026-9-1T1:2:3-05:30:15.5", "2026-9-1T1:2:3+0530:15", "2026-9-1T1:2:3Z", "2026-9-1T25:2:3+0000", "2026-09-01T10:00:00+05:30:15.123456", "2026-09-01T10:00:00-00:00:00.000001", "2026-09-01T10:00:00+05:30:15.1234567", "2026-09-01T10:00:00+05:30.5", "2026-09-01T10:00:00+05.5", "2026-09-01T10:00:00+053015.5", "2026-09-01T10:00:00+0530.5", "2026-09-01T10:00:00+00:00:00.", "2026-09-01T10:60:00", "2026-09-01T10:00:60", "2026-09-01T10:00:00+24:00", "2026-09-01T10:00:00.Z",
		"2026-09-01T10:00:00+", "2026-09-01T", "2026-9-1T10:00:00", "not a date", "", " ", "0001-01-01T00:00:00Z",
		"9999-12-31T23:59:59.999999Z", "2026-09-01T10:00:00.000+00:00Z", "2026-09-01T1:00:00",
	}
}

func goJiraDatetime(text string) any {
	stamp := parseJiraDatetime(text)
	if stamp == nil {
		return nil
	}
	offset := time.Duration(stamp.Offset)*time.Second + time.Duration(stamp.OffsetMicro)*time.Microsecond
	wall := stamp.Time
	if stamp.Aware {
		wall = stamp.Time.Add(offset)
	}
	iso := wall.UTC().Format("2006-01-02T15:04:05")
	if micro := wall.Nanosecond() / 1000; micro != 0 {
		iso += fmt.Sprintf(".%06d", micro)
	}
	if !stamp.Aware {
		return []any{iso, nil}
	}
	sign := "+"
	abs := int64(offset / time.Microsecond)
	if abs < 0 {
		sign, abs = "-", -abs
	}
	whole, micro := abs/1_000_000, abs%1_000_000
	iso += fmt.Sprintf("%s%02d:%02d", sign, whole/3600, whole%3600/60)
	if whole%60 != 0 || micro != 0 {
		iso += fmt.Sprintf(":%02d", whole%60)
	}
	if micro != 0 {
		iso += fmt.Sprintf(".%06d", micro)
	}
	return []any{iso, int64(offset / time.Microsecond)}
}

// TestJiraDatetimeParseMatchesPython compares parseJiraDatetime with
// JiraActivityInferenceService._parse_jira_datetime over the corpus above:
// parsed or not, the rendered isoformat and the UTC offset in seconds.
func TestJiraDatetimeParseMatchesPython(t *testing.T) {
	python := requireMemberOracleEnv(t)
	corpus := jiraDatetimeCorpus()
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonJiraDatetimeProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d for %d inputs", len(want), len(corpus))
	}
	mismatches := 0
	for index, text := range corpus {
		got, _ := json.Marshal(goJiraDatetime(text))
		expected, _ := json.Marshal(want[index])
		if string(got) != string(expected) {
			mismatches++
			t.Errorf("%q: go %s, python %s", text, got, expected)
		}
	}
	t.Logf("%d timestamps compared, %d mismatches", len(corpus), mismatches)
	venueoracle.WriteProof(t)
}
