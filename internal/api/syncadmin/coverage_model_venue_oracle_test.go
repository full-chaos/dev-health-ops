package syncadmin

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonCoverageModelProgram is build_sync_coverage_summary's handling of
// a stored payload, then the route's response: json.loads of the stored
// text, dict(), projection_refreshing, the log line's subscripts, and
// SyncCoverageSummaryResponse.model_validate dumped as the route's
// response_model. Any exception is the api's unhandled 500.
const pythonCoverageModelProgram = `
import json, sys
from pydantic import TypeAdapter
from dev_health_ops.api.admin.schemas_flat import SyncCoverageSummaryResponse
adapter = TypeAdapter(SyncCoverageSummaryResponse)
out = []
for case in json.loads(sys.stdin.read()):
    try:
        payload = dict(json.loads(case["text"]))
        payload["projection_refreshing"] = case["refreshing"]
        payload["overall"]["gap_count"], payload["overall"]["failed_range_count"], payload["projection_version"]
        model = SyncCoverageSummaryResponse.model_validate(payload)
        out.append({"ok": adapter.dump_json(model).decode()})
    except Exception as exc:
        out.append({"error": type(exc).__name__})
print(json.dumps(out))
`

type coverageCase struct {
	Name       string `json:"name"`
	Text       string `json:"text"`
	Refreshing bool   `json:"refreshing"`
}

// coverageBase is a projection payload in the shape the projector writes.
func coverageBase() map[string]any {
	rangeOf := func() map[string]any {
		return map[string]any{"since": "2026-01-01T00:00:00+00:00", "before": "2026-01-02T00:00:00+00:00",
			"source_ids": []any{"s1"}, "run_ids": []any{"r1"}}
	}
	return map[string]any{
		"config_id": "c1", "provider": "github", "generated_at": "2026-09-01T10:00:00.123456+00:00",
		"data_basis": "planner", "history_lookback_days": 3650, "truncated_before": "2016-09-03T10:00:00+00:00",
		"coverage_since": "2026-01-01T00:00:00+00:00", "coverage_through": nil, "is_truncated": false,
		"truncation_reason": nil, "projection_version": 2, "projection_complete": true,
		"overall": map[string]any{"health": "gaps", "latest_successful_run_at": "2026-08-31T00:00:00+00:00",
			"latest_covered_through": nil, "next_scheduled_run_at": nil, "gap_count": 1,
			"stale_dataset_count": 0, "failed_range_count": 0},
		"datasets": []any{map[string]any{"dataset_key": "git.commits", "status": "gaps",
			"covered_through": "2026-08-31T00:00:00+00:00", "requested_ranges": []any{rangeOf()},
			"covered_ranges": []any{rangeOf()}, "gaps": []any{rangeOf()}, "stale_ranges": []any{},
			"failed_ranges": []any{}}},
		"sources": []any{map[string]any{"source_id": "s1", "source_name": "org/repo", "status": "gaps",
			"covered_through": nil, "gap_count": 1, "failed_range_count": 0}},
		"backfill_windows": []any{map[string]any{"since": "2026-01-01T00:00:00+00:00",
			"before": "2026-01-02T00:00:00+00:00", "source_ids": []any{"s1"},
			"dataset_keys": []any{"git.commits"}, "reasons": []any{"gap"}}},
	}
}

// coverageProbeValues are the values put in every field in turn: every
// JSON type, lax int/bool/datetime spellings, literal near-misses, and the
// ISO forms datetime.fromisoformat and pydantic read differently.
var coverageProbeValues = []any{
	nil, true, false, 0, 1, 2, 5.0, 5.5, -1, 1e20, "5", " 5 ", "5_0", "x", "", "true", "off",
	[]any{}, []any{"a"}, []any{1}, map[string]any{},
	"healthy", "HEALTHY", "stale", "not_enabled", "running", "planner", "legacy", "lookback_limit", "gap", "failed",
	"paused", "not_scheduled", "gaps", "insufficient_data",
	"2026-01-01", "2026-01-01T00:00:00", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00+00:00",
	"2026-01-01T05:30:00+05:30", "2026-01-01T00:00:00-00:00", "2026-01-01 00:00:00", "2026-01-01T00:00",
	"2026-01-01T00:00:00.5Z", "2026-01-01T00:00:00.1234567Z", "2026-01-01T00:00:00,5Z", "2026-01-01T24:00:00Z",
	"20260101", "20260101T000000Z", "2026-W01-1", "2026-001", "2026-01-01T00:00:00+0530",
	"2026-01-01T00:00:00+05", "2026-01-01T00:00:00.000Z", "2026-02-30T00:00:00Z", "1767225600", "1767225600000",
	1767225600, 1767225600000, 1767225600.5, -1.5, "  2026-01-01T00:00:00Z", "2026-01-01t00:00:00z",
	"2026-01-01T00:00:00.1234567", "2026-01-01T00:00:00.123456789", "2026-01-01T00:00:00,5", "2026-01-01_00:00:00",
	"2026-01-01T00", "2026-01-01T00:00:00.", "2026-01-01T00:00:60", "2026-01-01T00:00:00 ",
}

// coverageFieldPaths names every field of the model tree, as a path from
// the payload root (an int step indexes a list).
var coverageFieldPaths = [][]any{
	{"config_id"}, {"provider"}, {"generated_at"}, {"data_basis"}, {"history_lookback_days"}, {"truncated_before"},
	{"coverage_since"}, {"coverage_through"}, {"is_truncated"}, {"truncation_reason"}, {"projection_version"},
	{"projection_complete"}, {"projection_refreshing"}, {"overall"}, {"datasets"}, {"sources"}, {"backfill_windows"},
	{"overall", "health"}, {"overall", "latest_successful_run_at"}, {"overall", "latest_covered_through"},
	{"overall", "next_scheduled_run_at"}, {"overall", "gap_count"}, {"overall", "stale_dataset_count"},
	{"overall", "failed_range_count"},
	{"datasets", 0}, {"datasets", 0, "dataset_key"}, {"datasets", 0, "status"}, {"datasets", 0, "covered_through"},
	{"datasets", 0, "requested_ranges"}, {"datasets", 0, "covered_ranges"}, {"datasets", 0, "gaps"},
	{"datasets", 0, "stale_ranges"}, {"datasets", 0, "failed_ranges"},
	{"datasets", 0, "gaps", 0}, {"datasets", 0, "gaps", 0, "since"}, {"datasets", 0, "gaps", 0, "before"},
	{"datasets", 0, "gaps", 0, "source_ids"}, {"datasets", 0, "gaps", 0, "run_ids"},
	{"datasets", 0, "gaps", 0, "source_ids", 0},
	{"sources", 0}, {"sources", 0, "source_id"}, {"sources", 0, "source_name"}, {"sources", 0, "status"},
	{"sources", 0, "covered_through"}, {"sources", 0, "gap_count"}, {"sources", 0, "failed_range_count"},
	{"backfill_windows", 0}, {"backfill_windows", 0, "since"}, {"backfill_windows", 0, "before"},
	{"backfill_windows", 0, "source_ids"}, {"backfill_windows", 0, "dataset_keys"}, {"backfill_windows", 0, "reasons"},
	{"backfill_windows", 0, "reasons", 0}, {"backfill_windows", 0, "dataset_keys", 0},
}

const absent = "\x00absent"

// setPath replaces (or, for absent, deletes) the value at path.
func setPath(root any, path []any, value any) {
	parent := root
	for _, step := range path[:len(path)-1] {
		switch typed := step.(type) {
		case string:
			parent = parent.(map[string]any)[typed]
		case int:
			parent = parent.([]any)[typed]
		}
	}
	switch last := path[len(path)-1].(type) {
	case string:
		if value == absent {
			delete(parent.(map[string]any), last)
		} else {
			parent.(map[string]any)[last] = value
		}
	case int:
		list := parent.([]any)
		if value != absent {
			list[last] = value
		}
	}
}

func coverageCases(t *testing.T) []coverageCase {
	t.Helper()
	encode := func(value any) string {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	cases := []coverageCase{
		{Name: "base", Text: encode(coverageBase())},
		{Name: "base refreshing", Text: encode(coverageBase()), Refreshing: true},
		{Name: "null", Text: "null"}, {Name: "number", Text: "5"}, {Name: "string", Text: `"ab"`},
		{Name: "empty list", Text: "[]"}, {Name: "empty object", Text: "{}"},
		{Name: "duplicate key, last wins", Text: strings.Replace(encode(coverageBase()), `"provider":"github"`, `"provider":7,"provider":"github"`, 1)},
	}
	pairs := []any{}
	for key, value := range coverageBase() {
		pairs = append(pairs, []any{key, value})
	}
	cases = append(cases, coverageCase{Name: "list of pairs", Text: encode(pairs)})
	for _, path := range coverageFieldPaths {
		for _, value := range append([]any{absent}, coverageProbeValues...) {
			payload := coverageBase()
			setPath(payload, path, value)
			cases = append(cases, coverageCase{Name: encode(path) + "=" + encode(value), Text: encode(payload)})
		}
	}
	return cases
}

// TestCoverageModelVenueOracleMatchesLivePydantic runs every stored
// payload shape through the api's own SyncCoverageSummaryResponse and
// through getCoverage's Go steps, and requires the same response bytes, or
// an exception on both. It imports the api's schemas, so it needs the full
// project environment: the venue-oracles job discovers it by name.
func TestCoverageModelVenueOracleMatchesLivePydantic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the coverage model oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	cases := coverageCases(t)
	input, _ := json.Marshal(cases)
	command := exec.Command(python, "-c", pythonCoverageModelProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live pydantic: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []struct{ OK, Error string }
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v\n%s", err, output)
	}
	if len(want) != len(cases) {
		t.Fatalf("python answered %d of %d", len(want), len(cases))
	}
	accepted, refused := 0, 0
	for index, c := range cases {
		got, goErr := goCoverage(c)
		switch {
		case want[index].Error != "" && goErr != nil:
			refused++
		case want[index].Error == "" && goErr == nil && got == want[index].OK:
			accepted++
		default:
			t.Errorf("%s:\n go     %s %v\n python %s %s", c.Name, got, goErr, want[index].OK, want[index].Error)
		}
	}
	if len(cases) != 3897 || accepted != 1077 || refused != 2820 {
		t.Fatalf("compared %d of 3897 cases: %d accepted (want 1077), %d refused (want 2820)", len(cases), accepted, refused)
	}
	t.Logf("%d cases: %d accepted, %d refused on both planes", len(cases), accepted, refused)
	venueoracle.WriteProof(t)
}

// goCoverage is getCoverage from the stored text on.
func goCoverage(c coverageCase) (string, error) {
	stored, err := decodeStored(&c.Text)
	if err != nil {
		return "", err
	}
	payload, err := strictDict(stored)
	if err != nil {
		return "", err
	}
	payload.Set("projection_refreshing", c.Refreshing)
	body, err := coverageSummary(payload)
	if err != nil {
		return "", err
	}
	data, err := pyjson.MarshalModel(body)
	return string(data), err
}
