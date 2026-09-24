package syncadmin

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonUnitsProgram runs each probe through the api's own code:
// _unit_to_response and SyncRunUnitResponse, SyncRunService.build_unit_rollups,
// compute_watermark_lag, build_watermark_index(...).resolve,
// _watermark_behavior and _dataset_cost_class. Env cases run in fresh
// processes (the resolver reads the environment at call time, but the
// planner module is imported once per process).
const pythonUnitsProgram = `
import json, sys, uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from pydantic import TypeAdapter
from dev_health_ops.api.admin.routers.integrations import _unit_to_response
from dev_health_ops.api.admin.schemas.integrations import SyncRunUnitResponse
from dev_health_ops.api.services.integrations import SyncRunService, _dataset_cost_class
from dev_health_ops.sync.watermark_lag import compute_watermark_lag, build_watermark_index
from dev_health_ops.sync.datasets import _watermark_behavior
adapter = TypeAdapter(SyncRunUnitResponse)
payload = json.loads(sys.stdin.read())
def ts(text):
    return None if text is None else datetime.fromisoformat(text)
def unit_of(case):
    source = None
    if case.get("source"):
        source = SimpleNamespace(**case["source"])
    return SimpleNamespace(
        id=uuid.UUID(case["id"]), org_id="org", sync_run_id=uuid.UUID(int=1), integration_id=uuid.UUID(int=2),
        source_id=uuid.UUID(case["source_id"]), source=source, provider=case.get("provider", "github"),
        dataset_key=case["dataset_key"], cost_class=case["cost_class"], mode="incremental",
        since_at=None, before_at=None, status=case["status"], attempts=1, available_at=ts(case.get("available_at")),
        rate_limit_deferrals=0, budget_deferrals=0, duration_seconds=case.get("duration_seconds"), error=None,
        last_heartbeat_at=None, result=case.get("result"), processor_flags=None,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc), updated_at=datetime(2026, 1, 2, tzinfo=timezone.utc))
out = {"responses": [], "rollups": [], "lags": [], "resolves": [], "registry": []}
for case in payload["responses"]:
    try:
        out["responses"].append({"ok": adapter.dump_json(_unit_to_response(unit_of(case))).decode()})
    except Exception as exc:
        out["responses"].append({"error": type(exc).__name__})
for group in payload["rollups"]:
    out["rollups"].append(json.dumps(SyncRunService.build_unit_rollups([unit_of(c) for c in group])))
for case in payload["lags"]:
    lag = compute_watermark_lag(cost_class=case["cost_class"], watermark_at=ts(case["watermark_at"]), now=ts(case["now"]),
        window_cap_days=case["cap"], net_advance_seconds=case["net"])
    out["lags"].append([None if lag.watermark_at is None else lag.watermark_at.isoformat(), lag.lag_seconds,
        lag.catching_up, lag.ticks_behind, lag.window_cap_days])
for case in payload["resolves"]:
    rows = [SimpleNamespace(source_id=r[0], dataset_key=r[1], repo_id=r[2], target=r[3], last_synced_at=ts(r[4])) for r in case["rows"]]
    index = build_watermark_index(rows)
    got = index.resolve(case["source"], case["dataset"])
    out["resolves"].append(None if got is None else got.isoformat())
for provider, dataset, unit_class in payload["registry"]:
    out["registry"].append([_watermark_behavior(dataset).value, _dataset_cost_class(provider, dataset, unit_class)])
print(json.dumps(out))
`

// pythonRatchetProgram prints the lag module's cap and net advance for the
// process's own environment.
const pythonRatchetProgram = `
import json
from dev_health_ops.sync.watermark_lag import heavy_max_window_days, heavy_net_advance_seconds
print(json.dumps([heavy_max_window_days(), heavy_net_advance_seconds()]))
`

type unitsCase struct {
	ID              string         `json:"id"`
	SourceID        string         `json:"source_id"`
	Source          map[string]any `json:"source,omitempty"`
	Provider        string         `json:"provider,omitempty"`
	DatasetKey      string         `json:"dataset_key"`
	CostClass       string         `json:"cost_class"`
	Status          string         `json:"status"`
	AvailableAt     *string        `json:"available_at,omitempty"`
	DurationSeconds *int64         `json:"duration_seconds,omitempty"`
	Result          any            `json:"result,omitempty"`
}

func (c unitsCase) decoded(t *testing.T) decodedUnit {
	t.Helper()
	unit := decodedUnit{runUnit: runUnit{
		ID: uuid.MustParse(c.ID), SyncRunID: uuid.UUID{15: 1}, IntegrationID: uuid.UUID{15: 2},
		SourceID: uuid.MustParse(c.SourceID), OrgID: "org", Provider: c.Provider, DatasetKey: c.DatasetKey,
		CostClass: c.CostClass, Mode: "incremental", Status: c.Status, Attempts: 1,
		DurationSeconds: c.DurationSeconds,
		CreatedAt:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}}
	if unit.Provider == "" {
		unit.Provider = "github"
	}
	if c.AvailableAt != nil {
		at, err := time.Parse(time.RFC3339Nano, *c.AvailableAt)
		if err != nil {
			t.Fatal(err)
		}
		unit.AvailableAt = &at
	}
	if c.Source != nil {
		unit.HasSource = true
		name, _ := c.Source["name"].(string)
		unit.SourceName = &name
		if full, ok := c.Source["full_name"].(string); ok {
			unit.SourceFullName = &full
		}
	}
	if c.Result != nil {
		text, err := json.Marshal(c.Result)
		if err != nil {
			t.Fatal(err)
		}
		value, err := pyjson.DecodeString(string(text))
		if err != nil {
			t.Fatal(err)
		}
		unit.result = value
		unit.resultDict, _ = value.(*pyjson.Object)
	}
	return unit
}

// unitResultProbes are result-dict values for every retry field.
var unitResultProbes = []any{
	nil, true, false, 0, 1, 3.0, 3.5, "3", " 3 ", "x", "", "yes", "off", []any{}, []any{"a", "b"}, []any{1},
	map[string]any{}, "2026-01-01T00:00:00", "2026-01-01T00:00:00+02:00", "2026-01-01", 1767225600, 1e20,
}

var unitResultKeys = []string{
	"error_category", "retry_count", "retry_reason", "last_lease_expired_at", "next_retry_at",
	"retry_exhausted", "retry_surfaces", "linear_page_count", "linear_batch_count",
}

func unitsOracleInput() (responses []unitsCase, rollups [][]unitsCase, lags []map[string]any, resolves []map[string]any, registry [][3]string) {
	source := map[string]any{"name": "repo", "full_name": "org/repo", "external_id": "org/repo"}
	id := func(n int) string { return uuid.UUID{0: byte(n >> 8), 15: byte(n)}.String() }
	at := "2026-03-04T05:06:07.123456+00:00"
	n := 0
	next := func() string { n++; return id(n) }
	for _, key := range unitResultKeys {
		for _, value := range unitResultProbes {
			for _, status := range []string{"retrying", "failed"} {
				responses = append(responses, unitsCase{ID: next(), SourceID: id(9000), Source: source,
					DatasetKey: "commits", CostClass: "medium", Status: status, AvailableAt: &at,
					Result: map[string]any{key: value}})
			}
		}
	}
	for _, result := range []any{nil, []any{1}, "text", 5, map[string]any{"a": []any{1.5, nil}}} {
		responses = append(responses, unitsCase{ID: next(), SourceID: id(9000), DatasetKey: "commits",
			CostClass: "medium", Status: "retrying", Result: result})
	}

	five, seven := int64(5), int64(7)
	rollups = [][]unitsCase{
		{},
		{{ID: next(), SourceID: id(9001), DatasetKey: "commits", CostClass: "medium", Status: "success", DurationSeconds: &five},
			{ID: next(), SourceID: id(9002), DatasetKey: "prs", CostClass: "light", Status: "failed", DurationSeconds: &seven,
				Result: map[string]any{"error_category": "boom"}},
			{ID: next(), SourceID: id(9001), DatasetKey: "prs", CostClass: "light", Status: "failed", DurationSeconds: &five,
				Result: []any{1}},
			{ID: next(), SourceID: id(9002), DatasetKey: "commits", CostClass: "heavy", Status: "failed",
				Result: map[string]any{"error_category": map[string]any{"x": 1.5}}},
			{ID: next(), SourceID: id(9003), DatasetKey: "files", CostClass: "heavy", Status: "failed",
				Result: map[string]any{"error_category": nil}},
			{ID: next(), SourceID: id(9003), DatasetKey: "files", CostClass: "heavy", Status: "pending", DurationSeconds: &seven}},
		{{ID: next(), SourceID: id(9001), DatasetKey: "a", CostClass: "light", Status: "failed"},
			{ID: next(), SourceID: id(9001), DatasetKey: "b", CostClass: "light", Status: "failed"}},
	}
	var many []unitsCase
	for index := range 107 {
		d := int64(index % 9)
		status := "failed"
		if index%3 == 0 {
			status = "success"
		}
		many = append(many, unitsCase{ID: next(), SourceID: id(9100 + index%4), DatasetKey: fmt.Sprintf("d%d", index%5),
			CostClass: "light", Status: status, DurationSeconds: &d})
	}
	rollups = append(rollups, many)
	// Over the 100 failed-id cap, and a success-only run (no partial
	// summary).
	var overCap, successOnly []unitsCase
	for index := range 103 {
		overCap = append(overCap, unitsCase{ID: next(), SourceID: id(9200 + index%3), DatasetKey: "d", CostClass: "light", Status: "failed"})
	}
	for range 3 {
		successOnly = append(successOnly, unitsCase{ID: next(), SourceID: id(9300), DatasetKey: "d", CostClass: "light", Status: "success"})
	}
	rollups = append(rollups, overCap, successOnly)

	now := "2026-06-01T12:00:00.000500+00:00"
	for _, cost := range []string{"heavy", "medium", "light", "HEAVY"} {
		for _, watermark := range []any{nil, now, "2026-06-01T12:00:00.000499+00:00", "2026-06-01T12:00:00.000501+00:00",
			"2026-05-25T12:00:00.000500+00:00", "2026-05-25T12:00:00.000499+00:00", "2026-05-25T12:00:01+00:00",
			"2026-01-01T00:00:00+00:00", "1900-01-01T00:00:00+00:00", "2030-01-01T00:00:00+00:00",
			"2026-06-01T14:00:00.5+02:00", "2026-05-25T12:00:00.000501+00:00", "2026-01-01T00:00:00.999999+00:00"} {
			for _, capNet := range [][2]int64{{7, 604800}, {7, 1}, {0, 0}, {-3, -5}, {1, 86400 - 3600}, {30, 2592000}} {
				lags = append(lags, map[string]any{"cost_class": cost, "watermark_at": watermark, "now": now,
					"cap": capNet[0], "net": capNet[1]})
			}
		}
	}

	w := func(source, dataset, repo, target string, at any) []any {
		return []any{source, dataset, repo, target, at}
	}
	t1, t2, t3 := "2026-01-01T00:00:00+00:00", "2026-02-01T00:00:00+00:00", "2026-03-01T00:00:00+00:00"
	rowSets := [][][]any{
		{},
		{w("S", "commits", "R", "git", t1)},
		{w("X", "other", "S", "commits", t2)},
		{w("X", "git", "S", "git", t3)},
		{w("S", "commits", "R", "git", nil), w("X", "other", "S", "commits", t2)},
		{w("X", "other", "S", "commits", nil), w("X", "git", "S", "git", t3)},
		{w("S", "commits", "R", "git", t1), w("S", "commits", "R2", "git", t2)},
		{w("X", "git", "S", "git", t3), w("X", "prs", "S", "prs", t1)},
		{w("X", "work-items", "S", "work-items", t2)},
		{w("X", "operational", "S", "operational", t1)},
	}
	for _, rows := range rowSets {
		for _, query := range [][2]string{{"S", "commits"}, {"S", "files"}, {"S", "prs"}, {"S", "work-item-labels"},
			{"S", "git"}, {"R", "commits"}, {"S", "services"}, {"S", "incident-alerts"}, {"S", "unknown"}} {
			resolves = append(resolves, map[string]any{"rows": rows, "source": query[0], "dataset": query[1]})
		}
	}

	datasets := map[string]bool{"unknown-dataset": true, "work-items": true, "Commits": true, "": true}
	for _, provider := range providersync.MatrixProviders() {
		for _, capability := range providersync.Capabilities(provider) {
			datasets[capability.Dataset] = true
		}
	}
	names := make([]string, 0, len(datasets))
	for name := range datasets {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, provider := range append(providersync.MatrixProviders(), "GitHub", "unknown") {
		for _, dataset := range names {
			registry = append(registry, [3]string{provider, dataset, "unit-class"})
		}
	}
	return responses, rollups, lags, resolves, registry
}

// TestRunUnitsModelVenueOracleMatchesLivePython runs every probe through
// the api's own run-units code and the Go port, and requires the same
// answer: the unit response bytes (or an exception on both), the rollup
// dicts, the lag fields at a pinned now, the watermark each precedence
// resolves, the watermark behavior and cost class of every registry pair,
// and the cap and net advance of every environment case.
func TestRunUnitsModelVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the run units oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	responses, rollups, lags, resolves, registry := unitsOracleInput()
	input, err := json.Marshal(map[string]any{"responses": responses, "rollups": rollups, "lags": lags,
		"resolves": resolves, "registry": registry})
	if err != nil {
		t.Fatal(err)
	}
	var want struct {
		Responses []struct{ OK, Error string }
		Rollups   []string
		Lags      [][]any
		Resolves  []*string
		Registry  [][2]string
	}
	runPython(t, python, root, pythonUnitsProgram, string(input), nil, &want)

	compared := 0
	for index, c := range responses {
		body, err := unitResponse(c.decoded(t))
		got := ""
		if err == nil {
			data, marshalErr := pyjson.MarshalModel(body)
			got, err = string(data), marshalErr
		}
		expected := want.Responses[index]
		if (expected.Error != "") != (err != nil) || (err == nil && got != expected.OK) {
			t.Errorf("response %d %v:\n go     %s %v\n python %s %s", index, c.Result, got, err, expected.OK, expected.Error)
		}
		compared++
	}
	for index, group := range rollups {
		units := make([]decodedUnit, len(group))
		for position, c := range group {
			units[position] = c.decoded(t)
		}
		got, err := pyjson.Dumps(unitRollups(units))
		if err != nil {
			t.Fatal(err)
		}
		if got != want.Rollups[index] {
			t.Errorf("rollup %d:\n go     %s\n python %s", index, got, want.Rollups[index])
		}
		compared++
	}
	for index, c := range lags {
		now, _ := time.Parse(time.RFC3339Nano, c["now"].(string))
		var watermark *time.Time
		if text, ok := c["watermark_at"].(string); ok {
			at, _ := time.Parse(time.RFC3339Nano, text)
			watermark = &at
		}
		lag := schedsync.ComputeWatermarkLag(c["cost_class"].(string), watermark, now, int(c["cap"].(int64)), c["net"].(int64))
		got := []any{nil, nil, lag.CatchingUp, nil, float64(lag.WindowCapDays)}
		if lag.WatermarkAt != nil {
			got[0] = lag.WatermarkAt.Format("2006-01-02T15:04:05.999999") + "+00:00"
			if lag.WatermarkAt.Nanosecond() != 0 {
				got[0] = lag.WatermarkAt.Format("2006-01-02T15:04:05.000000") + "+00:00"
			}
		}
		if lag.LagSeconds != nil {
			got[1] = float64(*lag.LagSeconds)
		}
		if lag.TicksBehind != nil {
			got[3] = float64(*lag.TicksBehind)
		}
		if fmt.Sprint(got) != fmt.Sprint(want.Lags[index]) {
			t.Errorf("lag %v:\n go     %v\n python %v", c, got, want.Lags[index])
		}
		compared++
	}
	for index, c := range resolves {
		var rows []schedsync.WatermarkRow
		for _, raw := range c["rows"].([][]any) {
			row := schedsync.WatermarkRow{SourceID: raw[0].(string), DatasetKey: raw[1].(string), RepoID: raw[2].(string), Target: raw[3].(string)}
			if text, ok := raw[4].(string); ok {
				at, _ := time.Parse(time.RFC3339, text)
				row.LastSyncedAt = &at
			}
			rows = append(rows, row)
		}
		resolved := schedsync.NewWatermarkIndex(rows).Resolve(c["source"].(string), c["dataset"].(string))
		got, expected := "<nil>", "<nil>"
		if resolved != nil {
			got = resolved.Format("2006-01-02T15:04:05") + "+00:00"
		}
		if want.Resolves[index] != nil {
			expected = *want.Resolves[index]
		}
		if got != expected {
			t.Errorf("resolve %v: go %s python %s", c, got, expected)
		}
		compared++
	}
	for index, pair := range registry {
		got := [2]string{string(providersync.DatasetWatermark(pair[1])), providersync.DatasetCostClass(pair[0], pair[1], pair[2])}
		if got != want.Registry[index] {
			t.Errorf("registry %v: go %v python %v", pair, got, want.Registry[index])
		}
		compared++
	}

	// The cap and net advance per environment: each case is a fresh Python
	// process and a Go call under the same variables.
	type envCase struct{ capRaw, overlapRaw *string }
	s := func(text string) *string { return &text }
	var envCases []envCase
	for _, capRaw := range []*string{nil, s("7"), s("3"), s(" 5 "), s("+4"), s("0"), s("-2"), s("x"), s(""), s("1")} {
		for _, overlapRaw := range []*string{nil, s("0"), s("3600"), s("86400"), s("604800"), s("700000"), s("-5"), s("x")} {
			envCases = append(envCases, envCase{capRaw, overlapRaw})
		}
	}
	for _, c := range envCases {
		env := map[string]*string{"SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS": c.capRaw, "SYNC_WATERMARK_OVERLAP": c.overlapRaw}
		var pythonAnswer [2]int64
		runPython(t, python, root, pythonRatchetProgram, "", env, &pythonAnswer)
		for key, value := range env {
			if value == nil {
				unsetForTest(t, key)
			} else {
				t.Setenv(key, *value)
			}
		}
		capDays, net := schedsync.HeavyRatchet()
		if [2]int64{int64(capDays), net} != pythonAnswer {
			t.Errorf("ratchet cap=%v overlap=%v: go %d %d python %v", deref(c.capRaw), deref(c.overlapRaw), capDays, net, pythonAnswer)
		}
		compared++
	}

	const wantCompared = 396 + 5 + 6 + 312 + 90 + 0 + 80
	registryCount := len(registry)
	if compared != wantCompared+registryCount || len(responses) != 401 || len(rollups) != 6 || len(lags) != 312 ||
		len(resolves) != 90 || len(envCases) != 80 {
		t.Fatalf("compared %d (responses %d, rollups %d, lags %d, resolves %d, registry %d, env %d)", compared,
			len(responses), len(rollups), len(lags), len(resolves), registryCount, len(envCases))
	}
	t.Logf("%d probes compared, %d registry pairs", compared, registryCount)
	venueoracle.WriteProof(t)
}

func deref(value *string) string {
	if value == nil {
		return "<unset>"
	}
	return fmt.Sprintf("%q", *value)
}

// unsetForTest removes key for the rest of the test and restores it after.
func unsetForTest(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
}

// runPython runs program with input on stdin, env overriding (nil
// removing) the named variables, and decodes the last output line into out.
func runPython(t *testing.T, python, root, program, input string, env map[string]*string, out any) {
	t.Helper()
	command := exec.Command(python, "-c", program)
	var environ []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if _, overridden := env[name]; !overridden {
			environ = append(environ, entry)
		}
	}
	for name, value := range env {
		if value != nil {
			environ = append(environ, name+"="+*value)
		}
	}
	command.Env = append(environ, "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), out); err != nil {
		t.Fatalf("decode: %v\n%s", err, output)
	}
}
