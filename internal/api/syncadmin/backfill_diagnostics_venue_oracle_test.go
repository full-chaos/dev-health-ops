package syncadmin

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonDiagnosticsProgram runs each case's canned ClickHouse rows through
// the api's own build_backfill_metrics_diagnostics (a fake sink answering
// each query by the table it reads) and writes the result as the route
// does: FastAPI's jsonable_encoder, then Starlette's JSONResponse.
const pythonDiagnosticsProgram = `
import json, sys
from datetime import date
from fastapi.encoders import jsonable_encoder
from dev_health_ops.api.services.backfill_diagnostics import build_backfill_metrics_diagnostics
class Sink:
    def __init__(self, case):
        self.case = case
    def query_dicts(self, query, parameters):
        for table in ("repo_metrics_daily", "repo_complexity_daily", "compounding_risk_daily"):
            if table in query:
                return [dict(row, day=date.fromisoformat(row["day"])) for row in self.case[table] or []]
        raise AssertionError(query)
out = []
for case in json.loads(sys.stdin.read()):
    result = build_backfill_metrics_diagnostics(Sink(case), org_id="org",
        range_start=date.fromisoformat(case["start"]), range_end=date.fromisoformat(case["end"]))
    out.append(json.dumps(jsonable_encoder(result), ensure_ascii=False, allow_nan=False, indent=None, separators=(",", ":")))
print(json.dumps(out))
`

type diagnosticsCase struct {
	Start      string           `json:"start"`
	End        string           `json:"end"`
	Metrics    []map[string]any `json:"repo_metrics_daily"`
	Complexity []map[string]any `json:"repo_complexity_daily"`
	Risk       []map[string]any `json:"compounding_risk_daily"`
}

func diagnosticsCases() []diagnosticsCase {
	count := func(day string, n int) map[string]any { return map[string]any{"day": day, "row_count": n} }
	risk := func(day string, values ...int) map[string]any {
		keys := []string{"total_rows", "non_null_rows", "unknown_rows", "missing_rework_churn", "missing_complexity_delta",
			"missing_review_latency", "missing_ownership_signal"}
		row := map[string]any{"day": day}
		for index, key := range keys {
			row[key] = values[index]
		}
		return row
	}
	var cases []diagnosticsCase
	for _, window := range [][2]string{
		{"2026-01-01", "2026-01-03"}, {"2026-01-01", "2026-01-01"}, {"2026-01-03", "2026-01-01"},
		{"2024-02-27", "2024-03-02"}, {"2025-12-30", "2026-01-02"}, {"2026-03-28", "2026-03-31"},
	} {
		start, end := window[0], window[1]
		cases = append(cases,
			diagnosticsCase{Start: start, End: end},
			diagnosticsCase{Start: start, End: end,
				Metrics:    []map[string]any{count(start, 3), count(end, 1), count("2019-01-01", 9)},
				Complexity: []map[string]any{count(start, 2)},
				Risk:       []map[string]any{risk(start, 5, 4, 1, 2, 3, 0, 1), risk(end, 1, 0, 1, 1, 1, 1, 1), risk("2030-01-01", 7, 7, 7, 7, 7, 7, 7)}},
			diagnosticsCase{Start: start, End: end,
				Metrics: []map[string]any{count(start, 1_000_000)},
				Risk:    []map[string]any{risk(end, 0, 0, 0, 0, 0, 0, 0)}},
		)
	}
	return cases
}

// TestBackfillDiagnosticsVenueOracleMatchesLivePython requires the Go
// assembly of the backfill metrics diagnostics to write the same bytes as
// the api's build_backfill_metrics_diagnostics does for the same ClickHouse
// rows: every day of the window in order, a day without rows as zeros,
// rows outside the window ignored, an empty window when the end is before
// the start, the aggregate the sum of the days, and the route's JSON form.
func TestBackfillDiagnosticsVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the diagnostics oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	cases := diagnosticsCases()
	input, _ := json.Marshal(cases)
	command := exec.Command(python, "-c", pythonDiagnosticsProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v\n%s", err, output)
	}
	if len(want) != 18 || len(cases) != 18 {
		t.Fatalf("python answered %d of %d cases, want 18", len(want), len(cases))
	}
	for index, c := range cases {
		day := func(text string) time.Time {
			at, err := time.Parse(time.DateOnly, text)
			if err != nil {
				t.Fatal(err)
			}
			return at
		}
		counts := func(rows []map[string]any) map[string]int64 {
			out := map[string]int64{}
			for _, row := range rows {
				out[row["day"].(string)] = int64(row["row_count"].(int))
			}
			return out
		}
		risk := map[string]bucket{}
		for _, row := range c.Risk {
			value := func(key string) int64 { return int64(row[key].(int)) }
			risk[row["day"].(string)] = bucket{risk: value("total_rows"), nonNull: value("non_null_rows"), unknown: value("unknown_rows"),
				reasons: [4]int64{value("missing_rework_churn"), value("missing_complexity_delta"), value("missing_review_latency"), value("missing_ownership_signal")}}
		}
		got, err := pyjson.Marshal(assembleDiagnostics(day(c.Start), day(c.End), counts(c.Metrics), counts(c.Complexity), risk))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want[index] {
			t.Errorf("case %d %s..%s:\n go     %s\n python %s", index, c.Start, c.End, got, want[index])
		}
	}
	t.Logf("%d cases compared", len(cases))
	venueoracle.WriteProof(t)
}
