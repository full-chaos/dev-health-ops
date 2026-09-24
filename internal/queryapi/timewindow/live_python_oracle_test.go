package timewindow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonTimeWindowProgram runs the real filtering.time_window (through a
// MetricFilter, as the routes build it) and people._time_window, with
// utc_today pinned, for each case on stdin, and prints the four dates or
// "overflow" for an OverflowError. Any other exception fails the run.
const pythonTimeWindowProgram = `
import json, sys
from datetime import date
import dev_health_ops.api.services.filtering as filtering
import dev_health_ops.api.services.people as people
from dev_health_ops.api.models.filters import MetricFilter
cases = json.loads(sys.stdin.read())
out = []
for case in cases:
    today = date.fromisoformat(case["today"])
    filtering.utc_today = lambda: today
    people.utc_today = lambda: today
    try:
        if case["people"]:
            window = people._time_window(int(case["range"]), int(case["compare"]))
        else:
            window = filtering.time_window(MetricFilter(time={
                "range_days": int(case["range"]), "compare_days": int(case["compare"]),
                "start_date": case["start"], "end_date": case["end"],
            }))
        out.append([d.isoformat() for d in window])
    except OverflowError:
        out.append("overflow")
print("RESULT " + json.dumps(out))
`

// TestComputeMatchesLivePythonTimeWindow compares Compute with Python's
// time_window and people._time_window on day counts and dates at the
// edges of Python's date and timedelta limits. A day count past the Go int
// range is sent to Python exactly and to Compute saturated, as the
// validators hand it on.
func TestComputeMatchesLivePythonTimeWindow(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)

	counts := []string{
		"-1000000000000000000000", "-5", "0", "1", "14", "365",
		"738000", "738522", "738523", "739000", "3652058", "3652059",
		"999999999", "1000000000", "2147483648", "9223372036854775807", "1000000000000000000000000000000",
	}
	var dates []*string
	for _, text := range []string{"", "0001-01-01", "0001-01-02", "2026-09-24", "9999-12-30", "9999-12-31"} {
		if text == "" {
			dates = append(dates, nil)
			continue
		}
		dates = append(dates, &text)
	}
	type windowCase struct {
		People  bool    `json:"people"`
		Range   string  `json:"range"`
		Compare string  `json:"compare"`
		Start   *string `json:"start"`
		End     *string `json:"end"`
		Today   string  `json:"today"`
	}
	var cases []windowCase
	for _, today := range []string{"2026-09-24", "9999-12-31", "0001-01-01"} {
		for _, rangeDays := range counts {
			for _, compareDays := range []string{"1", "14", "738522", "999999999", "1000000000", "1000000000000000000000000000000", "-3"} {
				cases = append(cases, windowCase{People: true, Range: rangeDays, Compare: compareDays, Today: today})
				for _, start := range dates {
					for _, end := range dates {
						cases = append(cases, windowCase{Range: rangeDays, Compare: compareDays, Start: start, End: end, Today: today})
					}
				}
			}
		}
	}

	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-c", pythonTimeWindowProgram)
	command.Stdin = bytes.NewReader(payload)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	var results []any
	for _, line := range strings.Split(string(output), "\n") {
		if rest, ok := strings.CutPrefix(line, "RESULT "); ok {
			if err := json.Unmarshal([]byte(rest), &results); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(results) != len(cases) {
		t.Fatalf("python answered %d of %d cases", len(results), len(cases))
	}
	overflows, windows := 0, 0
	for index, item := range cases {
		today := mustDate(t, item.Today)
		var start, end *time.Time
		if item.Start != nil {
			start = new(mustDate(t, *item.Start))
		}
		if item.End != nil {
			end = new(mustDate(t, *item.End))
		}
		window, err := Compute(saturated(t, item.Range), saturated(t, item.Compare), start, end, today)
		var got any = "overflow"
		if err == nil {
			got = []any{day(window.StartDay), day(window.EndDay), day(window.CompareStart), day(window.CompareEnd)}
		}
		if fmt.Sprint(got) != fmt.Sprint(results[index]) {
			t.Errorf("%+v: go %v, python %v", item, got, results[index])
			continue
		}
		if err != nil {
			overflows++
		} else {
			windows++
		}
	}
	if overflows == 0 || windows == 0 {
		t.Fatalf("one-sided comparison: %d overflows, %d windows", overflows, windows)
	}
	t.Logf("%d cases match Python: %d windows, %d OverflowErrors", len(cases), windows, overflows)
	if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" && !t.Failed() {
		if err := os.WriteFile(filepath.Join(proof, "query-api-time-window"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func mustDate(t *testing.T, text string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.DateOnly, text)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func day(t time.Time) string { return t.Format(time.DateOnly) }

// saturated is the validators' reading of a Python int: clamped to the Go
// int range.
func saturated(t *testing.T, text string) int {
	t.Helper()
	number, ok := new(big.Int).SetString(text, 10)
	if !ok {
		t.Fatalf("bad count %q", text)
	}
	switch {
	case number.Cmp(big.NewInt(math.MaxInt)) > 0:
		return math.MaxInt
	case number.Cmp(big.NewInt(math.MinInt)) < 0:
		return math.MinInt
	}
	return int(number.Int64())
}
