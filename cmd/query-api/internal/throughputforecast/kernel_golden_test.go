package throughputforecast

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// The golden parity test for the kernel, against
// tests/fixtures/throughput_forecast_golden.json -- captured by
// tests/fixtures/generate_throughput_forecast_golden.py from the REAL
// dev_health_ops.metrics.forecast, before any Python deletion.
//
// Every float is compared by IEEE-754 BIT PATTERN, never by tolerance. A
// tolerance would pass the exact defect class this port is most exposed to:
// arm64 FMA fusion moves a result by one ulp, and one ulp is invisible to any
// epsilon a reviewer would pick -- while math.Ceil downstream turns it into a
// whole week. If these numbers are allowed to be approximately right, the
// fixture is measuring nothing that matters.

type goldenRollingWindow struct {
	WindowWeeks          int       `json:"window_weeks"`
	MeanWeeklyThroughput float64   `json:"mean_weekly_throughput"`
	Samples              []float64 `json:"samples"`
	SampleCount          int       `json:"sample_count"`
	InsufficientHistory  bool      `json:"insufficient_history"`
}

type goldenOverlay struct {
	Kind      string  `json:"kind"`
	Score     float64 `json:"score"`
	Label     string  `json:"label"`
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
	Active    bool    `json:"active"`
}

type goldenExpected struct {
	TeamID              *string               `json:"team_id"`
	WorkScopeID         *string               `json:"work_scope_id"`
	BacklogSize         int                   `json:"backlog_size"`
	HistoryWeeks        int                   `json:"history_weeks"`
	P50Weeks            *int                  `json:"p50_weeks"`
	P75Weeks            *int                  `json:"p75_weeks"`
	P90Weeks            *int                  `json:"p90_weeks"`
	RollingWindows      []goldenRollingWindow `json:"rolling_windows"`
	PrimaryRisk         goldenOverlay         `json:"primary_risk"`
	WipCongestion       goldenOverlay         `json:"wip_congestion"`
	ReviewBottleneck    goldenOverlay         `json:"review_bottleneck"`
	IncidentLoad        goldenOverlay         `json:"incident_load"`
	InsufficientHistory bool                  `json:"insufficient_history"`

	// Only the resolver_paths entry carries this.
	ForecastID string `json:"forecast_id"`
}

type goldenInput struct {
	DailyThroughputs   []int   `json:"daily_throughputs"`
	BacklogSize        int     `json:"backlog_size"`
	TeamID             *string `json:"team_id"`
	WorkScopeID        *string `json:"work_scope_id"`
	HistoryWeeks       int     `json:"history_weeks"`
	CurrentWip         float64 `json:"current_wip"`
	AverageWip         float64 `json:"average_wip"`
	ReviewLatencyHours float64 `json:"review_latency_hours"`
	IncidentCount      float64 `json:"incident_count"`
}

type goldenCase struct {
	Name     string         `json:"name"`
	Why      string         `json:"why"`
	Input    goldenInput    `json:"input"`
	Expected goldenExpected `json:"expected"`
}

type goldenFixture struct {
	Source        string       `json:"source"`
	Cases         []goldenCase `json:"cases"`
	ResolverPaths []goldenCase `json:"resolver_paths"`
}

func loadGolden(t *testing.T) goldenFixture {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "tests", "fixtures", "throughput_forecast_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the throughput forecast golden: %v", err)
	}
	var fixture goldenFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("decode the throughput forecast golden: %v", err)
	}
	// A LOADED-BUT-EMPTY fixture would make every subtest below vacuously
	// pass, which is the failure mode a golden test is least likely to notice
	// about itself.
	if len(fixture.Cases) == 0 {
		t.Fatal("the golden fixture carries no cases -- every assertion below would pass vacuously")
	}
	return fixture
}

// bits renders a float64's exact IEEE-754 pattern, so a failure message shows
// WHICH bits moved rather than two decimal renderings that look identical.
func bits(value float64) string {
	return fmt.Sprintf("%.17g (0x%016x)", value, math.Float64bits(value))
}

func requireSameFloat(t *testing.T, label string, got, want float64) {
	t.Helper()
	if math.Float64bits(got) != math.Float64bits(want) {
		t.Errorf("%s: got %s, live Python produced %s", label, bits(got), bits(want))
	}
}

func requireSameIntPointer(t *testing.T, label string, got, want *int) {
	t.Helper()
	switch {
	case got == nil && want == nil:
	case got == nil:
		t.Errorf("%s: got nil, live Python produced %d", label, *want)
	case want == nil:
		t.Errorf("%s: got %d, live Python produced nil", label, *got)
	case *got != *want:
		t.Errorf("%s: got %d, live Python produced %d", label, *got, *want)
	}
}

func requireSameOverlay(t *testing.T, label string, got riskOverlay, want goldenOverlay) {
	t.Helper()
	if got.kind != want.Kind {
		t.Errorf("%s.kind: got %q, live Python produced %q", label, got.kind, want.Kind)
	}
	if got.label != want.Label {
		t.Errorf("%s.label: got %q, live Python produced %q", label, got.label, want.Label)
	}
	if got.active != want.Active {
		t.Errorf("%s.active: got %v, live Python produced %v", label, got.active, want.Active)
	}
	requireSameFloat(t, label+".score", got.score, want.Score)
	requireSameFloat(t, label+".value", got.value, want.Value)
	requireSameFloat(t, label+".threshold", got.threshold, want.Threshold)
}

func requireSameWindows(t *testing.T, got []rollingWindow, want []goldenRollingWindow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rolling windows: got %d, live Python produced %d", len(got), len(want))
	}
	for index := range want {
		label := fmt.Sprintf("rolling_windows[%d]", index)
		if got[index].windowWeeks != want[index].WindowWeeks {
			t.Errorf("%s.window_weeks: got %d, live Python produced %d",
				label, got[index].windowWeeks, want[index].WindowWeeks)
		}
		if len(got[index].samples) != want[index].SampleCount {
			t.Errorf("%s.sample_count: got %d, live Python produced %d",
				label, len(got[index].samples), want[index].SampleCount)
		}
		if got[index].insufficientHistory != want[index].InsufficientHistory {
			t.Errorf("%s.insufficient_history: got %v, live Python produced %v",
				label, got[index].insufficientHistory, want[index].InsufficientHistory)
		}
		requireSameFloat(t, label+".mean_weekly_throughput",
			got[index].meanWeeklyThroughput, want[index].MeanWeeklyThroughput)

		// The samples themselves, not just their count. The mean can agree
		// while an individual rolling sum is wrong -- an off-by-one in the
		// window stride shifts every sample and leaves the mean of a
		// symmetric series untouched.
		if len(got[index].samples) != len(want[index].Samples) {
			t.Errorf("%s.samples: got %d values, live Python produced %d",
				label, len(got[index].samples), len(want[index].Samples))
			continue
		}
		for sampleIndex := range want[index].Samples {
			requireSameFloat(t,
				fmt.Sprintf("%s.samples[%d]", label, sampleIndex),
				got[index].samples[sampleIndex], want[index].Samples[sampleIndex])
		}
	}
}

// The `MatchesLivePythonBitExact` SUFFIX IS LOAD-BEARING, not a style choice:
// .github/workflows/go.yml's go-arm64-numeric-parity job selects its tests with
// `-run '(MatchesLivePythonBitExact|IntegerPercentilesMatchesLivePython)$'`.
// Rename this test without that suffix and it silently stops running on arm64
// -- which is the only architecture where the FMA fusion this fixture exists to
// catch actually happens.
func TestForecastThroughputCapacityMatchesLivePythonBitExact(t *testing.T) {
	fixture := loadGolden(t)
	for _, testCase := range fixture.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			t.Logf("pins: %s", testCase.Why)
			got, err := forecastThroughputCapacity(
				testCase.Input.DailyThroughputs,
				testCase.Input.BacklogSize,
				testCase.Input.TeamID,
				testCase.Input.WorkScopeID,
				testCase.Input.HistoryWeeks,
				testCase.Input.CurrentWip,
				testCase.Input.AverageWip,
				testCase.Input.ReviewLatencyHours,
				testCase.Input.IncidentCount,
			)
			if err != nil {
				t.Fatalf("forecastThroughputCapacity: %v", err)
			}

			if got.backlogSize != testCase.Expected.BacklogSize {
				t.Errorf("backlog_size: got %d, live Python produced %d",
					got.backlogSize, testCase.Expected.BacklogSize)
			}
			if got.historyWeeks != testCase.Expected.HistoryWeeks {
				t.Errorf("history_weeks: got %d, live Python produced %d",
					got.historyWeeks, testCase.Expected.HistoryWeeks)
			}
			if got.insufficientHistory != testCase.Expected.InsufficientHistory {
				t.Errorf("insufficient_history: got %v, live Python produced %v",
					got.insufficientHistory, testCase.Expected.InsufficientHistory)
			}
			requireSameIntPointer(t, "p50_weeks", got.p50Weeks, testCase.Expected.P50Weeks)
			requireSameIntPointer(t, "p75_weeks", got.p75Weeks, testCase.Expected.P75Weeks)
			requireSameIntPointer(t, "p90_weeks", got.p90Weeks, testCase.Expected.P90Weeks)
			requireSameWindows(t, got.rollingWindows, testCase.Expected.RollingWindows)
			requireSameOverlay(t, "primary_risk", got.primaryRisk, testCase.Expected.PrimaryRisk)
			requireSameOverlay(t, "wip_congestion", got.wipCongestion, testCase.Expected.WipCongestion)
			requireSameOverlay(t, "review_bottleneck", got.reviewBottleneck, testCase.Expected.ReviewBottleneck)
			requireSameOverlay(t, "incident_load", got.incidentLoad, testCase.Expected.IncidentLoad)
		})
	}
}

// TestNoHistoryPayloadMatchesLivePython covers the ONE path the cases above
// cannot reach: resolve_throughput_forecast hand-builds its empty payload from
// compute_rolling_windows and compute_risk_overlays without ever calling
// forecast_throughput_capacity, so no kernel vector exercises it.
func TestNoHistoryPayloadMatchesLivePythonBitExact(t *testing.T) {
	fixture := loadGolden(t)
	if len(fixture.ResolverPaths) != 1 {
		t.Fatalf("expected exactly one resolver-path vector, got %d", len(fixture.ResolverPaths))
	}
	want := fixture.ResolverPaths[0].Expected

	if noHistoryForecastID != want.ForecastID {
		t.Errorf("forecast_id sentinel: got %q, live Python produced %q",
			noHistoryForecastID, want.ForecastID)
	}

	windows, err := computeRollingWindows(nil)
	if err != nil {
		t.Fatalf("computeRollingWindows: %v", err)
	}
	requireSameWindows(t, windows, want.RollingWindows)

	primary, wip, review, incident := computeRiskOverlays(0, 0, 0, 0)
	requireSameOverlay(t, "primary_risk", primary, want.PrimaryRisk)
	requireSameOverlay(t, "wip_congestion", wip, want.WipCongestion)
	requireSameOverlay(t, "review_bottleneck", review, want.ReviewBottleneck)
	requireSameOverlay(t, "incident_load", incident, want.IncidentLoad)

	if !want.InsufficientHistory {
		t.Fatal("the no-history vector must be flagged insufficient_history")
	}
	if want.P50Weeks != nil || want.P75Weeks != nil || want.P90Weeks != nil {
		t.Fatal("the no-history vector must carry no weeks estimate")
	}
}

// TestFsumIsExactWhereARunningSumIsNot is the unit vector behind fsum's
// existence. A running `total += value` loop over these three values loses the
// small addend entirely; an exactly-rounded sum keeps it. Without this, a
// reviewer replacing fsum with a plain loop would see every golden case still
// pass on the machine they happened to run.
func TestFsumIsExactWhereARunningSumIsNot(t *testing.T) {
	values := []float64{1e16, 1.0, -1e16}

	naive := 0.0
	for _, value := range values {
		naive += value
	}
	if naive == 1.0 {
		t.Fatal("the naive loop did not lose the addend on this machine -- " +
			"this vector no longer demonstrates anything and must be replaced")
	}
	if got := fsum(values); got != 1.0 {
		t.Errorf("fsum: got %s, want exactly 1", bits(got))
	}
}

// TestPercentileTakesAFractionNotAPercent guards the single most likely misuse
// of this package's percentile: handing it 50 instead of 0.50, which the
// capacity kernel's percentile next door would accept.
func TestPercentileTakesAFractionNotAPercent(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5}
	if got := percentile(values, 0.50); got != 3 {
		t.Errorf("percentile(0.50): got %v, want the median 3", got)
	}
	// A rank beyond the last index clamps to the maximum through
	// math.Floor/Ceil indexing, so 50 silently reads the top of the
	// distribution rather than erroring.
	if got := percentile(values, 1.0); got != 5 {
		t.Errorf("percentile(1.0): got %v, want the maximum 5", got)
	}
}

// TestPrimaryRiskKeepsTheFirstMaximumOnATie pins the tie-break Python's max()
// gives for free and a Go loop written with `>=` silently reverses.
func TestPrimaryRiskKeepsTheFirstMaximumOnATie(t *testing.T) {
	// WIP ratio 2.0/1.25 = 1.6; review 76.8/48.0 = 1.6. Equal scores, so the
	// EARLIER overlay in (wip, review, incident) order must win.
	primary, _, _, _ := computeRiskOverlays(2.0, 1.0, 76.8, 0)
	if primary.kind != riskKindWIP {
		t.Fatalf("primary risk on a score tie: got %q, want %q (the first maximum)",
			primary.kind, riskKindWIP)
	}
}
