package benchmarking

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	goldenOrgID = "org-benchmarking-golden"
)

func goldenAsOf() time.Time  { return time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) }
func goldenStamp() time.Time { return time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) }

// series builds one point per day ending on endDay, mirroring the generator's
// `_series` helper exactly.
func series(values []float64, endDay time.Time) []MetricPoint {
	start := endDay.AddDate(0, 0, -(len(values) - 1))
	points := make([]MetricPoint, len(values))
	for index, value := range values {
		points[index] = MetricPoint{Day: start.AddDate(0, 0, index), Value: value}
	}
	return points
}

// baselineSeries mirrors the generator's BASELINE_SERIES, scope for scope.
func baselineSeries() map[string][]MetricPoint {
	asOf := goldenAsOf()
	return map[string][]MetricPoint{
		"scope-a": series([]float64{0.1, 0.2, 0.3, 0.7, 1.1, 2.9, 5.5, 0.15, 0.25, 3.35, 1.05, 0.95, 4.45, 2.15}, asOf),
		"scope-b": series([]float64{5000000.0, 5000000.5, 5000001.0, 5000000.25, 5000000.75, 5000002.0, 5000003.5, 5000000.125}, asOf),
		"scope-c": series([]float64{42.0, 42.0, 42.0, 42.0, 42.0, 42.0, 42.0, 42.0}, asOf),
		"scope-d": series([]float64{1.0, 2.0, 3.0, 4.0, 5.0, 12.0}, asOf),
		"scope-e": series([]float64{0.0, 0.0, 0.0, 0.0, 0.0, 0.0}, asOf),
		"scope-f": series([]float64{1.0, 2.0, 3.0, 9.0}, asOf),
		"scope-g": series([]float64{1.0, 2.0, 3.0, 4.0, 5.0, 7.0}, asOf),
		// scope-h: compensated vs naive mean differ AFTER round(x,4). Only
		// ~0.15% of value sets do; without this the oracle is blind to a
		// naive-summation port because the rounding erases ordinary last-bit
		// differences.
		"scope-h": series([]float64{5.8481, 3.6, 3.7054, 5.584, 1.275, 2.742, 2.3621, 1.659}, asOf),
		// scope-i: the isclose discriminator -- zero-variance history at large
		// magnitude. CPython emits NO anomaly; a naive absolute compare emits
		// a spurious "critical" one.
		"scope-i": series([]float64{5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.000000005}, asOf),
	}
}

// goldenDocument mirrors the generator's six collections. Every field is the
// Python dataclass name; dates and datetimes arrive as isoformat strings.
type goldenDocument struct {
	Baselines []struct {
		MetricName        string  `json:"metric_name"`
		ScopeType         string  `json:"scope_type"`
		ScopeKey          string  `json:"scope_key"`
		PeriodStart       string  `json:"period_start"`
		PeriodEnd         string  `json:"period_end"`
		RollingWindowDays int     `json:"rolling_window_days"`
		CurrentValue      float64 `json:"current_value"`
		BaselineValue     float64 `json:"baseline_value"`
		PercentileRank    float64 `json:"percentile_rank"`
		P25Value          float64 `json:"p25_value"`
		P50Value          float64 `json:"p50_value"`
		P75Value          float64 `json:"p75_value"`
		P90Value          float64 `json:"p90_value"`
		SampleSize        int     `json:"sample_size"`
		ComputedAt        string  `json:"computed_at"`
		OrgID             string  `json:"org_id"`
	} `json:"baselines"`
	MaturityBands []struct {
		MetricName     string  `json:"metric_name"`
		ScopeType      string  `json:"scope_type"`
		ScopeKey       string  `json:"scope_key"`
		PeriodStart    string  `json:"period_start"`
		PeriodEnd      string  `json:"period_end"`
		Value          float64 `json:"value"`
		PercentileRank float64 `json:"percentile_rank"`
		MaturityBand   string  `json:"maturity_band"`
		Confidence     float64 `json:"confidence"`
		ComputedAt     string  `json:"computed_at"`
		OrgID          string  `json:"org_id"`
	} `json:"maturity_bands"`
	Anomalies []struct {
		MetricName      string  `json:"metric_name"`
		ScopeType       string  `json:"scope_type"`
		ScopeKey        string  `json:"scope_key"`
		Day             string  `json:"day"`
		Value           float64 `json:"value"`
		BaselineValue   float64 `json:"baseline_value"`
		ZScore          float64 `json:"z_score"`
		AnomalyType     string  `json:"anomaly_type"`
		Direction       string  `json:"direction"`
		Severity        string  `json:"severity"`
		VolatilityScore float64 `json:"volatility_score"`
		ComputedAt      string  `json:"computed_at"`
		OrgID           string  `json:"org_id"`
	} `json:"anomalies"`
	PeriodComparisons []struct {
		MetricName            string   `json:"metric_name"`
		ScopeType             string   `json:"scope_type"`
		ScopeKey              string   `json:"scope_key"`
		CurrentPeriodStart    string   `json:"current_period_start"`
		CurrentPeriodEnd      string   `json:"current_period_end"`
		ComparisonPeriodStart string   `json:"comparison_period_start"`
		ComparisonPeriodEnd   string   `json:"comparison_period_end"`
		CurrentValue          float64  `json:"current_value"`
		ComparisonValue       float64  `json:"comparison_value"`
		AbsoluteDelta         float64  `json:"absolute_delta"`
		PercentageChange      *float64 `json:"percentage_change"`
		TrendDirection        string   `json:"trend_direction"`
		ComputedAt            string   `json:"computed_at"`
		OrgID                 string   `json:"org_id"`
	} `json:"period_comparisons"`
	Correlations []struct {
		MetricName       string  `json:"metric_name"`
		PairedMetricName string  `json:"paired_metric_name"`
		ScopeType        string  `json:"scope_type"`
		ScopeKey         string  `json:"scope_key"`
		PeriodStart      string  `json:"period_start"`
		PeriodEnd        string  `json:"period_end"`
		Coefficient      float64 `json:"coefficient"`
		PValue           float64 `json:"p_value"`
		SampleSize       int     `json:"sample_size"`
		IsSignificant    bool    `json:"is_significant"`
		Interpretation   string  `json:"interpretation"`
		ComputedAt       string  `json:"computed_at"`
		OrgID            string  `json:"org_id"`
	} `json:"correlations"`
	Insights []struct {
		InsightID        string  `json:"insight_id"`
		InsightType      string  `json:"insight_type"`
		ScopeType        string  `json:"scope_type"`
		ScopeKey         string  `json:"scope_key"`
		MetricName       string  `json:"metric_name"`
		PairedMetricName *string `json:"paired_metric_name"`
		PeriodStart      *string `json:"period_start"`
		PeriodEnd        *string `json:"period_end"`
		Severity         string  `json:"severity"`
		Summary          string  `json:"summary"`
		EvidenceJSON     string  `json:"evidence_json"`
		ComputedAt       string  `json:"computed_at"`
		OrgID            string  `json:"org_id"`
	} `json:"insights"`
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find repository root (no go.mod found)")
		}
		directory = parent
	}
}

func loadGolden(t *testing.T) goldenDocument {
	t.Helper()
	path := filepath.Join(
		repositoryRoot(t), "tests", "fixtures", "daily_benchmarking_python_golden.json",
	)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read benchmarking golden: %v (this fixture is frozen -- "+
			"CHAOS-4288 deleted the Python generator, do not recreate it)", err)
	}
	var document goldenDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parse benchmarking golden: %v", err)
	}
	return document
}

func isoDay(day time.Time) string  { return day.Format("2006-01-02") }
func isoStamp(at time.Time) string { return at.Format("2006-01-02T15:04:05-07:00") }

// goldenOutputs runs the Go computes over the same corpus the generator uses.
func goldenOutputs(t *testing.T) Outputs {
	t.Helper()
	asOf := goldenAsOf()
	stamp := goldenStamp()
	seriesByScope := baselineSeries()

	baselines := ComputeInternalBaselines(
		"success_rate", ScopeRepo, seriesByScope, asOf, stamp, DefaultBaselineWindows, goldenOrgID,
	)
	maturity := ClassifyMaturityBands(baselines)

	anomalies := DetectMetricAnomalies(
		"success_rate", ScopeRepo, seriesByScope, asOf, stamp,
		DefaultRollingWindowDays, DefaultZThreshold, DefaultVolatilityThresh,
		DefaultMinHistoryPoints, goldenOrgID,
	)
	anomalies = append(anomalies, DetectMetricAnomalies(
		"flake_rate", ScopeTeam, seriesByScope, asOf, stamp,
		DefaultRollingWindowDays, DefaultZThreshold, DefaultVolatilityThresh,
		DefaultMinHistoryPoints, goldenOrgID,
	)...)

	currentEnd := asOf
	currentStart := asOf.AddDate(0, 0, -6)
	priorEnd := currentStart.AddDate(0, 0, -1)
	priorStart := priorEnd.AddDate(0, 0, -6)

	periodCases := []struct {
		metric     string
		scopeKey   string
		current    []float64
		comparison []float64
	}{
		{"success_rate", "scope-a", []float64{0.90, 0.92, 0.94}, []float64{0.70, 0.72, 0.74}},
		{"success_rate", "scope-zero", []float64{0.5, 0.5, 0.5}, []float64{0.0, 0.0, 0.0}},
		{"success_rate", "scope-flat", []float64{0.5, 0.5}, []float64{0.5, 0.5}},
		{"flake_rate", "scope-a", []float64{0.30, 0.32}, []float64{0.10, 0.11}},
		{"success_rate", "scope-five", []float64{1.05}, []float64{1.0}},
	}
	var comparisons []PeriodComparisonRecord
	for _, testCase := range periodCases {
		record, ok := ComputePeriodComparison(
			testCase.metric, ScopeRepo, testCase.scopeKey,
			currentStart, currentEnd, priorStart, priorEnd,
			series(testCase.current, currentEnd), series(testCase.comparison, priorEnd),
			stamp, goldenOrgID,
		)
		if ok {
			comparisons = append(comparisons, record)
		}
	}

	correlationStart := asOf.AddDate(0, 0, -29)
	left := map[string][]MetricPoint{
		"scope-a":     series([]float64{1, 2, 3, 4, 5, 6, 7}, asOf),
		"scope-weak":  series([]float64{1, 2, 3, 4, 5, 6, 7}, asOf),
		"scope-short": series([]float64{1, 2, 3, 4}, asOf),
	}
	right := map[string][]MetricPoint{
		"scope-a":     series([]float64{2.3, 4.1, 6.4, 8.2, 10.5, 12.1, 14.4}, asOf),
		"scope-weak":  series([]float64{5, 4, 6, 3, 7, 2, 8}, asOf),
		"scope-short": series([]float64{9, 8, 7, 6}, asOf),
	}
	correlations := ComputeMetricCorrelation(
		"flake_rate", "cycle_time_hours", ScopeTeam, left, right,
		correlationStart, asOf, stamp, DefaultCorrelationMinPoints, goldenOrgID,
	)

	insights, err := GenerateBenchmarkInsights(comparisons, anomalies, correlations, stamp)
	if err != nil {
		t.Fatalf("generate insights: %v", err)
	}

	return Outputs{
		Baselines:         baselines,
		MaturityBands:     maturity,
		Anomalies:         anomalies,
		PeriodComparisons: comparisons,
		Correlations:      correlations,
		Insights:          insights,
	}
}

// TestComputeMatchesFrozenPythonGolden is the differential oracle for all six
// output collections.
//
// Comparison is EXACT: floats are compared by value after a JSON round-trip
// that preserves them exactly (Python writes repr, Go parses the same digits),
// with no epsilon anywhere. A tolerance would hide the three classes this port
// exists to avoid -- CPython's compensated sum(), the FMA contraction on
// percentile's `rank`, and math.isclose's default rel_tol -- each of which
// moves a row between severity or maturity buckets rather than merely
// perturbing a displayed number.
func TestComputeMatchesFrozenPythonGolden(t *testing.T) {
	golden := loadGolden(t)
	live := goldenOutputs(t)

	if len(live.Baselines) != len(golden.Baselines) {
		t.Fatalf("baselines: got %d rows, want %d", len(live.Baselines), len(golden.Baselines))
	}
	for index, want := range golden.Baselines {
		got := live.Baselines[index]
		gotRow := []any{
			got.MetricName, got.ScopeType, got.ScopeKey, isoDay(got.PeriodStart), isoDay(got.PeriodEnd),
			got.RollingWindowDays, got.CurrentValue, got.BaselineValue, got.PercentileRank,
			got.P25Value, got.P50Value, got.P75Value, got.P90Value, got.SampleSize,
			isoStamp(got.ComputedAt), got.OrgID,
		}
		wantRow := []any{
			want.MetricName, want.ScopeType, want.ScopeKey, want.PeriodStart, want.PeriodEnd,
			want.RollingWindowDays, want.CurrentValue, want.BaselineValue, want.PercentileRank,
			want.P25Value, want.P50Value, want.P75Value, want.P90Value, want.SampleSize,
			want.ComputedAt, want.OrgID,
		}
		if !reflect.DeepEqual(gotRow, wantRow) {
			t.Errorf("baseline %d (%s/%dd):\n got  %v\n want %v", index, want.ScopeKey, want.RollingWindowDays, gotRow, wantRow)
		}
	}

	if len(live.MaturityBands) != len(golden.MaturityBands) {
		t.Fatalf("maturity_bands: got %d rows, want %d", len(live.MaturityBands), len(golden.MaturityBands))
	}
	for index, want := range golden.MaturityBands {
		got := live.MaturityBands[index]
		gotRow := []any{got.ScopeKey, got.Value, got.PercentileRank, got.MaturityBand, got.Confidence, got.OrgID}
		wantRow := []any{want.ScopeKey, want.Value, want.PercentileRank, want.MaturityBand, want.Confidence, want.OrgID}
		if !reflect.DeepEqual(gotRow, wantRow) {
			t.Errorf("maturity band %d:\n got  %v\n want %v", index, gotRow, wantRow)
		}
	}

	if len(live.Anomalies) != len(golden.Anomalies) {
		t.Fatalf("anomalies: got %d rows, want %d", len(live.Anomalies), len(golden.Anomalies))
	}
	for index, want := range golden.Anomalies {
		got := live.Anomalies[index]
		gotRow := []any{
			got.MetricName, got.ScopeType, got.ScopeKey, isoDay(got.Day), got.Value,
			got.BaselineValue, got.ZScore, got.AnomalyType, got.Direction, got.Severity,
			got.VolatilityScore, got.OrgID,
		}
		wantRow := []any{
			want.MetricName, want.ScopeType, want.ScopeKey, want.Day, want.Value,
			want.BaselineValue, want.ZScore, want.AnomalyType, want.Direction, want.Severity,
			want.VolatilityScore, want.OrgID,
		}
		if !reflect.DeepEqual(gotRow, wantRow) {
			t.Errorf("anomaly %d (%s):\n got  %v\n want %v", index, want.ScopeKey, gotRow, wantRow)
		}
	}

	if len(live.PeriodComparisons) != len(golden.PeriodComparisons) {
		t.Fatalf("period_comparisons: got %d rows, want %d", len(live.PeriodComparisons), len(golden.PeriodComparisons))
	}
	for index, want := range golden.PeriodComparisons {
		got := live.PeriodComparisons[index]
		gotRow := []any{
			got.MetricName, got.ScopeKey, got.CurrentValue, got.ComparisonValue,
			got.AbsoluteDelta, got.PercentageChange, got.TrendDirection,
		}
		wantRow := []any{
			want.MetricName, want.ScopeKey, want.CurrentValue, want.ComparisonValue,
			want.AbsoluteDelta, want.PercentageChange, want.TrendDirection,
		}
		if !reflect.DeepEqual(gotRow, wantRow) {
			t.Errorf("period comparison %d (%s):\n got  %v\n want %v", index, want.ScopeKey, gotRow, wantRow)
		}
	}

	if len(live.Correlations) != len(golden.Correlations) {
		t.Fatalf("correlations: got %d rows, want %d", len(live.Correlations), len(golden.Correlations))
	}
	for index, want := range golden.Correlations {
		got := live.Correlations[index]
		gotRow := []any{
			got.MetricName, got.PairedMetricName, got.ScopeKey, got.Coefficient,
			got.PValue, got.SampleSize, got.IsSignificant, got.Interpretation,
		}
		wantRow := []any{
			want.MetricName, want.PairedMetricName, want.ScopeKey, want.Coefficient,
			want.PValue, want.SampleSize, want.IsSignificant, want.Interpretation,
		}
		if !reflect.DeepEqual(gotRow, wantRow) {
			t.Errorf("correlation %d (%s):\n got  %v\n want %v", index, want.ScopeKey, gotRow, wantRow)
		}
	}

	if len(live.Insights) != len(golden.Insights) {
		t.Fatalf("insights: got %d rows, want %d", len(live.Insights), len(golden.Insights))
	}
	for index, want := range golden.Insights {
		got := live.Insights[index]
		gotRow := []any{
			got.InsightID, got.InsightType, got.ScopeType, got.ScopeKey, got.MetricName,
			got.Severity, got.Summary, got.EvidenceJSON, got.OrgID,
		}
		wantRow := []any{
			want.InsightID, want.InsightType, want.ScopeType, want.ScopeKey, want.MetricName,
			want.Severity, want.Summary, want.EvidenceJSON, want.OrgID,
		}
		if !reflect.DeepEqual(gotRow, wantRow) {
			t.Errorf("insight %d (%s):\n got  %v\n want %v", index, want.InsightType, gotRow, wantRow)
		}
	}
}

// TestTheCorpusExercisesEveryBranchItClaimsTo is the guard on the GUARD.
//
// The frozen golden is only worth what it covers. This fails if the fixture
// stops exercising a branch the port depends on -- so a future corpus edit that
// quietly drops, say, the only "warning" anomaly or the only suppressed
// correlation cannot leave the oracle passing while proving less.
func TestTheCorpusExercisesEveryBranchItClaimsTo(t *testing.T) {
	golden := loadGolden(t)

	severities := map[string]bool{}
	anomalyTypes := map[string]bool{}
	for _, anomaly := range golden.Anomalies {
		severities[anomaly.Severity] = true
		anomalyTypes[anomaly.AnomalyType] = true
	}
	for _, required := range []string{"warning", "critical"} {
		if !severities[required] {
			t.Errorf("corpus no longer produces a %q anomaly -- the severity ladder is untested", required)
		}
	}
	for _, required := range []string{"improvement", "regression", "volatility"} {
		if !anomalyTypes[required] {
			t.Errorf("corpus no longer produces a %q anomaly", required)
		}
	}

	bands := map[string]bool{}
	for _, band := range golden.MaturityBands {
		bands[band.MaturityBand] = true
	}
	for _, required := range []string{"emerging", "developing", "established", "leading"} {
		if !bands[required] {
			t.Errorf("corpus no longer produces the %q maturity band", required)
		}
	}

	trends := map[string]bool{}
	sawNullPercentage := false
	for _, comparison := range golden.PeriodComparisons {
		trends[comparison.TrendDirection] = true
		if comparison.PercentageChange == nil {
			sawNullPercentage = true
		}
	}
	for _, required := range []string{"improving", "regressing", "stable"} {
		if !trends[required] {
			t.Errorf("corpus no longer produces the %q trend direction", required)
		}
	}
	if !sawNullPercentage {
		t.Error("corpus no longer produces a NULL percentage_change -- the zero-comparison-value branch is untested")
	}

	sawSignificant, sawInsignificant := false, false
	for _, correlation := range golden.Correlations {
		if correlation.IsSignificant {
			sawSignificant = true
		} else {
			sawInsignificant = true
		}
	}
	if !sawSignificant || !sawInsignificant {
		t.Error("corpus must contain BOTH a significant and an insignificant correlation -- " +
			"the insight-suppression branch is only covered by the latter")
	}

	insightTypes := map[string]bool{}
	for _, insight := range golden.Insights {
		insightTypes[insight.InsightType] = true
	}
	for _, required := range []string{"comparison", "anomaly", "correlation"} {
		if !insightTypes[required] {
			t.Errorf("corpus no longer produces a %q insight", required)
		}
	}

	// The libm containment property: no p_value may sit near the 0.05
	// significance threshold, or the is_significant decision becomes a coin
	// flip between CPython's system libm and Go's own implementation.
	for _, correlation := range golden.Correlations {
		if distance := correlation.PValue - 0.05; distance < 1e-6 && distance > -1e-6 {
			t.Errorf(
				"correlation %q has p_value %v, within 1e-6 of the 0.05 significance threshold -- "+
					"is_significant would be runtime-dependent (see FisherTwoTailedPValue's doc comment)",
				correlation.ScopeKey, correlation.PValue,
			)
		}
	}
}

// TestRawPrimitivesMatchCPythonBitForBit proves the numeric barriers directly,
// at the RAW value, because the frozen golden structurally cannot.
//
// # WHY THIS TEST HAS TO EXIST
//
// Every field of every output record is written through round(x, 4) (or 6 for
// p_value). A one-ulp difference in an intermediate -- the kind an FMA
// contraction or a naive summation produces -- is ~1e-16 and the rounding
// erases it. MEASURED: only 587 of 400,000 random value sets produce a
// compensated-vs-naive mean that still differs after round(x,4), i.e. 0.15%.
//
// So a records-level oracle is blind to these classes by construction unless
// the corpus is deliberately seeded with the rare surviving cases (scope-h and
// scope-i above do exactly that, and both were found by search rather than by
// intuition). This test closes the remainder by asserting the primitives
// themselves against values taken from the live interpreter, before any
// rounding can hide a difference.
//
// The reference values below came from CPython via the now-deleted
// dev_health_ops.metrics.benchmarking._common (CHAOS-4288) on the same
// corpus, captured before the Python source was removed.
func TestRawPrimitivesMatchCPythonBitForBit(t *testing.T) {
	values := []float64{0.1, 0.2, 0.3, 0.7, 1.1, 2.9, 5.5}
	paired := make([]float64, len(values))
	for index, value := range values {
		paired[index] = value*2 + 0.3
	}

	for _, testCase := range []struct {
		name string
		got  float64
		want float64
	}{
		// Percentile's FMA barrier lives on `rank`, not on the interpolation:
		// `rank - math.Floor(rank)` fuses the multiply in rank's DEFINITION
		// into the subtraction. Unbarriered this returns 3.9400000000000004.
		{"percentile p90", Percentile(values, 90.0), 3.940000000000001},
		{"percentile p25", Percentile(values, 25.0), 0.25},
		// Mean's numerator is CPython's compensated sum().
		{"mean", Mean(values), 1.542857142857143},
		{"population stdev", PopulationStdev(values), 1.8453471549952274},
		{"percentile rank", PercentileRank(values, 0.7), 50.0},
		{"pearson", PearsonCorrelation(values, paired), 0.9999999999999999},
	} {
		if math.Float64bits(testCase.got) != math.Float64bits(testCase.want) {
			t.Errorf("%s: got %v (%#016x), want %v (%#016x)",
				testCase.name, testCase.got, math.Float64bits(testCase.got),
				testCase.want, math.Float64bits(testCase.want))
		}
	}
}

// TestFisherPValueDiffersFromCPythonOnlyBelowThePersistedPrecision documents
// and BOUNDS the one genuine cross-runtime divergence in this family.
//
// fisher_two_tailed_p_value calls math.log and math.erfc. CPython takes both
// from the system libm; Go implements them itself. The raw results can differ
// in the last bit, and CPython's own answer differs between macOS and the
// Linux CI image for the same reason.
//
// That is contained rather than tolerated: nothing persists the raw value.
// correlations.py:70 writes round(p_value, 6), which is many orders of
// magnitude coarser than the disagreement, so the RECORD is bit-exact on every
// host. This asserts that containment holds -- if a future Go release changed
// Erfc enough to move the 6-decimal value, this fails and the "no tolerance"
// claim gets revisited rather than silently becoming false.
func TestFisherPValueDiffersFromCPythonOnlyBelowThePersistedPrecision(t *testing.T) {
	// CPython (system libm) on this corpus: 6.354651335736281e-05.
	const cpythonRaw = 6.354651335736281e-05
	got := FisherTwoTailedPValue(0.87, 12)

	gotRounded, err := pythonparityRound(got, 6)
	if err != nil {
		t.Fatal(err)
	}
	wantRounded, err := pythonparityRound(cpythonRaw, 6)
	if err != nil {
		t.Fatal(err)
	}
	if math.Float64bits(gotRounded) != math.Float64bits(wantRounded) {
		t.Errorf(
			"round(p_value, 6) diverges from CPython: got %v, want %v -- the libm "+
				"difference has grown past the persisted precision, so this family can no "+
				"longer claim bit-exact records without a tolerance",
			gotRounded, wantRounded,
		)
	}
}

// pythonparityRound is a thin alias so the test reads as the Python call it mirrors.
func pythonparityRound(value float64, digits int) (float64, error) {
	return pythonparity.Round(value, digits)
}

// TestPercentileMatchesCPythonIncludingSingleNaNPlacements pins Percentile at
// the CALL SITE against CPython, and states the class it does NOT pin.
//
// codex r1 on #2235 (F4): the implementation used sort.Float64s, which sorts
// NaN to the FRONT, while CPython's sorted moves it nowhere because every
// comparison against NaN is False. They disagree on [1, NaN, 2], the simplest
// possible case. Measured against real CPython before the fix was chosen:
//
//	input           CPython sorted      sort.Float64s     SliceStable(a<b)
//	[1, NaN, 2]     [1, NaN, 2]         [NaN, 1, 2] X     [1, NaN, 2] ok
//	[NaN, 1, 2]     [NaN, 1, 2]         [NaN, 1, 2] ok    [NaN, 1, 2] ok
//	[2, NaN, 1]     [2, NaN, 1]         [NaN, 1, 2] X     [2, NaN, 1] ok
//	[3, 1, NaN, 2]  [1, 2, 3, NaN]      [NaN,1,2,3] X     [1, 3, NaN, 2] X
//
// The last row is the RESIDUAL and it is deliberately not fixed. CPython's
// ordering under NaN is a Timsort artefact of a non-transitive comparator --
// sorted([2.0, nan, 1.0]) returns [2.0, nan, 1.0], which is not sorted at all
// -- so no stable sort reproduces it in general and matching it would mean
// reimplementing Timsort, i.e. parity with an artefact rather than with a
// specification. Ticketed against BOTH planes; neither should be taking a
// percentile over a NaN-bearing series.
//
// So this test pins exactly what is provably shared: every NaN-free input, and
// the single-NaN placements above. It does NOT assert the four-element case,
// and that omission is the point rather than an oversight.
func TestPercentileMatchesCPythonIncludingSingleNaNPlacements(t *testing.T) {
	nan := math.NaN()

	// NaN-free: ordinary parity, unaffected by any of the above.
	for _, testCase := range []struct {
		name   string
		values []float64
		pct    float64
		want   float64
	}{
		{"p50 odd", []float64{1, 2, 3}, 50, 2},
		{"p50 even interpolates", []float64{1, 2, 3, 4}, 50, 2.5},
		{"p0 is the min", []float64{5, 1, 3}, 0, 1},
		{"p100 is the max", []float64{5, 1, 3}, 100, 5},
		{"single element", []float64{7}, 50, 7},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := Percentile(testCase.values, testCase.pct); got != testCase.want {
				t.Errorf("Percentile(%v, %v) = %v, want %v",
					testCase.values, testCase.pct, got, testCase.want)
			}
		})
	}

	// Single-NaN placements. Expectations are CPython's ACTUAL output, captured
	// from `sorted(...)` on the same input order -- not what a total order
	// would give, because CPython does not produce one here.
	for _, testCase := range []struct {
		name    string
		values  []float64
		ordered []float64 // what CPython's sorted() returns
	}{
		{"NaN in the middle", []float64{1, nan, 2}, []float64{1, nan, 2}},
		{"NaN first", []float64{nan, 1, 2}, []float64{nan, 1, 2}},
		{"NaN middle, descending ends", []float64{2, nan, 1}, []float64{2, nan, 1}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// p0 reads index 0 of the ordered slice, which is the position the
			// sort disagreement actually moves -- so it discriminates
			// sort.Float64s from CPython without depending on interpolation.
			got := Percentile(testCase.values, 0)
			want := testCase.ordered[0]
			if math.IsNaN(want) {
				if !math.IsNaN(got) {
					t.Errorf("Percentile(%v, 0) = %v, want NaN (CPython's sorted leaves NaN at index 0)",
						testCase.values, got)
				}
				return
			}
			if math.IsNaN(got) || got != want {
				t.Errorf(
					"Percentile(%v, 0) = %v, want %v -- CPython's sorted gives %v, so index 0 is %v; "+
						"a NaN here means the sort moved NaN to the front, which is sort.Float64s' "+
						"behaviour and not CPython's",
					testCase.values, got, want, testCase.ordered, want,
				)
			}
		})
	}
}
