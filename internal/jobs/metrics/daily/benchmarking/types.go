// Package benchmarking is the native Go port of the Python `benchmarking`
// daily metrics family: `run_benchmarking_for_day` (CHAOS-4288 deleted the
// Python source, formerly src/dev_health_ops/metrics/benchmarking/runner.py:259)
// and the five compute primitives plus insight generation beneath it.
//
// # It is ORG-scoped compute, and Python runs it once per REPO
//
// `run_benchmarking_for_day(sink, as_of_day, computed_at, org_id)` takes no
// repo_id at all, yet job_daily.py:2037 calls it inside run_daily_metrics_job's
// per-day loop, ABOVE the `if not skip_finalize:` gate. The compatibility
// bridge fans out one partition per repo, so an org with N repos recomputes the
// whole org's benchmarks N times and appends N identical row sets to six
// append-only tables every night. Readers do not deduplicate them.
//
// Team-lead ruled this FIXED rather than mirrored: the native executor computes
// for the org exactly ONCE per day, on the partition holding the org's
// lexicographically-first repository id, and no-ops on every other partition.
// See BenchmarkingExecutor's doc comment for the mechanism.
//
// # Write mode
//
// All six output tables are PLAIN MergeTree
// (migrations/clickhouse/031_testops_benchmarking.sql) with no version column,
// so every write appends. That is why the Python write-skip guard is
// load-bearing and why the once-per-org fix above matters.
//
// # Ledger correction
//
// families.json's `writes` list for this family omitted
// `testops_period_comparisons`, which write_benchmarking_outputs
// (runner.py:251-252) actively writes. CHAOS-4288's own "known ledger bug"
// (a non-existent `benchmarking_rollups`) was already fixed; this omission is
// the surviving defect and is corrected alongside the port.
package benchmarking

import "time"

// MetricPoint is one (day, value) sample of a metric for one scope.
type MetricPoint struct {
	Day   time.Time
	Value float64
}

// ScopeType values, matching the Python scope vocabulary exactly.
const (
	ScopeRepo   = "repo"
	ScopeTeam   = "team"
	ScopeGlobal = "global"
)

// MetricDefinition mirrors _common.py's MetricDefinition: where a metric's
// series comes from and how it is grouped before averaging.
type MetricDefinition struct {
	Table             string
	ValueColumn       string
	ScopeSupport      map[string]bool
	InnerGroupColumns []string
	// ExtraFilters are appended verbatim to the WHERE clause. Only
	// deployment_frequency uses one (it shares dora_metrics_daily with other
	// metrics and must select its own by name).
	ExtraFilters []string
}

// PeriodComparisonRecord mirrors testops_schemas.py:480-496.
type PeriodComparisonRecord struct {
	MetricName            string
	ScopeType             string
	ScopeKey              string
	CurrentPeriodStart    time.Time
	CurrentPeriodEnd      time.Time
	ComparisonPeriodStart time.Time
	ComparisonPeriodEnd   time.Time
	CurrentValue          float64
	ComparisonValue       float64
	AbsoluteDelta         float64
	// PercentageChange is nil when the comparison value is indistinguishable
	// from zero -- a real NULL in the table, not a zero.
	PercentageChange *float64
	TrendDirection   string
	ComputedAt       time.Time
	OrgID            string
}

// BenchmarkBaselineRecord mirrors testops_schemas.py:499-518.
type BenchmarkBaselineRecord struct {
	MetricName        string
	ScopeType         string
	ScopeKey          string
	PeriodStart       time.Time
	PeriodEnd         time.Time
	RollingWindowDays int
	CurrentValue      float64
	BaselineValue     float64
	PercentileRank    float64
	P25Value          float64
	P50Value          float64
	P75Value          float64
	P90Value          float64
	SampleSize        int
	ComputedAt        time.Time
	OrgID             string
}

// MaturityBandRecord mirrors testops_schemas.py:521-535.
type MaturityBandRecord struct {
	MetricName     string
	ScopeType      string
	ScopeKey       string
	PeriodStart    time.Time
	PeriodEnd      time.Time
	Value          float64
	PercentileRank float64
	MaturityBand   string
	Confidence     float64
	ComputedAt     time.Time
	OrgID          string
}

// BenchmarkAnomalyRecord mirrors testops_schemas.py:538-553.
type BenchmarkAnomalyRecord struct {
	MetricName      string
	ScopeType       string
	ScopeKey        string
	Day             time.Time
	Value           float64
	BaselineValue   float64
	ZScore          float64
	AnomalyType     string
	Direction       string
	Severity        string
	VolatilityScore float64
	ComputedAt      time.Time
	OrgID           string
}

// MetricCorrelationRecord mirrors testops_schemas.py:556-572.
type MetricCorrelationRecord struct {
	MetricName       string
	PairedMetricName string
	ScopeType        string
	ScopeKey         string
	PeriodStart      time.Time
	PeriodEnd        time.Time
	Coefficient      float64
	PValue           float64
	SampleSize       int
	IsSignificant    bool
	Interpretation   string
	ComputedAt       time.Time
	OrgID            string
}

// BenchmarkInsightRecord mirrors testops_schemas.py:575-590. PairedMetricName,
// PeriodStart and PeriodEnd are nullable in the Python dataclass and on the
// wire; only the correlation insight populates PairedMetricName.
type BenchmarkInsightRecord struct {
	InsightID        string
	InsightType      string
	ScopeType        string
	ScopeKey         string
	MetricName       string
	PairedMetricName *string
	PeriodStart      *time.Time
	PeriodEnd        *time.Time
	Severity         string
	Summary          string
	EvidenceJSON     string
	ComputedAt       time.Time
	OrgID            string
}

// Outputs is compute_benchmarking_for_day's return shape (runner.py:233-240),
// kept as one struct so the writer and the executor cannot disagree about
// which collections exist.
type Outputs struct {
	Baselines         []BenchmarkBaselineRecord
	MaturityBands     []MaturityBandRecord
	Anomalies         []BenchmarkAnomalyRecord
	PeriodComparisons []PeriodComparisonRecord
	Correlations      []MetricCorrelationRecord
	Insights          []BenchmarkInsightRecord
	// FetchFailures counts every SeriesFetcher.FetchMetricSeriesByScope call
	// ComputeBenchmarkingForDay swallowed (forwarded finding, #2276 r2 F2,
	// P1, verified by lane-ci-required-to-arc): Python's own per-metric
	// try/except semantics are preserved -- a fetch failure logs a warning
	// and contributes zero rows for that slice, the run continues -- but
	// until this field existed nothing counted HOW MANY slices were
	// affected, so a run silently degrading to fewer and fewer rows over
	// time (an intermittent ClickHouse issue, a metric whose source table
	// stopped being populated) was indistinguishable from "nothing to
	// report" at the result level. Incremented once per SWALLOWED slice
	// (baselines, anomalies, one period-comparison pair, one correlation
	// pair) -- a period-comparison or correlation pair's SECOND fetch is
	// never attempted once its FIRST has already failed (each is an early
	// `return` inside its own closure), so a pair contributes at most 1 to
	// this count, not 2, matching the single warn() line each failure path
	// actually logs.
	FetchFailures int
}

// Total is the row count across every collection -- what the executor reports
// as rowsWritten.
func (outputs Outputs) Total() int {
	return len(outputs.Baselines) + len(outputs.MaturityBands) + len(outputs.Anomalies) +
		len(outputs.PeriodComparisons) + len(outputs.Correlations) + len(outputs.Insights)
}
