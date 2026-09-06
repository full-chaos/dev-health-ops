package benchmarking

import (
	"context"
	"log/slog"
	"sort"
	"time"
)

// DefaultBenchmarkMetrics mirrors runner.py:48-59. ORDER IS LOAD-BEARING: it
// determines the emission order of every output collection, which the frozen
// golden compares positionally.
var DefaultBenchmarkMetrics = [][2]string{
	{"success_rate", ScopeRepo},
	{"failure_rate", ScopeRepo},
	{"p95_duration_seconds", ScopeRepo},
	{"rerun_rate", ScopeTeam},
	{"pass_rate", ScopeRepo},
	{"flake_rate", ScopeTeam},
	{"failure_recurrence_score", ScopeTeam},
	{"line_coverage_pct", ScopeRepo},
	{"branch_coverage_pct", ScopeRepo},
	{"coverage_delta_pct", ScopeRepo},
}

// Window constants, mirroring runner.py:62-66.
const (
	PeriodComparisonCurrentDays = 7
	PeriodComparisonPriorDays   = 7
	CorrelationWindowDays       = 30
)

// SeriesFetcher is the one I/O capability the compute needs: a metric's series
// for a window, keyed by scope.
type SeriesFetcher interface {
	FetchMetricSeriesByScope(
		ctx context.Context, metricName string, startDay, endDay time.Time, scopeType string,
	) (map[string][]MetricPoint, error)
}

// ComputeBenchmarkingForDay ports compute_benchmarking_for_day
// (runner.py:119-240).
//
// # THE ERROR SWALLOWING IS PART OF THE CONTRACT
//
// Python wraps each metric in THREE independent try/except blocks -- baselines
// (+maturity bands, which are derived from them and so vanish together),
// anomalies, and period comparisons -- and each correlation pair in a fourth.
// A failure logs a warning and contributes ZERO rows for that slice; the run
// continues and still WRITES whatever else succeeded. That is mirrored exactly:
// a fetch error here is recorded and skipped, never propagated.
//
// Insight generation is deliberately OUTSIDE that pattern (runner.py:226): an
// error there aborts the run, and does so BEFORE anything is written, since
// write_benchmarking_outputs runs after this function returns. So it returns an
// error rather than swallowing it.
func ComputeBenchmarkingForDay(
	ctx context.Context,
	fetcher SeriesFetcher,
	asOfDay time.Time,
	computedAt time.Time,
	orgID string,
	metrics [][2]string,
	correlationPairs [][3]string,
	logger *slog.Logger,
) (Outputs, error) {
	var outputs Outputs

	// warnFetchFailure is the SINGLE choke point every swallowed fetch
	// failure in this function goes through (forwarded finding, #2276 r2
	// F2, P1): it counts the failure on outputs.FetchFailures (see that
	// field's own doc comment) AND logs it with org_id/day identifiers
	// alongside whatever call-site-specific fields (metric, scope, paired
	// metric) the caller supplies -- centralizing this here means every
	// call site gets both effects automatically, with no risk of one site
	// remembering the counter increment and another forgetting it.
	warnFetchFailure := func(message string, args ...any) {
		outputs.FetchFailures++
		if logger != nil {
			fields := append([]any{
				"org_id", orgID, "day", asOfDay.Format("2006-01-02"),
			}, args...)
			logger.Warn(message, fields...)
		}
	}

	for _, entry := range metrics {
		metricName, scopeType := entry[0], entry[1]

		// --- baselines + maturity bands (runner.py:142-161) ---
		func() {
			maxWindow := 30
			for _, window := range DefaultBaselineWindows {
				if window > maxWindow {
					maxWindow = window
				}
			}
			startDay := asOfDay.AddDate(0, 0, -(maxWindow - 1))
			series, err := fetcher.FetchMetricSeriesByScope(ctx, metricName, startDay, asOfDay, scopeType)
			if err != nil {
				warnFetchFailure("benchmark baselines failed",
					"metric", metricName, "scope", scopeType, "error", err)
				return
			}
			baselines := ComputeInternalBaselines(
				metricName, scopeType, series, asOfDay, computedAt, DefaultBaselineWindows, orgID,
			)
			outputs.Baselines = append(outputs.Baselines, baselines...)
			outputs.MaturityBands = append(outputs.MaturityBands, ClassifyMaturityBands(baselines)...)
		}()

		// --- anomalies (runner.py:163-180) ---
		func() {
			startDay := asOfDay.AddDate(0, 0, -(DefaultRollingWindowDays + DefaultMinHistoryPoints))
			series, err := fetcher.FetchMetricSeriesByScope(ctx, metricName, startDay, asOfDay, scopeType)
			if err != nil {
				warnFetchFailure("benchmark anomalies failed",
					"metric", metricName, "scope", scopeType, "error", err)
				return
			}
			outputs.Anomalies = append(outputs.Anomalies, DetectMetricAnomalies(
				metricName, scopeType, series, asOfDay, computedAt,
				DefaultRollingWindowDays, DefaultZThreshold,
				DefaultVolatilityThresh, DefaultMinHistoryPoints, orgID,
			)...)
		}()

		// --- period comparisons (runner.py:182-199, via _build_period_comparisons) ---
		func() {
			currentEnd := asOfDay
			currentStart := asOfDay.AddDate(0, 0, -(PeriodComparisonCurrentDays - 1))
			priorEnd := currentStart.AddDate(0, 0, -1)
			priorStart := priorEnd.AddDate(0, 0, -(PeriodComparisonPriorDays - 1))

			currentSeries, err := fetcher.FetchMetricSeriesByScope(ctx, metricName, currentStart, currentEnd, scopeType)
			if err != nil {
				warnFetchFailure("period comparison failed",
					"metric", metricName, "scope", scopeType, "error", err)
				return
			}
			priorSeries, err := fetcher.FetchMetricSeriesByScope(ctx, metricName, priorStart, priorEnd, scopeType)
			if err != nil {
				warnFetchFailure("period comparison failed",
					"metric", metricName, "scope", scopeType, "error", err)
				return
			}
			// `sorted(set(current) & set(prior))` (runner.py:100).
			for _, scopeKey := range sortedIntersection(currentSeries, priorSeries) {
				record, ok := ComputePeriodComparison(
					metricName, scopeType, scopeKey,
					currentStart, currentEnd, priorStart, priorEnd,
					currentSeries[scopeKey], priorSeries[scopeKey],
					computedAt, orgID,
				)
				if ok {
					outputs.PeriodComparisons = append(outputs.PeriodComparisons, record)
				}
			}
		}()
	}

	// --- correlations (runner.py:201-224) ---
	corrEnd := asOfDay
	corrStart := asOfDay.AddDate(0, 0, -(CorrelationWindowDays - 1))
	for _, pair := range correlationPairs {
		metricName, pairedMetricName, scopeType := pair[0], pair[1], pair[2]
		func() {
			left, err := fetcher.FetchMetricSeriesByScope(ctx, metricName, corrStart, corrEnd, scopeType)
			if err != nil {
				warnFetchFailure("correlation failed",
					"metric", metricName, "paired", pairedMetricName, "scope", scopeType, "error", err)
				return
			}
			right, err := fetcher.FetchMetricSeriesByScope(ctx, pairedMetricName, corrStart, corrEnd, scopeType)
			if err != nil {
				warnFetchFailure("correlation failed",
					"metric", metricName, "paired", pairedMetricName, "scope", scopeType, "error", err)
				return
			}
			outputs.Correlations = append(outputs.Correlations, ComputeMetricCorrelation(
				metricName, pairedMetricName, scopeType, left, right,
				corrStart, corrEnd, computedAt, DefaultCorrelationMinPoints, orgID,
			)...)
		}()
	}

	insights, err := GenerateBenchmarkInsights(
		outputs.PeriodComparisons, outputs.Anomalies, outputs.Correlations, computedAt,
	)
	if err != nil {
		return Outputs{}, err
	}
	outputs.Insights = insights
	return outputs, nil
}

// sortedIntersection returns the scope keys present in both maps, ascending.
func sortedIntersection(left, right map[string][]MetricPoint) []string {
	var keys []string
	for key := range left {
		if _, ok := right[key]; ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}
