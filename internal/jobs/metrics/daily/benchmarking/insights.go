package benchmarking

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// insightID ports _insight_id (__init__.py:33-34):
// str(uuid.uuid5(uuid.NAMESPACE_URL, ":".join(parts))).
//
// uuid5 is SHA-1 over the namespace bytes plus the name, which
// uuid.NewSHA1(uuid.NameSpaceURL, ...) reproduces exactly -- the established
// pattern in this repo (internal/scheduler/sync/source_discovery.go:1513-1515).
// These ids are the output table's ORDER BY key, so a divergence here does not
// merely relabel a row, it writes a different row.
func insightID(parts ...string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join(parts, ":"))).String()
}

// evidenceJSON reproduces `json.dumps({...})` for one insight's payload.
//
// # encoding/json IS BYTE-WRONG HERE, ON EVERY ROW
//
// CPython's json.dumps defaults to `separators=(', ', ': ')` -- a space after
// every comma and colon -- while Go's encoding/json emits none. That is not an
// edge case: every insight row's evidence_json would differ.
//
// # INSERTION ORDER, NOT SORTED
//
// `json.dumps(payload)` is called WITHOUT sort_keys (__init__.py:78,115,150),
// so CPython writes the dict in insertion order. pythonparity offers one
// encoder per Python call rather than a flag, so this must be
// MarshalPythonJSONInsertionOrder and NOT MarshalPythonJSON (which reproduces
// `sort_keys=True`). Picking the wrong one produces plausible bytes and a
// silent divergence -- the exact failure that package exists to prevent. The
// callers below therefore pass an OrderedObject in Python's own field order.
//
// # ints STAY ints
//
// `sample_size` is a Python int, so json.dumps writes `5`, not `5.0`. It is
// passed through as an int for that reason; the float fields are the already-
// rounded values off the source record, so this serialises exactly what Python
// serialises.
func evidenceJSON(fields pythonparity.OrderedObject) (string, error) {
	encoded, err := pythonparity.MarshalPythonJSONInsertionOrder(fields)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// nullableFloat renders a *float64 as either the value or a JSON null,
// matching `percentage_change`'s Optional[float] on the Python record.
func nullableFloat(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

// GenerateBenchmarkInsights ports generate_benchmark_insights
// (__init__.py:37-161). Order is comparisons, then anomalies, then
// correlations -- and within each, the input slice's order.
//
// Note it is NOT wrapped in the runner's per-metric try/except: an error here
// aborts the whole run AFTER the other five outputs are computed but BEFORE
// anything is written (runner.py:226 sits outside the try blocks and
// write_benchmarking_outputs runs later). That asymmetry is mirrored by
// returning an error rather than swallowing it.
func GenerateBenchmarkInsights(
	periodComparisons []PeriodComparisonRecord,
	anomalies []BenchmarkAnomalyRecord,
	correlations []MetricCorrelationRecord,
	computedAt time.Time,
) ([]BenchmarkInsightRecord, error) {
	var insights []BenchmarkInsightRecord

	for _, comparison := range periodComparisons {
		if comparison.TrendDirection == "stable" {
			continue
		}
		// Python: `if pct is not None and abs(pct) < 5.0: continue`. A NULL
		// percentage_change does NOT suppress the insight.
		if comparison.PercentageChange != nil && abs(*comparison.PercentageChange) < 5.0 {
			continue
		}
		// `f"{absolute_delta:.2f}"` -- Python's format spec, not Go's %v.
		delta, err := pythonparity.FormatFixed(comparison.AbsoluteDelta, 2)
		if err != nil {
			return nil, fmt.Errorf("format absolute_delta: %w", err)
		}
		summary := fmt.Sprintf(
			"%s appears %s versus the prior period, which suggests a %s shift over the selected window.",
			comparison.MetricName, comparison.TrendDirection, delta,
		)
		evidence, err := evidenceJSON(pythonparity.OrderedObject{
			{Key: "current_value", Value: comparison.CurrentValue},
			{Key: "comparison_value", Value: comparison.ComparisonValue},
			{Key: "absolute_delta", Value: comparison.AbsoluteDelta},
			{Key: "percentage_change", Value: nullableFloat(comparison.PercentageChange)},
		})
		if err != nil {
			return nil, err
		}
		severity := "info"
		if comparison.TrendDirection == "regressing" {
			severity = "warning"
		}
		periodStart := comparison.CurrentPeriodStart
		periodEnd := comparison.CurrentPeriodEnd
		insights = append(insights, BenchmarkInsightRecord{
			InsightID: insightID(
				"comparison", comparison.MetricName, comparison.ScopeType,
				comparison.ScopeKey, isoDate(comparison.CurrentPeriodEnd),
			),
			InsightType:  "comparison",
			ScopeType:    comparison.ScopeType,
			ScopeKey:     comparison.ScopeKey,
			MetricName:   comparison.MetricName,
			PeriodStart:  &periodStart,
			PeriodEnd:    &periodEnd,
			Severity:     severity,
			Summary:      summary,
			EvidenceJSON: evidence,
			ComputedAt:   computedAt,
			OrgID:        comparison.OrgID,
		})
	}

	for _, anomaly := range anomalies {
		summary := fmt.Sprintf(
			"%s appears to lean %s on %s, which suggests the observed value moved away from its rolling baseline.",
			anomaly.MetricName, anomaly.AnomalyType, isoDate(anomaly.Day),
		)
		evidence, err := evidenceJSON(pythonparity.OrderedObject{
			{Key: "value", Value: anomaly.Value},
			{Key: "baseline_value", Value: anomaly.BaselineValue},
			{Key: "z_score", Value: anomaly.ZScore},
			{Key: "volatility_score", Value: anomaly.VolatilityScore},
		})
		if err != nil {
			return nil, err
		}
		day := anomaly.Day
		periodStart, periodEnd := day, day
		insights = append(insights, BenchmarkInsightRecord{
			InsightID: insightID(
				"anomaly", anomaly.MetricName, anomaly.ScopeType,
				anomaly.ScopeKey, isoDate(anomaly.Day), anomaly.AnomalyType,
			),
			InsightType:  "anomaly",
			ScopeType:    anomaly.ScopeType,
			ScopeKey:     anomaly.ScopeKey,
			MetricName:   anomaly.MetricName,
			PeriodStart:  &periodStart,
			PeriodEnd:    &periodEnd,
			Severity:     anomaly.Severity,
			Summary:      summary,
			EvidenceJSON: evidence,
			ComputedAt:   computedAt,
			OrgID:        anomaly.OrgID,
		})
	}

	for _, correlation := range correlations {
		if !correlation.IsSignificant {
			continue
		}
		evidence, err := evidenceJSON(pythonparity.OrderedObject{
			{Key: "coefficient", Value: correlation.Coefficient},
			{Key: "p_value", Value: correlation.PValue},
			// int, not float: json.dumps writes 5, never 5.0.
			{Key: "sample_size", Value: correlation.SampleSize},
		})
		if err != nil {
			return nil, err
		}
		paired := correlation.PairedMetricName
		periodStart := correlation.PeriodStart
		periodEnd := correlation.PeriodEnd
		insights = append(insights, BenchmarkInsightRecord{
			InsightID: insightID(
				"correlation", correlation.MetricName, correlation.PairedMetricName,
				correlation.ScopeType, correlation.ScopeKey, isoDate(correlation.PeriodEnd),
			),
			InsightType:      "correlation",
			ScopeType:        correlation.ScopeType,
			ScopeKey:         correlation.ScopeKey,
			MetricName:       correlation.MetricName,
			PairedMetricName: &paired,
			PeriodStart:      &periodStart,
			PeriodEnd:        &periodEnd,
			Severity:         "info",
			Summary:          correlation.Interpretation,
			EvidenceJSON:     evidence,
			ComputedAt:       computedAt,
			OrgID:            correlation.OrgID,
		})
	}

	return insights, nil
}

// isoDate renders a date the way Python's date.isoformat() does.
func isoDate(day time.Time) string { return day.Format("2006-01-02") }
