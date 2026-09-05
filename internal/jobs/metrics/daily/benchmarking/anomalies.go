package benchmarking

import (
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Anomaly detection defaults, mirroring detect_metric_anomalies' signature
// (anomalies.py:36-46).
const (
	DefaultRollingWindowDays = 30
	DefaultZThreshold        = 2.0
	DefaultVolatilityThresh  = 0.5
	DefaultMinHistoryPoints  = 5
)

// severityFromZScore ports anomalies.py:18-24. Boundaries are inclusive, so
// |z| exactly 3.0 is "critical" and exactly 2.0 is "warning".
func severityFromZScore(zScore float64) string {
	magnitude := abs(zScore)
	if magnitude >= 3.0 {
		return "critical"
	}
	if magnitude >= 2.0 {
		return "warning"
	}
	return "info"
}

// anomalyDirection ports anomalies.py:27-33.
//
// The isclose here is a ZERO comparison, which is provably equivalent to a
// plain |delta| <= 1e-9 at every magnitude -- pythonparity.IsCloseAbs is used
// for uniformity across the family, so a future edit that swaps the 0.0 for a
// real value cannot silently reintroduce the rel_tol bug.
func anomalyDirection(metricName string, delta float64) string {
	if pythonparity.IsCloseAbs(delta, 0.0, 1e-9) {
		return "stable"
	}
	negative := MetricIsNegative(metricName)
	improving := delta > 0
	if negative {
		improving = delta < 0
	}
	if improving {
		if negative {
			return "down"
		}
		return "up"
	}
	if negative {
		return "up"
	}
	return "down"
}

// DetectMetricAnomalies ports detect_metric_anomalies (anomalies.py:36-118).
//
// A scope can emit TWO rows for the same day: one z-score anomaly and one
// volatility anomaly. They are distinct `anomaly_type` values and the output
// table's ORDER BY includes anomaly_type, so both persist -- this is not a
// duplicate.
func DetectMetricAnomalies(
	metricName string,
	scopeType string,
	seriesByScope map[string][]MetricPoint,
	asOfDay time.Time,
	computedAt time.Time,
	rollingWindowDays int,
	zThreshold float64,
	volatilityThreshold float64,
	minHistoryPoints int,
	orgID string,
) []BenchmarkAnomalyRecord {
	historyStart := asOfDay.AddDate(0, 0, -rollingWindowDays)

	scopeKeys := make([]string, 0, len(seriesByScope))
	for scopeKey := range seriesByScope {
		scopeKeys = append(scopeKeys, scopeKey)
	}
	sort.Strings(scopeKeys)

	var results []BenchmarkAnomalyRecord
	for _, scopeKey := range scopeKeys {
		points := seriesByScope[scopeKey]
		ordered := make([]MetricPoint, len(points))
		copy(ordered, points)
		// Python sorts by day with a stable sort; sort.SliceStable keeps the
		// relative order of same-day points, which decides which one `next(...
		// reversed(...))` picks below.
		sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Day.Before(ordered[j].Day) })

		// `next((p for p in reversed(ordered) if p.day <= as_of), None)` --
		// the LAST point at or before as_of.
		var current MetricPoint
		found := false
		for index := len(ordered) - 1; index >= 0; index-- {
			if !ordered[index].Day.After(asOfDay) {
				current = ordered[index]
				found = true
				break
			}
		}
		if !found {
			continue
		}

		var history []float64
		for _, point := range ordered {
			if !point.Day.Before(historyStart) && point.Day.Before(current.Day) {
				history = append(history, point.Value)
			}
		}
		if len(history) < minHistoryPoints {
			continue
		}

		baselineValue := Mean(history)
		stdev := PopulationStdev(history)

		var zScore float64
		if pythonparity.IsCloseAbs(stdev, 0.0, 1e-12) {
			// THE REAL-VALUE isclose SITE (anomalies.py:72). Both operands are
			// metric values, routinely far greater than 1, so CPython's
			// DEFAULT rel_tol=1e-9 is active and a naive
			// math.Abs(a-b) <= 1e-9 diverges here. This branch decides 0.0 vs
			// 3.0, which selects "info" vs "critical" and whether the row is
			// emitted at all -- an alerting-level difference.
			if pythonparity.IsCloseAbs(current.Value, baselineValue, 1e-9) {
				zScore = 0.0
			} else {
				zScore = 3.0
			}
		} else {
			zScore = (current.Value - baselineValue) / stdev
		}

		delta := current.Value - baselineValue
		volatilityScore := 0.0
		denominator := abs(baselineValue)
		if !pythonparity.IsCloseAbs(denominator, 0.0, 1e-12) {
			volatilityScore = stdev / denominator
		}

		if abs(zScore) >= zThreshold {
			negative := MetricIsNegative(metricName)
			improving := delta > 0
			if negative {
				improving = delta < 0
			}
			anomalyType := "regression"
			if improving {
				anomalyType = "improvement"
			}
			results = append(results, BenchmarkAnomalyRecord{
				MetricName:      metricName,
				ScopeType:       scopeType,
				ScopeKey:        scopeKey,
				Day:             current.Day,
				Value:           round4(current.Value),
				BaselineValue:   round4(baselineValue),
				ZScore:          round4(zScore),
				AnomalyType:     anomalyType,
				Direction:       anomalyDirection(metricName, delta),
				Severity:        severityFromZScore(zScore),
				VolatilityScore: round4(volatilityScore),
				ComputedAt:      computedAt,
				OrgID:           orgID,
			})
		}

		if volatilityScore >= volatilityThreshold {
			severity := "critical"
			if volatilityScore < 1.0 {
				severity = "warning"
			}
			results = append(results, BenchmarkAnomalyRecord{
				MetricName:      metricName,
				ScopeType:       scopeType,
				ScopeKey:        scopeKey,
				Day:             current.Day,
				Value:           round4(current.Value),
				BaselineValue:   round4(baselineValue),
				ZScore:          round4(zScore),
				AnomalyType:     "volatility",
				Direction:       "volatile",
				Severity:        severity,
				VolatilityScore: round4(volatilityScore),
				ComputedAt:      computedAt,
				OrgID:           orgID,
			})
		}
	}
	return results
}
