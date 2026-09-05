package benchmarking

import (
	"fmt"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// DefaultCorrelationPairs mirrors correlations.py:16-20. Note the metric names
// here are ALIASES (`pipeline_success`, `defect_rate`) which the loader
// canonicalises -- but the RECORD keeps the alias, because Python passes
// `metric_name` through unchanged.
var DefaultCorrelationPairs = [][3]string{
	{"flake_rate", "cycle_time_hours", ScopeTeam},
	{"line_coverage_pct", "defect_rate", ScopeTeam},
	{"pipeline_success", "deployment_frequency", ScopeRepo},
}

// DefaultCorrelationMinPoints mirrors compute_metric_correlation's min_points.
const DefaultCorrelationMinPoints = 5

// ComputePeriodComparison ports compute_period_comparison
// (period_comparison.py:17-69). Returns false when either period is empty
// (Python returns None).
func ComputePeriodComparison(
	metricName string,
	scopeType string,
	scopeKey string,
	currentPeriodStart, currentPeriodEnd time.Time,
	comparisonPeriodStart, comparisonPeriodEnd time.Time,
	currentPoints, comparisonPoints []MetricPoint,
	computedAt time.Time,
	orgID string,
) (PeriodComparisonRecord, bool) {
	if len(currentPoints) == 0 || len(comparisonPoints) == 0 {
		return PeriodComparisonRecord{}, false
	}
	currentValues := make([]float64, len(currentPoints))
	for index, point := range currentPoints {
		currentValues[index] = point.Value
	}
	comparisonValues := make([]float64, len(comparisonPoints))
	for index, point := range comparisonPoints {
		comparisonValues[index] = point.Value
	}

	currentValue := Mean(currentValues)
	comparisonValue := Mean(comparisonValues)
	absoluteDelta := currentValue - comparisonValue

	var percentageChange *float64
	if !pythonparity.IsCloseAbs(comparisonValue, 0.0, 1e-9) {
		// `(absolute_delta / abs(comparison_value)) * 100.0` -- a divide then a
		// multiply, no add, so no FMA candidate here.
		change := (absoluteDelta / abs(comparisonValue)) * 100.0
		percentageChange = &change
	}

	trendDirection := "stable"
	if !pythonparity.IsCloseAbs(absoluteDelta, 0.0, 1e-9) {
		if MetricIsNegative(metricName) {
			if absoluteDelta < 0 {
				trendDirection = "improving"
			} else {
				trendDirection = "regressing"
			}
		} else {
			if absoluteDelta > 0 {
				trendDirection = "improving"
			} else {
				trendDirection = "regressing"
			}
		}
	}

	var roundedChange *float64
	if percentageChange != nil {
		rounded := round4(*percentageChange)
		roundedChange = &rounded
	}

	return PeriodComparisonRecord{
		MetricName:            metricName,
		ScopeType:             scopeType,
		ScopeKey:              scopeKey,
		CurrentPeriodStart:    currentPeriodStart,
		CurrentPeriodEnd:      currentPeriodEnd,
		ComparisonPeriodStart: comparisonPeriodStart,
		ComparisonPeriodEnd:   comparisonPeriodEnd,
		CurrentValue:          round4(currentValue),
		ComparisonValue:       round4(comparisonValue),
		AbsoluteDelta:         round4(absoluteDelta),
		PercentageChange:      roundedChange,
		TrendDirection:        trendDirection,
		ComputedAt:            computedAt,
		OrgID:                 orgID,
	}, true
}

// correlationInterpretation ports correlations.py:24-34. `direction` uses
// `r_value >= 0`, so an exact 0.0 reads as "positively".
func correlationInterpretation(metricName, pairedMetricName string, rValue float64) string {
	strength := "weakly"
	magnitude := abs(rValue)
	if magnitude >= 0.8 {
		strength = "strongly"
	} else if magnitude >= 0.5 {
		strength = "moderately"
	}
	direction := "negatively"
	if rValue >= 0 {
		direction = "positively"
	}
	return fmt.Sprintf(
		"%s appears %s and %s correlated with %s over this window.",
		metricName, strength, direction, pairedMetricName,
	)
}

// ComputeMetricCorrelation ports compute_metric_correlation
// (correlations.py:37-79), over the scopes present in BOTH series.
func ComputeMetricCorrelation(
	metricName string,
	pairedMetricName string,
	scopeType string,
	leftSeriesByScope map[string][]MetricPoint,
	rightSeriesByScope map[string][]MetricPoint,
	periodStart, periodEnd time.Time,
	computedAt time.Time,
	minPoints int,
	orgID string,
) []MetricCorrelationRecord {
	var scopeKeys []string
	for scopeKey := range leftSeriesByScope {
		if _, ok := rightSeriesByScope[scopeKey]; ok {
			scopeKeys = append(scopeKeys, scopeKey)
		}
	}
	sort.Strings(scopeKeys)

	var results []MetricCorrelationRecord
	for _, scopeKey := range scopeKeys {
		leftValues, rightValues, commonDays := AlignSeries(
			leftSeriesByScope[scopeKey], rightSeriesByScope[scopeKey],
		)
		if len(commonDays) < minPoints {
			continue
		}
		coefficient := PearsonCorrelation(leftValues, rightValues)
		pValue := FisherTwoTailedPValue(coefficient, len(commonDays))
		// is_significant reads the RAW p_value, BEFORE the round to 6
		// (correlations.py:60). That is why the golden corpus must keep every
		// case away from the 0.05 boundary -- see FisherTwoTailedPValue's doc
		// comment on the libm difference.
		isSignificant := abs(coefficient) > 0.5 && pValue < 0.05

		results = append(results, MetricCorrelationRecord{
			MetricName:       metricName,
			PairedMetricName: pairedMetricName,
			ScopeType:        scopeType,
			ScopeKey:         scopeKey,
			PeriodStart:      periodStart,
			PeriodEnd:        periodEnd,
			Coefficient:      round4(coefficient),
			PValue:           round6(pValue),
			SampleSize:       len(commonDays),
			IsSignificant:    isSignificant,
			Interpretation:   correlationInterpretation(metricName, pairedMetricName, coefficient),
			ComputedAt:       computedAt,
			OrgID:            orgID,
		})
	}
	return results
}
