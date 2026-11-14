package analytics

import (
	"strings"
	"testing"
)

// The rate measures of the TestOps daily tables are summed numerators over summed denominators, each with a NULLIF so a
// group with nothing to divide by is NULL, and none is an AVG of stored rates (CHAOS-13775).
func TestRateMeasuresAreWeightedAndNeverAnAverageOfStoredRates(t *testing.T) {
	for measure, want := range map[Measure][]string{
		MeasurePipelineSuccessRate: {"SUM(success_count)", "NULLIF(SUM(pipelines_count), 0)"},
		MeasurePipelineFailureRate: {"SUM(failure_count)", "NULLIF(SUM(pipelines_count), 0)"},
		MeasurePipelineRerunRate:   {"SUM(rerun_rate * pipelines_count)", "NULLIF(SUM(pipelines_count), 0)"},
		MeasureTestPassRate:        {"SUM(passed_count)", "NULLIF(SUM(passed_count) + SUM(failed_count), 0)"},
		MeasureTestFailureRate:     {"SUM(failed_count)", "NULLIF(SUM(passed_count) + SUM(failed_count), 0)"},
		MeasureTestFlakeRate:       {"countIf(flake_cases IS NULL OR distinct_cases IS NULL) > 0, NULL", "NULLIF(SUM(distinct_cases), 0)"},
		MeasureCoverageLinePct:     {"SUM(if(lines_covered IS NOT NULL AND lines_total IS NOT NULL AND lines_total > 0, lines_covered, 0))", "NULLIF(SUM(if(lines_covered IS NOT NULL AND lines_total IS NOT NULL AND lines_total > 0, lines_total, 0)), 0)"},
	} {
		expr, err := dbExpression(measure, false, false)
		if err != nil {
			t.Fatalf("%s: %v", measure, err)
		}
		if strings.Contains(expr, "AVG(") || strings.Contains(expr, "_rate)") && !strings.Contains(expr, "rerun_rate *") {
			t.Errorf("%s: %q reads a stored rate or a mean", measure, expr)
		}
		if !strings.HasSuffix(expr, " * 100") {
			t.Errorf("%s: %q is not a percent", measure, expr)
		}
		for _, w := range want {
			if !strings.Contains(expr, w) {
				t.Errorf("%s: %q lacks %q", measure, expr, w)
			}
		}
	}
}

// What has no stored denominator stays a mean, and is named so.
func TestMeasuresWithoutADenominatorStayAnAverage(t *testing.T) {
	for _, measure := range []Measure{MeasurePipelineDurationP95, MeasurePipelineQueueTime, MeasureTestSuiteDurationP95, MeasureCoverageBranchPct, MeasureCoverageDeltaPct} {
		expr, err := dbExpression(measure, false, false)
		if err != nil || !strings.HasPrefix(expr, "AVG(") {
			t.Errorf("%s: %q err %v, want an AVG", measure, expr, err)
		}
	}
}

// The ORG series and the dimension series of a pass or failure rate are the same terms.
func TestOrgPassAndFailureRateUseTheSameTermsAsTheDimensionSeries(t *testing.T) {
	for measure, terms := range map[Measure]rateTerms{MeasureTestPassRate: testPassTerms, MeasureTestFailureRate: testFailureTerms} {
		num, den, scale, ok := orgRateTerms(measure)
		if !ok || num != terms.numerator || den != terms.denominator || scale != 100 {
			t.Errorf("%s: org terms %q / %q x%v ok=%v, want %q / %q x100", measure, num, den, scale, ok, terms.numerator, terms.denominator)
		}
	}
	for _, measure := range []Measure{MeasurePipelineFailureRate, MeasurePipelineRerunRate} {
		if _, _, _, ok := orgRateTerms(measure); ok {
			t.Errorf("%s has an ORG series: the web pipeline query would average it in as one more team", measure)
		}
	}
}
