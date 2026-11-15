package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// flake_rate, retry_dependency_rate and failure_recurrence_score are rates over
// the day's measured cases. A day that measured no case (its case rows are all
// of runs left out of the flake measures, or it has no case row at all) writes
// NULL in the three: a rate over 0 cases is unknown, not 0. A day that measured
// cases writes its rates, a measured 0 included.
func TestWriteTestopsTestMetricsWritesNullRatesForADayWithNoMeasuredCase(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	const flakeRate, retryRate, recurrence = 9, 10, 14
	for name, test := range map[string]struct {
		metric testops.TestMetric
		want   *[3]float64
	}{
		"unknown: case rows of left-out runs only": {
			metric: testops.TestMetric{TotalCases: 12, FlakeUnknown: true},
		},
		"no case row, no suite case": {
			metric: testops.TestMetric{TotalCases: 0},
		},
		"suites but no case row": {
			metric: testops.TestMetric{TotalCases: 5},
		},
		"measured": {
			metric: testops.TestMetric{TotalCases: 12, FlakeCases: 2, DistinctCases: 8,
				FlakeRate: 0.25, RetryDependencyRate: 0.125, FailureRecurrence: 0.5},
			want: &[3]float64{0.25, 0.125, 0.5},
		},
		"measured, nothing flaked": {
			metric: testops.TestMetric{TotalCases: 12, DistinctCases: 8},
			want:   &[3]float64{0, 0, 0},
		},
		"case rows without a suite": {
			metric: testops.TestMetric{TotalCases: 0, FlakeCases: 1, DistinctCases: 4,
				FlakeRate: 0.25, RetryDependencyRate: 0.5, FailureRecurrence: 1},
			want: &[3]float64{0.25, 0.5, 1},
		},
	} {
		conn := &queryRecordingBatchConn{batch: &recordingBatch{}}
		metric := test.metric
		metric.RepoID = uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
		metric.OrgID = "org-1"
		if _, err := writeTestopsTestMetrics(context.Background(), conn, "org-1", day, day, []testops.TestMetric{metric}); err != nil {
			t.Fatal(err)
		}
		row := conn.batch.appended[0]
		for index, column := range []int{flakeRate, retryRate, recurrence} {
			got, ok := row[column].(*float64)
			if !ok {
				t.Errorf("%s: column %d appended as %T, want *float64 (NULL when no case was measured)", name, column, row[column])
				continue
			}
			switch {
			case test.want == nil && got != nil:
				t.Errorf("%s: column %d = %v, want NULL", name, column, *got)
			case test.want != nil && got == nil:
				t.Errorf("%s: column %d = NULL, want %v", name, column, test.want[index])
			case test.want != nil && *got != test.want[index]:
				t.Errorf("%s: column %d = %v, want %v", name, column, *got, test.want[index])
			}
		}
	}
}
