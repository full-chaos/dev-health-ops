package analytics

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// The tile reads the shared pooled definition (testopspooled), the one the AI report charts read: the source and the
// expression of every pooled measure are the package's.
func TestPooledMeasuresReadTheSharedDefinition(t *testing.T) {
	for measure, want := range map[Measure]string{
		MeasurePipelineDurationP95:  testopspooled.PipelineRunsSource,
		MeasurePipelineQueueTime:    testopspooled.PipelineRunsSource,
		MeasureTestSuiteDurationP95: testopspooled.SuiteRunsSource,
	} {
		source, _, ok := pooledSource(measure)
		if !ok || source != want {
			t.Errorf("%s: source is not the shared package's", measure)
		}
	}
	for measure, want := range map[Measure]string{
		MeasurePipelineDurationP95:  testopspooled.P95,
		MeasureTestSuiteDurationP95: testopspooled.P95,
		MeasurePipelineQueueTime:    testopspooled.QueueMean,
	} {
		value, _, ok := orgPooledTerms(measure)
		if !ok || value != want {
			t.Errorf("%s: expression is not the shared package's", measure)
		}
	}
	if TestopsPooledMaxRowsToRead != testopspooled.MaxRowsToRead {
		t.Errorf("the row bound is %d, the shared default %d", TestopsPooledMaxRowsToRead, testopspooled.MaxRowsToRead)
	}
}
