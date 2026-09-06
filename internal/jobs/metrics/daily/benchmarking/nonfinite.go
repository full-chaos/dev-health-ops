package benchmarking

import (
	"fmt"
	"io"
	"math"
	"sync/atomic"
)

// nonFinitePercentileInputs counts percentile inputs containing a NaN or an
// infinity. Process-wide, like the writer counters.
//
// OBSERVABILITY ONLY -- it deliberately changes NO behaviour. Percentile over a
// series containing NaN is undefined in the sense that matters here: CPython's
// ordering is a Timsort artefact of a non-transitive comparator, not a
// specification, and no Go sort reproduces it in general (CHAOS-4288). The
// honest response is to make the input VISIBLE rather than to silently skip it,
// because a Go-only skip would be a NEW divergence from Python dressed up as a
// fix. The real repair belongs upstream on both planes and is ticketed.
//
// A metric that fires means: a percentile was taken over data that cannot have
// a well-defined answer on either plane. Nonzero here is a data-quality signal,
// not a Go bug.
var nonFinitePercentileInputs atomic.Uint64

func observeNonFinitePercentileInput(values []float64) {
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			nonFinitePercentileInputs.Add(1)
			return
		}
	}
}

// NonFinitePercentileInputMetrics reports the counter above.
type NonFinitePercentileInputMetrics struct{}

// NonFinitePercentileInputMetricsSource returns the process-wide source.
func NonFinitePercentileInputMetricsSource() *NonFinitePercentileInputMetrics {
	return &NonFinitePercentileInputMetrics{}
}

// WritePrometheus implements health.MetricsSource.
func (m *NonFinitePercentileInputMetrics) WritePrometheus(output io.Writer) error {
	if m == nil {
		return nil
	}
	if _, err := io.WriteString(output,
		"# HELP dev_health_benchmarking_nonfinite_percentile_inputs_total Percentile inputs containing NaN or an infinity, over which no percentile is well-defined on either plane (CHAOS-4288).\n"+
			"# TYPE dev_health_benchmarking_nonfinite_percentile_inputs_total counter\n"); err != nil {
		return err
	}
	_, err := fmt.Fprintf(output,
		"dev_health_benchmarking_nonfinite_percentile_inputs_total %d\n",
		nonFinitePercentileInputs.Load())
	return err
}
