package investment

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// ShadowPhaseCounts is what one shadow phase reports to the metrics. Every
// field is a count or a member of a closed set: no org id, no work unit id and
// no free text.
type ShadowPhaseCounts struct {
	// Model is the requested (configured) model id.
	Model string
	// StopReason is one of ShadowStopReasons.
	StopReason string
	// AttemptsByState counts the HTTP attempts: the last attempt of a
	// classification under its state, every earlier one under
	// ShadowAttemptRetried.
	AttemptsByState    map[string]int
	AttemptLatencies   []time.Duration
	PanicsRecovered    int
	AttemptWriteErrors int
	AttemptRowsDropped int
}

// ShadowObserver receives the counts of one shadow phase. A narrow interface,
// same pattern as RepoAttributionObserver.
type ShadowObserver interface {
	ObserveShadowPhase(ShadowPhaseCounts)
}

// CollectorShadowObserver adapts the metrics collector to ShadowObserver.
type CollectorShadowObserver struct {
	Collector *jobruntime.MetricsCollector
}

func (observer CollectorShadowObserver) ObserveShadowPhase(counts ShadowPhaseCounts) {
	if observer.Collector == nil {
		return
	}
	_ = observer.Collector.ObserveInvestmentShadowPhase(jobruntime.InvestmentShadowPhase{
		Model: counts.Model, StopReason: counts.StopReason,
		AttemptsByState: counts.AttemptsByState, AttemptLatencies: counts.AttemptLatencies,
		PanicsRecovered: counts.PanicsRecovered, AttemptWriteErrors: counts.AttemptWriteErrors,
		AttemptRowsDropped: counts.AttemptRowsDropped,
	})
}
