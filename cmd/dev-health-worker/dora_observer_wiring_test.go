package main

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

// TestTheRealCollectorSatisfiesTheDORAObserver guards the type assertion in
// daily.go, which is the whole of the DORA telemetry wiring:
//
//	if candidate, ok := observer.(remaining.DORAObserver); ok { ... }
//
// A failed assertion there is SILENT. doraObserver stays nil, the executor
// skips its observation, every counter reads zero forever, and nothing --
// no build error, no test, no log line -- says why. That is the same
// unfalsifiable shape the executor's fail-closed construction exists to avoid,
// arriving through the back door.
//
// Renaming ObserveDORAPartition, changing its signature, or moving it to a type
// that is not what the worker passes as its observer all compile cleanly and
// all break the wiring. This test is what turns any of those into a failure.
func TestTheRealCollectorSatisfiesTheDORAObserver(t *testing.T) {
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	// Asserted through jobruntime.Observer, the STATIC type daily.go holds --
	// not through the concrete type. Asserting on *MetricsCollector directly
	// would pass even if the worker were handed something else entirely, which
	// is precisely the case that breaks telemetry in production.
	var observer jobruntime.Observer = collector
	candidate, ok := observer.(remaining.DORAObserver)
	if !ok {
		t.Fatal(
			"the worker's observer does NOT satisfy remaining.DORAObserver, so " +
				"daily.go silently passes a nil observer and every native DORA " +
				"counter stays at zero with nothing to indicate why",
		)
	}
	if err := candidate.ObserveDORAPartition(1, 1, 0); err != nil {
		t.Fatalf("the wired observer must accept a valid observation: %v", err)
	}
}
