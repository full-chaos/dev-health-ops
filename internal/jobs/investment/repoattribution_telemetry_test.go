package investment

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// TestMetricsCollectorSatisfiesTheRepoAttributionObserver is the wiring proof
// for CHAOS-5459's narrow-capability assertion in
// cmd/dev-health-worker/workgraph.go: the production observer IS a
// *jobruntime.MetricsCollector, and the assertion there is a runtime type
// switch that fails SILENTLY (no metric, no error) if this interface ever
// stops matching. This test turns that silent failure into a compile/test
// failure in the package that defines the interface.
func TestMetricsCollectorSatisfiesTheRepoAttributionObserver(t *testing.T) {
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	var observer RepoAttributionObserver = collector
	if observer == nil {
		t.Fatal("collector did not satisfy RepoAttributionObserver")
	}
}

// TestEveryRepoAttributionSourceIsRegisteredWithTheCollector keeps this
// package's tier names and jobruntime's closed label set in step.
//
// They CANNOT share a symbol: jobruntime is the lower layer and this package
// imports it, so the constants live here and the vocabulary lives there. A
// rename on either side would otherwise degrade silently -- every
// ObserveInvestmentRepoAttribution call would return "not registered", which
// observeRepoAttribution logs at Warn and swallows by design, so the series
// would just stop moving with nothing failing.
func TestEveryRepoAttributionSourceIsRegisteredWithTheCollector(t *testing.T) {
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		RepoAttributionSourceOwnEdges,
		RepoAttributionSourceAncestor,
		RepoAttributionSourceChildren,
		RepoAttributionSourceTeamOwnership,
		RepoAttributionSourceUnassigned,
	} {
		if err := collector.ObserveInvestmentRepoAttribution(source, 0); err != nil {
			t.Errorf("source %q is not registered with the metrics collector: %v", source, err)
		}
	}
	// Negative control: the guard above is only meaningful if the collector
	// can actually reject something.
	if err := collector.ObserveInvestmentRepoAttribution("definitely-not-a-tier", 0); err == nil {
		t.Error("the collector accepted an unregistered source, so the loop above proves nothing")
	}
}

// recordingObserver captures what observeRepoAttribution emits.
type recordingObserver struct {
	seen map[string]int
	err  error
}

func (r *recordingObserver) ObserveInvestmentRepoAttribution(source string, count int) error {
	if r.seen == nil {
		r.seen = map[string]int{}
	}
	r.seen[source] = count
	return r.err
}

// TestObserveRepoAttributionReportsEveryTierIncludingZero: a tier that fired
// zero times must still be reported, or a dashboard cannot distinguish "the
// team tier resolved nothing this run" from "the team tier was removed".
func TestObserveRepoAttributionReportsEveryTierIncludingZero(t *testing.T) {
	observer := &recordingObserver{}
	m := &Materializer{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), observer: observer}

	m.observeRepoAttribution(context.Background(), Config{OrgID: "org", RunID: "run"}, Stats{
		Components:               10,
		RepoCascadeOwn:           4,
		RepoCascadeAncestor:      1,
		RepoCascadeChildren:      0,
		RepoCascadeTeamOwnership: 3,
		RepoCascadeUnassigned:    2,
	})

	want := map[string]int{
		RepoAttributionSourceOwnEdges:      4,
		RepoAttributionSourceAncestor:      1,
		RepoAttributionSourceChildren:      0,
		RepoAttributionSourceTeamOwnership: 3,
		RepoAttributionSourceUnassigned:    2,
	}
	if len(observer.seen) != len(want) {
		t.Fatalf("reported %d sources, want %d: %v", len(observer.seen), len(want), observer.seen)
	}
	for source, count := range want {
		if got := observer.seen[source]; got != count {
			t.Errorf("source %q reported %d, want %d", source, got, count)
		}
	}
}

// TestObserveRepoAttributionSurvivesATelemetryFault: a rejected metric must
// never fail a materialization that otherwise succeeded. observeRepoAttribution
// returns nothing, so the proof is that it completes and still attempts every
// source rather than stopping at the first error.
func TestObserveRepoAttributionSurvivesATelemetryFault(t *testing.T) {
	observer := &recordingObserver{err: errors.New("registry mismatch")}
	m := &Materializer{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), observer: observer}

	m.observeRepoAttribution(context.Background(), Config{OrgID: "org", RunID: "run"}, Stats{
		Components: 1, RepoCascadeOwn: 1,
	})

	if len(observer.seen) != 5 {
		t.Errorf("attempted %d sources after a rejection, want all 5: %v", len(observer.seen), observer.seen)
	}
}

// TestObserveRepoAttributionWithoutAnObserverStillLogs pins that the metric
// sink is OPTIONAL: a materializer with no observer must not panic, so a
// process that cannot supply a collector can still materialize (and is still
// observable through the log line).
func TestObserveRepoAttributionWithoutAnObserverStillLogs(t *testing.T) {
	m := &Materializer{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	m.observeRepoAttribution(context.Background(), Config{OrgID: "org", RunID: "run"}, Stats{Components: 1})
}
