package benchmarking

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

// failingFetcher is a SeriesFetcher that returns an error for any metric
// name in failOn, and a trivial single-point series (present, above zero,
// so downstream compute functions have something to work with) for every
// other metric.
type failingFetcher struct {
	failOn map[string]bool
	calls  int
}

func (f *failingFetcher) FetchMetricSeriesByScope(
	_ context.Context, metricName string, _, endDay time.Time, _ string,
) (map[string][]MetricPoint, error) {
	f.calls++
	if f.failOn[metricName] {
		return nil, errors.New("simulated fetch failure")
	}
	return map[string][]MetricPoint{
		"scope-a": {{Day: endDay, Value: 1.0}},
	}, nil
}

// TestComputeBenchmarkingForDayCountsSwallowedFetchFailures is the red/green
// proof for the forwarded finding (#2276 r2 F2, P1): a fetch failure must be
// COUNTED on Outputs.FetchFailures, not merely logged and silently
// discarded. RED (before the fix): FetchFailures does not exist / stays 0
// regardless of how many fetches fail. GREEN (this test, against the fixed
// code): a metric whose EVERY fetch fails increments the counter once per
// swallowed slice.
func TestComputeBenchmarkingForDayCountsSwallowedFetchFailures(t *testing.T) {
	fetcher := &failingFetcher{failOn: map[string]bool{"success_rate": true}}
	metrics := [][2]string{{"success_rate", ScopeRepo}}

	outputs, err := ComputeBenchmarkingForDay(
		context.Background(), fetcher, goldenAsOf(), goldenStamp(), goldenOrgID,
		metrics, nil, slog.Default(),
	)
	if err != nil {
		t.Fatalf("ComputeBenchmarkingForDay returned an error -- fetch failures must be swallowed, "+
			"matching Python's per-metric try/except, never propagated: %v", err)
	}

	// Every metric in the `metrics` loop is independently fetched for
	// baselines, anomalies, AND period-comparisons (period-comparisons'
	// "current" fetch, before it ever reaches "prior") -- three independent
	// fetch attempts for one metric, all failing here since they all fetch
	// "success_rate".
	if outputs.FetchFailures != 3 {
		t.Fatalf("FetchFailures = %d, want 3 (baselines + anomalies + period-comparison's "+
			"current-window fetch, all failing for the one metric in this fixture) -- a "+
			"swallowed fetch failure must be counted, not silently discarded", outputs.FetchFailures)
	}
	// Positive control: nothing for "success_rate" should have landed, since
	// every fetch for it failed.
	if len(outputs.Baselines) != 0 || len(outputs.Anomalies) != 0 {
		t.Errorf("got %d baseline(s) and %d anomal(y/ies) despite every fetch failing -- "+
			"the fixture's premise (all fetches for this metric fail) does not hold",
			len(outputs.Baselines), len(outputs.Anomalies))
	}
}

// TestComputeBenchmarkingForDayReportsZeroFetchFailuresOnSuccess is the
// negative control for the test above: a run where every fetch succeeds
// must report FetchFailures == 0, proving the counter does not fire
// spuriously on the happy path.
func TestComputeBenchmarkingForDayReportsZeroFetchFailuresOnSuccess(t *testing.T) {
	fetcher := &failingFetcher{failOn: map[string]bool{}}
	metrics := [][2]string{{"success_rate", ScopeRepo}}

	outputs, err := ComputeBenchmarkingForDay(
		context.Background(), fetcher, goldenAsOf(), goldenStamp(), goldenOrgID,
		metrics, nil, slog.Default(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outputs.FetchFailures != 0 {
		t.Fatalf("FetchFailures = %d, want 0 -- no fetch failed in this fixture, "+
			"the counter must not fire spuriously", outputs.FetchFailures)
	}
	if fetcher.calls == 0 {
		t.Fatal("the fake fetcher was never called -- this test's premise (fetches happen) does not hold")
	}
}

// TestComputeBenchmarkingForDayCountsCorrelationFetchFailureOnce proves the
// "at most 1 per pair" rule stated in Outputs.FetchFailures' own doc
// comment: a correlation pair's SECOND fetch is never attempted once the
// FIRST has already failed, so a failing pair contributes exactly 1, not 2.
func TestComputeBenchmarkingForDayCountsCorrelationFetchFailureOnce(t *testing.T) {
	fetcher := &failingFetcher{failOn: map[string]bool{"metric-a": true}}
	pairs := [][3]string{{"metric-a", "metric-b", ScopeRepo}}

	outputs, err := ComputeBenchmarkingForDay(
		context.Background(), fetcher, goldenAsOf(), goldenStamp(), goldenOrgID,
		nil, pairs, slog.Default(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outputs.FetchFailures != 1 {
		t.Fatalf("FetchFailures = %d, want exactly 1 -- the correlation pair's left fetch "+
			"('metric-a') fails and the function must return before attempting the right "+
			"fetch ('metric-b'), so this pair contributes 1, not 2", outputs.FetchFailures)
	}
	// The right fetch ('metric-b') must never have been attempted at all --
	// exactly ONE call (the failing left fetch), not two.
	if fetcher.calls != 1 {
		t.Fatalf("fetcher was called %d time(s), want exactly 1 -- the right fetch "+
			"('metric-b') must never be attempted once the left fetch has already failed",
			fetcher.calls)
	}
}
