package synchandoff

import (
	"strings"
	"testing"
	"time"
)

func scrapeAwait(t *testing.T, m *AwaitMetrics) string {
	t.Helper()
	var b strings.Builder
	if err := m.WritePrometheus(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func newAwaitMetrics() *AwaitMetrics {
	return &AwaitMetrics{count: map[string]uint64{}, sum: map[string]float64{}, buckets: map[string][]uint64{}}
}

// Every Wait state maps to the Python outcome label: a disabled PagerDuty sync
// (StateTerminal) is a completed occurrence there.
func TestAwaitOutcomeLabelMatchesPython(t *testing.T) {
	for state, want := range map[State]string{
		StateMaterialized: "materialized", StateTerminal: "materialized",
		StateQuarantined: "quarantined", StatePending: "pending",
	} {
		if got := awaitOutcomeLabel(state); got != want {
			t.Errorf("state %d -> %q, want %q", state, got, want)
		}
	}
}

// The exposition is prometheus_client's: cumulative buckets with the Python le
// text, _sum and _count per outcome, the counter under its _total name, and no
// series for an outcome that never happened.
func TestAwaitMetricsExposition(t *testing.T) {
	m := newAwaitMetrics()
	if got := scrapeAwait(t, m); strings.Contains(got, "{outcome=") {
		t.Fatalf("a series before any observation:\n%s", got)
	}
	m.observe(StateMaterialized, 300*time.Millisecond)
	m.observe(StateTerminal, 30*time.Millisecond)
	m.observe(StateQuarantined, 3*time.Second)
	m.observe(StatePending, 10*time.Second)
	got := scrapeAwait(t, m)
	for _, want := range []string{
		"# TYPE sync_manual_trigger_await_outcome_total counter\n",
		"# TYPE sync_manual_trigger_await_latency_seconds histogram\n",
		`sync_manual_trigger_await_outcome_total{outcome="materialized"} 2` + "\n",
		`sync_manual_trigger_await_outcome_total{outcome="quarantined"} 1` + "\n",
		`sync_manual_trigger_await_outcome_total{outcome="pending"} 1` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="0.05"} 1` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="0.25"} 1` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="0.5"} 2` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="+Inf"} 2` + "\n",
		`sync_manual_trigger_await_latency_seconds_count{outcome="materialized"} 2` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="quarantined",le="2.0"} 0` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="quarantined",le="5.0"} 1` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="pending",le="10.0"} 1` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="pending",le="5.0"} 0` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Count(got, "_sum{") != 3 {
		t.Errorf("want one _sum per observed outcome:\n%s", got)
	}
}

// A nil receiver observes nothing (a caller built without metrics).
func TestAwaitMetricsNilReceiverIsSafe(t *testing.T) {
	var m *AwaitMetrics
	m.observe(StatePending, time.Second)
}
