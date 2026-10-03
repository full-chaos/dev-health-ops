package synchandoff

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func scrapeAwait(t *testing.T, m *awaitMetrics) string {
	t.Helper()
	var b strings.Builder
	if err := m.WritePrometheus(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func newAwaitMetrics() *awaitMetrics {
	return &awaitMetrics{count: map[string]uint64{}, sum: map[string]float64{}, buckets: map[string][]uint64{}}
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
	var m *awaitMetrics
	m.observe(StatePending, time.Second)
}

// Every Python bucket bound and its le text is pinned: one outcome observed AT
// each bound and 1ms ABOVE each bound, then the ten bucket lines read back.
// Cumulative counts are 2i+1 for bound i (the AT observations of bounds 0..i and
// the ABOVE observations of bounds 0..i-1) and 18 for +Inf.
func TestAwaitBucketBoundsAndLeTextArePythons(t *testing.T) {
	m := newAwaitMetrics()
	bounds := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
		time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 20 * time.Second}
	for _, bound := range bounds {
		m.observe(StateMaterialized, bound)
		m.observe(StateMaterialized, bound+time.Millisecond)
	}
	got := scrapeAwait(t, m)
	labels := []string{"0.05", "0.1", "0.25", "0.5", "1.0", "2.0", "5.0", "10.0", "20.0"}
	for i, label := range labels {
		want := fmt.Sprintf(`sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="%s"} %d`+"\n", label, 2*i+1)
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if want := `sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="+Inf"} 18` + "\n"; !strings.Contains(got, want) {
		t.Errorf("missing %q in:\n%s", want, got)
	}
	if n := strings.Count(got, "_bucket{"); n != 10 {
		t.Errorf("want ten bucket lines, got %d", n)
	}
}

// observe and a scrape race-free: run under the race leg.
func TestAwaitMetricsObserveAndScrapeAreRaceFree(t *testing.T) {
	m := newAwaitMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				m.observe(StatePending, time.Millisecond)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				var b strings.Builder
				_ = m.WritePrometheus(&b)
			}
		}()
	}
	wg.Wait()
	if got := scrapeAwait(t, m); !strings.Contains(got, `sync_manual_trigger_await_outcome_total{outcome="pending"} 1600`+"\n") {
		t.Errorf("lost observations:\n%s", got)
	}
}
