package synchandoff

import (
	"io"
	"strconv"
	"sync"
	"time"
)

const (
	awaitOutcomeMetric = "sync_manual_trigger_await_outcome_total"
	awaitLatencyMetric = "sync_manual_trigger_await_latency_seconds"
)

// awaitBuckets and awaitBucketLabels are the Python histogram's buckets
// (metrics/prometheus.py SYNC_MANUAL_TRIGGER_AWAIT_LATENCY_SECONDS), with the
// le text prometheus_client writes for them.
var (
	awaitBuckets      = []float64{0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0}
	awaitBucketLabels = []string{"0.05", "0.1", "0.25", "0.5", "1.0", "2.0", "5.0", "10.0", "20.0", "+Inf"}
	awaitOutcomeNames = []string{"materialized", "pending", "quarantined"}
)

// awaitMetrics is sync_manual_trigger_await_outcome_total and
// sync_manual_trigger_await_latency_seconds (CHAOS-8222): the terminal outcome
// of Wait and the time it polled, both by outcome. Every caller of Wait (the
// manual Sync Now and Backfill routes, the integrations hand-off, the backfill
// verb) is counted, because the count is taken inside Wait.
type awaitMetrics struct {
	mu      sync.Mutex
	count   map[string]uint64
	sum     map[string]float64
	buckets map[string][]uint64 // per outcome, cumulative, one slot per awaitBuckets entry then +Inf
}

var processAwaitMetrics = &awaitMetrics{count: map[string]uint64{}, sum: map[string]float64{}, buckets: map[string][]uint64{}}

// AwaitMetricsSource returns the process-wide metrics; register it on the api
// registry with RegisterMetrics.
func AwaitMetricsSource() *awaitMetrics { return processAwaitMetrics }

// awaitOutcomeLabel maps a Wait state to the Python outcome label. A disabled
// PagerDuty sync (StateTerminal) is a completed occurrence there, so it is
// "materialized".
func awaitOutcomeLabel(state State) string {
	switch state {
	case StateQuarantined:
		return "quarantined"
	case StatePending:
		return "pending"
	default:
		return "materialized"
	}
}

func (m *awaitMetrics) observe(state State, elapsed time.Duration) {
	if m == nil {
		return
	}
	outcome := awaitOutcomeLabel(state)
	seconds := elapsed.Seconds()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count[outcome]++
	m.sum[outcome] += seconds
	slots := m.buckets[outcome]
	if slots == nil {
		slots = make([]uint64, len(awaitBuckets)+1)
		m.buckets[outcome] = slots
	}
	for i, bound := range awaitBuckets {
		if seconds <= bound {
			slots[i]++
		}
	}
	slots[len(awaitBuckets)]++
}

// WritePrometheus writes both families (health.MetricsSource). A series exists
// only after its first observation, as in prometheus_client for a labelled
// family.
func (m *awaitMetrics) WritePrometheus(w io.Writer) error {
	type snap struct {
		count   uint64
		sum     float64
		buckets []uint64
	}
	m.mu.Lock()
	snaps := map[string]snap{}
	for outcome, n := range m.count {
		snaps[outcome] = snap{count: n, sum: m.sum[outcome], buckets: append([]uint64(nil), m.buckets[outcome]...)}
	}
	m.mu.Unlock()
	out := "# HELP " + awaitOutcomeMetric + " Terminal outcomes of the bounded await on a Go-owned scheduled_sync_occurrences row materializing a manual Sync Now or Backfill trigger: materialized, pending (deadline elapsed, never an error), or quarantined (a client-visible failure).\n" +
		"# TYPE " + awaitOutcomeMetric + " counter\n"
	for _, outcome := range awaitOutcomeNames {
		if s, ok := snaps[outcome]; ok {
			out += awaitOutcomeMetric + "{outcome=\"" + outcome + "\"} " + strconv.FormatUint(s.count, 10) + "\n"
		}
	}
	out += "# HELP " + awaitLatencyMetric + " Wall-clock time the await spent polling before reaching a terminal outcome, labeled by that outcome.\n" +
		"# TYPE " + awaitLatencyMetric + " histogram\n"
	for _, outcome := range awaitOutcomeNames {
		s, ok := snaps[outcome]
		if !ok {
			continue
		}
		for i, label := range awaitBucketLabels {
			out += awaitLatencyMetric + "_bucket{outcome=\"" + outcome + "\",le=\"" + label + "\"} " + strconv.FormatUint(s.buckets[i], 10) + "\n"
		}
		out += awaitLatencyMetric + "_sum{outcome=\"" + outcome + "\"} " + strconv.FormatFloat(s.sum, 'g', -1, 64) + "\n"
		out += awaitLatencyMetric + "_count{outcome=\"" + outcome + "\"} " + strconv.FormatUint(s.count, 10) + "\n"
	}
	_, err := io.WriteString(w, out)
	return err
}
