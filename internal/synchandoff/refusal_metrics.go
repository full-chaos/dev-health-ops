package synchandoff

import (
	"io"
	"sort"
	"strconv"
	"sync"
)

const refusedMetric = "dev_health_sync_manual_trigger_refused_total"

const (
	refusalUnnamedCaller    = "unnamed_caller"
	refusalUnselectedSource = "unselected_source"
)

// refusalMetrics is dev_health_sync_manual_trigger_refused_total: the triggers Mint
// refused before any write, by reason (unnamed_caller, unselected_source).
type refusalMetrics struct {
	mu    sync.Mutex
	count map[string]uint64
}

var processRefusalMetrics = &refusalMetrics{count: map[string]uint64{}}

// RefusalMetricsSource returns the process-wide counter; register it on the
// registry of each process that mints triggers.
func RefusalMetricsSource() *refusalMetrics { return processRefusalMetrics }

func (m *refusalMetrics) observe(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count[reason]++
}

func (m *refusalMetrics) value(reason string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count[reason]
}

// WritePrometheus writes the family (health.MetricsSource).
func (m *refusalMetrics) WritePrometheus(w io.Writer) error {
	m.mu.Lock()
	reasons := make([]string, 0, len(m.count))
	counts := make(map[string]uint64, len(m.count))
	for reason, n := range m.count {
		reasons = append(reasons, reason)
		counts[reason] = n
	}
	m.mu.Unlock()
	sort.Strings(reasons)
	out := "# HELP " + refusedMetric + " Manual sync triggers refused before any write, by reason: unnamed_caller (the trigger path names no caller) or unselected_source (a source the configuration does not select).\n" +
		"# TYPE " + refusedMetric + " counter\n"
	for _, reason := range reasons {
		out += refusedMetric + "{reason=" + strconv.Quote(reason) + "} " + strconv.FormatUint(counts[reason], 10) + "\n"
	}
	_, err := io.WriteString(w, out)
	return err
}
