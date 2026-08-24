package syncdispatchruntime

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// ZeroUnitFinalizationMetrics is the native counterpart of
// src/dev_health_ops/sync/zero_unit_telemetry.py's
// ZERO_UNIT_FINALIZATIONS_TOTAL. There is no OTel-metrics or
// prometheus/client_golang usage anywhere in this Go tree (checked); the
// established pattern for a process-wide Prometheus counter here is a
// mutex-protected map plus a WritePrometheus method registered on a
// binary's health.Registry -- see internal/synccoverage.ScopeIntentMetrics,
// which this mirrors exactly. The metric NAME is kept identical to the
// Python counter's ("devhealth_sync_run_zero_unit_finalizations_total", not
// this tree's usual "dev_health_..." spelling) deliberately: it is the same
// operational signal the CHAOS-4159 zero-unit-finalize incident depends on,
// and a dashboard querying it must not care which runtime emitted it during
// the bridge-to-native cutover.
type ZeroUnitFinalizationMetrics struct {
	mu     sync.Mutex
	counts map[zeroUnitLabelKey]uint64
}

type zeroUnitLabelKey struct {
	provider string
	reason   string
}

// zeroUnitFinalizationMetrics is the process-wide counter. Register it on a
// binary's health registry with RegisterMetrics to expose it.
var zeroUnitFinalizationMetrics = NewZeroUnitFinalizationMetrics()

func NewZeroUnitFinalizationMetrics() *ZeroUnitFinalizationMetrics {
	return &ZeroUnitFinalizationMetrics{counts: make(map[zeroUnitLabelKey]uint64)}
}

// ZeroUnitFinalizationMetricsSource returns the process-wide zero-unit
// finalization counters as a health.MetricsSource-shaped value.
func ZeroUnitFinalizationMetricsSource() *ZeroUnitFinalizationMetrics {
	return zeroUnitFinalizationMetrics
}

// observe records one zero-unit finalization for (provider, reason). Called
// only AFTER the finalizing transaction commits -- see Finalize's comment on
// why the increment cannot live inside it (a counter incremented inside a
// transaction that then rolls back would overcount retries).
func (metrics *ZeroUnitFinalizationMetrics) observe(provider, reason string) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.counts[zeroUnitLabelKey{provider: provider, reason: reason}]++
}

// countFor reports the counter value for one (provider, reason) pair.
// Test-facing.
func (metrics *ZeroUnitFinalizationMetrics) countFor(provider, reason string) uint64 {
	if metrics == nil {
		return 0
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	return metrics.counts[zeroUnitLabelKey{provider: provider, reason: reason}]
}

func (metrics *ZeroUnitFinalizationMetrics) WritePrometheus(writer io.Writer) error {
	if metrics == nil {
		return nil
	}
	metrics.mu.Lock()
	snapshot := make(map[zeroUnitLabelKey]uint64, len(metrics.counts))
	for key, value := range metrics.counts {
		snapshot[key] = value
	}
	metrics.mu.Unlock()

	if _, err := io.WriteString(writer,
		"# HELP devhealth_sync_run_zero_unit_finalizations_total "+
			"Sync runs finalized with zero planned units, by provider and by the cause finalize classified them under.\n"+
			"# TYPE devhealth_sync_run_zero_unit_finalizations_total counter\n"); err != nil {
		return err
	}
	type row struct {
		key zeroUnitLabelKey
	}
	rows := make([]row, 0, len(snapshot))
	for key := range snapshot {
		rows = append(rows, row{key: key})
	}
	sort.Slice(rows, func(left, right int) bool {
		if rows[left].key.provider != rows[right].key.provider {
			return rows[left].key.provider < rows[right].key.provider
		}
		return rows[left].key.reason < rows[right].key.reason
	})
	for _, entry := range rows {
		if _, err := fmt.Fprintf(writer,
			"devhealth_sync_run_zero_unit_finalizations_total{provider=%q,reason=%q} %d\n",
			escapeLabelValue(entry.key.provider), escapeLabelValue(entry.key.reason),
			snapshot[entry.key]); err != nil {
			return err
		}
	}
	return nil
}

// escapeLabelValue guards against a provider/reason value containing a
// quote or backslash corrupting the Prometheus text-exposition line. Neither
// axis is a bounded enum here (provider comes from Integration.provider,
// reason can be any planner-recorded string), unlike
// ScopeIntentMetrics.metricProviderLabel's closed provider set -- so this
// escapes rather than clamps to a fixed allow-list.
func escapeLabelValue(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return replacer.Replace(value)
}
