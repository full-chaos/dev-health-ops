package synccoverage

import (
	"fmt"
	"io"
	"sort"
	"sync"
)

// ScopeIntentMetrics counts coverage-scope decisions that silently change what
// the UI advertises. CHAOS-4106 was invisible for as long as it was because
// nothing counted it: coverage scoped a dataset in, the projection grew
// backfill windows for it, and the only signal was a screenshot. A dropped
// dataset is now counted and logged at WARN at the point of the decision.
type ScopeIntentMetrics struct {
	mu                       sync.Mutex
	datasetsExcludedByIntent map[string]uint64
}

// datasetScopeIntentMetrics is the process-wide counter for scope decisions.
// Register it on a binary's health registry with RegisterMetrics to expose it.
var datasetScopeIntentMetrics = NewScopeIntentMetrics()

func NewScopeIntentMetrics() *ScopeIntentMetrics {
	return &ScopeIntentMetrics{datasetsExcludedByIntent: make(map[string]uint64)}
}

// ScopeIntentMetricsSource returns the process-wide scope-decision counters as
// a health.MetricsSource-shaped value.
func ScopeIntentMetricsSource() *ScopeIntentMetrics { return datasetScopeIntentMetrics }

// observeExcluded records that `count` datasets were dropped from a config's
// coverage scope because their integration_datasets row is disabled.
func (m *ScopeIntentMetrics) observeExcluded(provider string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.datasetsExcludedByIntent[metricProviderLabel(provider)] += uint64(count)
}

// excludedCount reports the counter value for one provider. Test-facing.
func (m *ScopeIntentMetrics) excludedCount(provider string) uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.datasetsExcludedByIntent[metricProviderLabel(provider)]
}

func (m *ScopeIntentMetrics) WritePrometheus(writer io.Writer) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	snapshot := make(map[string]uint64, len(m.datasetsExcludedByIntent))
	for label, value := range m.datasetsExcludedByIntent {
		snapshot[label] = value
	}
	m.mu.Unlock()

	if _, err := io.WriteString(writer,
		"# HELP dev_health_sync_coverage_datasets_excluded_by_intent_total "+
			"Datasets dropped from a sync coverage scope because their integration_datasets row is disabled.\n"+
			"# TYPE dev_health_sync_coverage_datasets_excluded_by_intent_total counter\n"); err != nil {
		return err
	}
	labels := make([]string, 0, len(snapshot))
	for label := range snapshot {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		if _, err := fmt.Fprintf(writer,
			"dev_health_sync_coverage_datasets_excluded_by_intent_total{provider=%q} %d\n",
			label, snapshot[label]); err != nil {
			return err
		}
	}
	return nil
}

// metricProviderLabel bounds the provider label to the providers this package
// knows about, so an unexpected value cannot blow up metric cardinality.
func metricProviderLabel(provider string) string {
	if _, ok := providerDatasets[provider]; ok {
		return provider
	}
	return "other"
}
