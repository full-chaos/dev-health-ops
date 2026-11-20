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

// FoldedKeyResolutionMetrics counts coverage windows attributed to a folded
// child dataset key (e.g. "tests") via a canonical composite unit's
// family_dataset_* flags (e.g. a "cicd" unit carrying
// family_dataset_tests=true). CHAOS-4393: without a counter here, a fold
// silently never firing (a policy/flag mismatch reintroducing the
// unclosable-gap bug) looks identical to a fold correctly firing zero times
// because no org enabled a folded alias -- only a labeled per-canonical-key
// counter tells the two apart.
type FoldedKeyResolutionMetrics struct {
	mu         sync.Mutex
	resolution map[string]uint64
}

var foldedKeyResolutionMetrics = NewFoldedKeyResolutionMetrics()

func NewFoldedKeyResolutionMetrics() *FoldedKeyResolutionMetrics {
	return &FoldedKeyResolutionMetrics{resolution: make(map[string]uint64)}
}

// FoldedKeyResolutionMetricsSource returns the process-wide folded-key
// resolution counters as a health.MetricsSource-shaped value.
func FoldedKeyResolutionMetricsSource() *FoldedKeyResolutionMetrics {
	return foldedKeyResolutionMetrics
}

// observe records that `count` coverage windows were attributed to family
// child keys decoded from a canonical unit keyed `canonicalDataset`.
func (m *FoldedKeyResolutionMetrics) observe(canonicalDataset string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resolution[canonicalDataset] += uint64(count)
}

// resolutionCount reports the counter value for one canonical dataset key.
// Test-facing.
func (m *FoldedKeyResolutionMetrics) resolutionCount(canonicalDataset string) uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resolution[canonicalDataset]
}

func (m *FoldedKeyResolutionMetrics) WritePrometheus(writer io.Writer) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	snapshot := make(map[string]uint64, len(m.resolution))
	for label, value := range m.resolution {
		snapshot[label] = value
	}
	m.mu.Unlock()

	if _, err := io.WriteString(writer,
		"# HELP dev_health_sync_coverage_folded_key_resolutions_total "+
			"Coverage windows attributed to a folded child dataset key via a canonical composite unit's family_dataset_* flags.\n"+
			"# TYPE dev_health_sync_coverage_folded_key_resolutions_total counter\n"); err != nil {
		return err
	}
	labels := make([]string, 0, len(snapshot))
	for label := range snapshot {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		if _, err := fmt.Fprintf(writer,
			"dev_health_sync_coverage_folded_key_resolutions_total{canonical_dataset_key=%q} %d\n",
			label, snapshot[label]); err != nil {
			return err
		}
	}
	return nil
}

// Backfill resolution outcomes: how a coverage build resolved each backfill_jobs row of the config.
const (
	backfillOutcomeSyncRun             = "sync_run"
	backfillOutcomeOccurrence          = "occurrence"
	backfillOutcomeOccurrenceUnplanned = "occurrence_unplanned"
	backfillOutcomeUnresolved          = "unresolved"
)

var backfillResolutionOutcomes = []string{
	backfillOutcomeOccurrence, backfillOutcomeOccurrenceUnplanned, backfillOutcomeSyncRun, backfillOutcomeUnresolved,
}

// BackfillResolutionMetrics counts backfill jobs by how a coverage build resolved them to a run (CHAOS-14298).
// occurrence_unplanned (no run planned for an admin backfill's occurrence) and unresolved (no run marker at all)
// are the jobs coverage cannot read units for.
type BackfillResolutionMetrics struct {
	mu       sync.Mutex
	outcomes map[string]uint64
}

var backfillResolutionMetrics = NewBackfillResolutionMetrics()

func NewBackfillResolutionMetrics() *BackfillResolutionMetrics {
	return &BackfillResolutionMetrics{outcomes: make(map[string]uint64)}
}

// BackfillResolutionMetricsSource returns the process-wide backfill resolution counters as a
// health.MetricsSource-shaped value.
func BackfillResolutionMetricsSource() *BackfillResolutionMetrics { return backfillResolutionMetrics }

func (m *BackfillResolutionMetrics) observe(outcomes map[string]int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, outcome := range backfillResolutionOutcomes {
		if count := outcomes[outcome]; count > 0 {
			m.outcomes[outcome] += uint64(count)
		}
	}
}

// outcomeCount reports the counter value for one outcome. Test-facing.
func (m *BackfillResolutionMetrics) outcomeCount(outcome string) uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.outcomes[outcome]
}

func (m *BackfillResolutionMetrics) WritePrometheus(writer io.Writer) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	snapshot := make(map[string]uint64, len(backfillResolutionOutcomes))
	for _, outcome := range backfillResolutionOutcomes {
		snapshot[outcome] = m.outcomes[outcome]
	}
	m.mu.Unlock()

	if _, err := io.WriteString(writer,
		"# HELP dev_health_sync_coverage_backfill_resolutions_total "+
			"Backfill jobs a sync coverage build resolved, by outcome (sync_run, occurrence, occurrence_unplanned, unresolved).\n"+
			"# TYPE dev_health_sync_coverage_backfill_resolutions_total counter\n"); err != nil {
		return err
	}
	for _, outcome := range backfillResolutionOutcomes {
		if _, err := fmt.Fprintf(writer,
			"dev_health_sync_coverage_backfill_resolutions_total{outcome=%q} %d\n",
			outcome, snapshot[outcome]); err != nil {
			return err
		}
	}
	return nil
}
