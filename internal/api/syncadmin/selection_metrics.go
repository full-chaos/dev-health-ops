package syncadmin

import (
	"io"
	"sort"
	"strconv"
	"sync"
)

const (
	selectionRowsChangedMetric = "sync_config_dataset_rows_changed_total"
	selectionStaleBaseMetric   = "sync_config_save_stale_base_total"
)

// SelectionMetrics counts what PATCH /sync-configs/{id} does to
// integration_datasets rows (CHAOS-8816): the rows a save switched on or
// off, by provider and direction, and the saves whose sync_targets_base was
// not the list the rows showed, by provider. It replaces
// sync_target_dataset_drift_repaired_total: a save no longer rewrites rows
// its own change does not name, so there is no repair to count.
type SelectionMetrics struct {
	mu        sync.Mutex
	rows      map[[2]string]uint64
	staleBase map[string]uint64
}

func newSelectionMetrics() *SelectionMetrics {
	return &SelectionMetrics{rows: map[[2]string]uint64{}, staleBase: map[string]uint64{}}
}

// selectionMetrics is process-wide; register it on the api's registry with
// RegisterMetrics (apiservice.RegisterOperatorMetrics).
var selectionMetrics = newSelectionMetrics()

// SelectionMetricsSource returns the process-wide counters.
func SelectionMetricsSource() *SelectionMetrics { return selectionMetrics }

func (m *SelectionMetrics) observeRows(provider, direction string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[[2]string{provider, direction}] += uint64(count)
}

func (m *SelectionMetrics) observeStaleBase(provider string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.staleBase[provider]++
}

// WritePrometheus writes both families (health.MetricsSource).
func (m *SelectionMetrics) WritePrometheus(w io.Writer) error {
	m.mu.Lock()
	rows := make(map[[2]string]uint64, len(m.rows))
	for key, value := range m.rows {
		rows[key] = value
	}
	stale := make(map[string]uint64, len(m.staleBase))
	for provider, value := range m.staleBase {
		stale[provider] = value
	}
	m.mu.Unlock()

	if _, err := io.WriteString(w, "# HELP "+selectionRowsChangedMetric+" integration_datasets rows a sync-config save switched on or off, by provider and direction.\n"+
		"# TYPE "+selectionRowsChangedMetric+" counter\n"); err != nil {
		return err
	}
	keys := make([][2]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left][0] != keys[right][0] {
			return keys[left][0] < keys[right][0]
		}
		return keys[left][1] < keys[right][1]
	})
	for _, key := range keys {
		if _, err := io.WriteString(w, selectionRowsChangedMetric+"{provider="+strconv.Quote(key[0])+",direction="+strconv.Quote(key[1])+"} "+
			strconv.FormatUint(rows[key], 10)+"\n"); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(w, "# HELP "+selectionStaleBaseMetric+" sync-config saves whose sync_targets_base was not the list the dataset rows showed, by provider.\n"+
		"# TYPE "+selectionStaleBaseMetric+" counter\n"); err != nil {
		return err
	}
	providers := make([]string, 0, len(stale))
	for provider := range stale {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	for _, provider := range providers {
		if _, err := io.WriteString(w, selectionStaleBaseMetric+"{provider="+strconv.Quote(provider)+"} "+strconv.FormatUint(stale[provider], 10)+"\n"); err != nil {
			return err
		}
	}
	return nil
}
