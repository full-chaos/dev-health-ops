package syncadmin

import (
	"io"
	"sort"
	"strconv"
	"sync"
)

const selectionRowsChangedMetric = "sync_config_dataset_rows_changed_total"

// SelectionMetrics counts what PATCH /sync-configs/{id} does to
// integration_datasets rows (CHAOS-8816): the rows a save switched on or
// off, by provider and direction. It replaces
// sync_target_dataset_drift_repaired_total: a save no longer rewrites rows
// its own change does not name, so there is no repair to count.
type SelectionMetrics struct {
	mu   sync.Mutex
	rows map[[2]string]uint64
}

func newSelectionMetrics() *SelectionMetrics {
	return &SelectionMetrics{rows: map[[2]string]uint64{}}
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

// WritePrometheus writes the family (health.MetricsSource).
func (m *SelectionMetrics) WritePrometheus(w io.Writer) error {
	m.mu.Lock()
	rows := make(map[[2]string]uint64, len(m.rows))
	for key, value := range m.rows {
		rows[key] = value
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
	return nil
}
