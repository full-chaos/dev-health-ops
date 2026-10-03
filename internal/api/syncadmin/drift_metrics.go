package syncadmin

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
)

const driftRepairedMetric = "sync_target_dataset_drift_repaired_total"

// DriftRepairedMetrics counts integration_datasets rows a config edit switched
// off that the previous-vs-new target delta never named (sync.py's
// SYNC_TARGET_DATASET_DRIFT_REPAIRED_TOTAL, by provider). Before CHAOS-8221 the
// Go api only logged the WARN, so a repair that fired looked identical on
// /metrics to one that never did.
type DriftRepairedMetrics struct {
	mu       sync.Mutex
	repaired map[string]uint64
}

// driftRepairedMetrics is process-wide; register it on the api's registry with
// RegisterMetrics (apiservice.RegisterOperatorMetrics).
var driftRepairedMetrics = &DriftRepairedMetrics{repaired: map[string]uint64{}}

// DriftRepairedMetricsSource returns the process-wide counter.
func DriftRepairedMetricsSource() *DriftRepairedMetrics { return driftRepairedMetrics }

func (m *DriftRepairedMetrics) observe(provider string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.repaired[provider] += uint64(count)
}

// WritePrometheus writes the family (health.MetricsSource).
func (m *DriftRepairedMetrics) WritePrometheus(w io.Writer) error {
	m.mu.Lock()
	snapshot := make(map[string]uint64, len(m.repaired))
	for provider, value := range m.repaired {
		snapshot[provider] = value
	}
	m.mu.Unlock()
	if _, err := io.WriteString(w, "# HELP "+driftRepairedMetric+" integration_datasets rows a sync-config edit switched off that the previous-vs-new target delta never named, by provider.\n"+
		"# TYPE "+driftRepairedMetric+" counter\n"); err != nil {
		return err
	}
	providers := make([]string, 0, len(snapshot))
	for provider := range snapshot {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	for _, provider := range providers {
		if _, err := io.WriteString(w, driftRepairedMetric+"{provider="+strconv.Quote(provider)+"} "+strconv.FormatUint(snapshot[provider], 10)+"\n"); err != nil {
			return err
		}
	}
	return nil
}

// driftedDatasetKeys is sync.py's `sorted(set(disabled_keys) - (previously_desired - desired))`
// in the form the loop above builds it: a key this call disabled that the
// previous-vs-new delta does not name.
func driftedDatasetKeys(disabled []string, previouslyDesired, desired map[string]bool) []string {
	var drifted []string
	for _, key := range disabled {
		if !(previouslyDesired[key] && !desired[key]) {
			drifted = append(drifted, key)
		}
	}
	return drifted
}

// recordDatasetDrift is the tail of sync.py's _reconcile_dataset_rows_for_sync_targets:
// when this call disabled rows the previous-vs-new delta never named, it counts
// them by provider and logs the repair.
func recordDatasetDrift(ctx context.Context, logger *slog.Logger, orgID string, integrationID uuid.UUID, provider string,
	disabled []string, previouslyDesired, desired map[string]bool) {
	drifted := driftedDatasetKeys(disabled, previouslyDesired, desired)
	if len(drifted) == 0 {
		return
	}
	driftRepairedMetrics.observe(provider, len(drifted))
	logger.WarnContext(ctx, "sync_target_dataset_drift_repaired", "org_id", orgID, "integration_id", integrationID.String(),
		"provider", provider, "drifted_dataset_keys", strings.Join(drifted, ","), "drifted_count", len(drifted),
		"reason", "integration_datasets rows were enabled that this config's sync_targets cannot account for; "+
			"the planner was syncing datasets the operator had deselected")
}
