package sync

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// planIncidentDatasetsSkippedEvent is the WARN written when a plan leaves out
// datasets because the canonical-incident feature is off for the organization.
const planIncidentDatasetsSkippedEvent = "sync.plan.incident_datasets_skipped_feature_disabled"

// planIncidentSkipWarnNow is the clock the once-per-day WARN reads; tests replace it.
var planIncidentSkipWarnNow = time.Now

// planIncidentSkipWarnPruneAt is the entry count above which entries for past days are dropped.
const planIncidentSkipWarnPruneAt = 4096

type planIncidentSkipWarnKey struct {
	orgID         string
	integrationID string
}

type planIncidentSkipWarnState struct {
	day     string
	keysSig string
}

// planIncidentSkipWarnMemory remembers, in this process only, the last day and skipped
// dataset set a WARN was written for, per (organization, integration). A restart, or a second
// scheduler replica, writes at most one more WARN per configuration per day.
type planIncidentSkipWarnMemory struct {
	mu      sync.Mutex
	entries map[planIncidentSkipWarnKey]planIncidentSkipWarnState
}

var globalPlanIncidentSkipWarn = &planIncidentSkipWarnMemory{entries: make(map[planIncidentSkipWarnKey]planIncidentSkipWarnState)}

// shouldLog is true the first time, on a new UTC day, or when the skipped set changes
// (order ignored), and records that decision.
func (memory *planIncidentSkipWarnMemory) shouldLog(orgID, integrationID string, now time.Time, skippedKeys []string) bool {
	sorted := slices.Clone(skippedKeys)
	slices.Sort(sorted)
	state := planIncidentSkipWarnState{day: now.UTC().Format(time.DateOnly), keysSig: strings.Join(sorted, ",")}
	key := planIncidentSkipWarnKey{orgID: orgID, integrationID: integrationID}
	memory.mu.Lock()
	defer memory.mu.Unlock()
	if memory.entries == nil {
		memory.entries = make(map[planIncidentSkipWarnKey]planIncidentSkipWarnState)
	}
	if previous, ok := memory.entries[key]; ok && previous == state {
		return false
	}
	if len(memory.entries) >= planIncidentSkipWarnPruneAt {
		for other, entry := range memory.entries {
			if entry.day != state.day {
				delete(memory.entries, other)
			}
		}
	}
	memory.entries[key] = state
	return true
}

func (memory *planIncidentSkipWarnMemory) resetForTest() {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	memory.entries = make(map[planIncidentSkipWarnKey]planIncidentSkipWarnState)
}

// emitIncidentSkipWarn writes the skip WARN once per configuration per UTC day, or again
// when the skipped set changes. sync_plan_gate_total is counted by the caller on every attempt.
func emitIncidentSkipWarn(provider, orgID, integrationID, decisionReason string, skippedKeys []string, plannedDatasets int) {
	if !globalPlanIncidentSkipWarn.shouldLog(orgID, integrationID, planIncidentSkipWarnNow(), skippedKeys) {
		return
	}
	slog.Default().Warn(planIncidentDatasetsSkippedEvent,
		slog.String("provider", provider),
		slog.String("org_id", orgID),
		slog.String("integration_id", integrationID),
		slog.String("reason", planGateOutcomeFeatureDisabled),
		slog.String("feature_decision_reason", decisionReason),
		slog.Int("skipped_datasets", len(skippedKeys)),
		slog.Any("skipped_dataset_keys", skippedKeys),
		slog.Int("planned_datasets", plannedDatasets),
	)
}

// PlanDatasetRequiresCanonicalIncident reports whether one dataset of a
// provider writes canonical incident data, by the same legacy-target rule the
// stored-target gate uses (syncTargetsRequireCanonicalIncident). A dataset the
// provider's catalogue does not hold is never gated: the planner plans no unit
// for it anyway.
func PlanDatasetRequiresCanonicalIncident(provider, dataset string) bool {
	spec, ok := datasetSpecification(provider, dataset)
	if !ok {
		return false
	}
	return syncTargetsRequireCanonicalIncident(spec.LegacyTargets)
}

// splitCanonicalIncidentDatasets separates the datasets that need the
// canonical-incident feature from the ones that do not. Order is kept on both
// sides, so the planner sees the remaining datasets exactly as it would have.
func splitCanonicalIncidentDatasets(provider string, datasets []PlanDataset) (kept, gated []PlanDataset) {
	for _, dataset := range datasets {
		if PlanDatasetRequiresCanonicalIncident(provider, dataset.Key) {
			gated = append(gated, dataset)
			continue
		}
		kept = append(kept, dataset)
	}
	return kept, gated
}

// dropCanonicalIncidentDatasetsWhenFeatureOff is the plan-time gate on the
// enabled dataset rows. When the canonical-incident feature is off for the
// organization, it leaves the datasets that need the feature out of the plan
// and returns the rest, so one feature decision stops only the data it
// governs: no unit is planned or minted for a left-out dataset, and every
// other dataset of the configuration runs.
//
// The feature is consulted only when at least one enabled dataset needs it,
// and through the row-locking phase-B read, as before.
//
// A skip is never silent. Each left-out dataset counts on
// sync_plan_gate_total (outcome feature_disabled) on every attempt, and a WARN
// names the dataset keys and the feature decision's reason once per
// configuration per UTC day, or again when the skipped set changes. When EVERY enabled dataset
// needs the feature the occurrence has nothing to do and the answer stays
// ErrOccurrenceIneligible, with the same count and WARN: an accepted run that
// plans zero units and reports success would read as coverage.
//
// The second result is the keys of the datasets left out, in plan order. The
// caller needs the count after unit planning (see
// errIfOnlySkippedDatasetsHadWork) and writes the keys onto the run
// (SkippedDatasetsResult).
func dropCanonicalIncidentDatasetsWhenFeatureOff(
	ctx context.Context, tx pgx.Tx, orgID, integrationID, provider string, datasets []PlanDataset, now time.Time,
) ([]PlanDataset, []string, error) {
	kept, gated := splitCanonicalIncidentDatasets(provider, datasets)
	if len(gated) == 0 {
		return datasets, nil, nil
	}
	allowed, reason, err := canonicalIncidentDecision(ctx, tx, orgID, now, true)
	if err != nil {
		return nil, nil, err
	}
	if allowed {
		return datasets, nil, nil
	}
	skippedKeys := make([]string, 0, len(gated))
	for _, dataset := range gated {
		skippedKeys = append(skippedKeys, dataset.Key)
		globalPlanGateTelemetry.observe(provider, dataset.Key, planGateOutcomeFeatureDisabled)
	}
	emitIncidentSkipWarn(provider, orgID, integrationID, string(reason), skippedKeys, len(kept))
	if len(kept) == 0 {
		return nil, skippedKeys, ErrOccurrenceIneligible
	}
	return kept, skippedKeys, nil
}

// The skip reason and the sync-status text a run carries for a dataset left out
// because the canonical-incident feature is off.
const (
	skippedDatasetReasonFeatureDisabled = planGateOutcomeFeatureDisabled
	skippedDatasetMessageFeatureOff     = "skipped: feature off"
)

// SkippedDatasetsResult is the entry the plan writes into sync_runs.result
// under "skipped_datasets": one object per left-out dataset with its key and
// reason. nil when nothing was left out, so a run without a skip keeps the
// result it always had.
func SkippedDatasetsResult(keys []string) []map[string]string {
	if len(keys) == 0 {
		return nil
	}
	entries := make([]map[string]string, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, map[string]string{
			"dataset_key": key,
			"reason":      skippedDatasetReasonFeatureDisabled,
			"message":     skippedDatasetMessageFeatureOff,
		})
	}
	return entries
}

// errIfOnlySkippedDatasetsHadWork keeps an occurrence ineligible when datasets
// were left out because the feature is off and the remaining datasets plan no
// unit at all (for example, every source is disabled). Such a run would hold
// zero units, and the dispatch-time feature check, which falls back to the
// enabled dataset rows for a run with no unit, would fail it as
// feature_disabled on every occurrence. Nothing is fetched either way; this
// keeps the one answer the occurrence had before, with the skip already
// counted and written by dropCanonicalIncidentDatasetsWhenFeatureOff.
func errIfOnlySkippedDatasetsHadWork(skippedDatasets, plannedUnits int) error {
	if skippedDatasets > 0 && plannedUnits == 0 {
		return ErrOccurrenceIneligible
	}
	return nil
}
