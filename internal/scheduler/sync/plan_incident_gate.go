package sync

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// planIncidentDatasetsSkippedEvent is the WARN written when a plan leaves out
// datasets because the canonical-incident feature is off for the organization.
const planIncidentDatasetsSkippedEvent = "sync.plan.incident_datasets_skipped_feature_disabled"

// planDatasetRequiresCanonicalIncident reports whether one dataset of a
// provider writes canonical incident data, by the same legacy-target rule the
// stored-target gate uses (syncTargetsRequireCanonicalIncident). A dataset the
// provider's catalogue does not hold is never gated: the planner plans no unit
// for it anyway.
func planDatasetRequiresCanonicalIncident(provider, dataset string) bool {
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
		if planDatasetRequiresCanonicalIncident(provider, dataset.Key) {
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
// sync_plan_gate_total (outcome feature_disabled) and one WARN names the
// dataset keys and the feature decision's reason. When EVERY enabled dataset
// needs the feature the occurrence has nothing to do and the answer stays
// ErrOccurrenceIneligible, with the same count and WARN: an accepted run that
// plans zero units and reports success would read as coverage.
//
// The second result is the number of datasets left out. The caller needs it
// after unit planning: see errIfOnlySkippedDatasetsHadWork.
func dropCanonicalIncidentDatasetsWhenFeatureOff(
	ctx context.Context, tx pgx.Tx, orgID, integrationID, provider string, datasets []PlanDataset, now time.Time,
) ([]PlanDataset, int, error) {
	kept, gated := splitCanonicalIncidentDatasets(provider, datasets)
	if len(gated) == 0 {
		return datasets, 0, nil
	}
	allowed, reason, err := canonicalIncidentDecision(ctx, tx, orgID, now, true)
	if err != nil {
		return nil, 0, err
	}
	if allowed {
		return datasets, 0, nil
	}
	skippedKeys := make([]string, 0, len(gated))
	for _, dataset := range gated {
		skippedKeys = append(skippedKeys, dataset.Key)
		globalPlanGateTelemetry.observe(provider, dataset.Key, planGateOutcomeFeatureDisabled)
	}
	slog.Default().Warn(planIncidentDatasetsSkippedEvent,
		slog.String("provider", provider),
		slog.String("org_id", orgID),
		slog.String("integration_id", integrationID),
		slog.String("reason", planGateOutcomeFeatureDisabled),
		slog.String("feature_decision_reason", string(reason)),
		slog.Int("skipped_datasets", len(skippedKeys)),
		slog.Any("skipped_dataset_keys", skippedKeys),
		slog.Int("planned_datasets", len(kept)),
	)
	if len(kept) == 0 {
		return nil, len(gated), ErrOccurrenceIneligible
	}
	return kept, len(gated), nil
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
