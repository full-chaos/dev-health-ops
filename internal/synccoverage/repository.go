package synccoverage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func loadConfig(ctx context.Context, tx pgx.Tx, orgID string, configID uuid.UUID) (syncConfig, error) {
	var config syncConfig
	var targets json.RawMessage
	err := tx.QueryRow(ctx, `
SELECT id, org_id, provider, sync_targets, is_active, planner_managed,
       integration_id, source_id
FROM public.sync_configurations
WHERE id = $1 AND org_id = $2`, configID, orgID).Scan(
		&config.ID, &config.OrgID, &config.Provider, &targets, &config.Active,
		&config.PlannerManaged, &config.IntegrationID, &config.SourceID,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return syncConfig{}, ErrConfigNotFound
		}
		return syncConfig{}, fmt.Errorf("load sync coverage config: %w", err)
	}
	if err := json.Unmarshal(targets, &config.SyncTargets); err != nil {
		return syncConfig{}, fmt.Errorf("decode sync targets: %w", err)
	}
	return config, nil
}

func resolveScope(ctx context.Context, tx pgx.Tx, config syncConfig) (effectiveScope, error) {
	scope := effectiveScope{IntegrationID: config.IntegrationID}
	targetDatasets := datasetsForTargets(config.Provider, config.SyncTargets)
	if config.IntegrationID == nil {
		scope.DatasetKeys = targetDatasets
		return scope, nil
	}

	query := `
SELECT id, name, full_name
FROM public.integration_sources
WHERE org_id = $1 AND integration_id = $2 AND is_enabled IS TRUE`
	args := []any{config.OrgID, *config.IntegrationID}
	if config.SourceID != nil {
		query += " AND id = $3"
		args = append(args, *config.SourceID)
	} else if config.PlannerManaged {
		query += " AND metadata->>'planner_managed_sync_config_id' = $3"
		args = append(args, config.ID.String())
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return effectiveScope{}, fmt.Errorf("load sync coverage sources: %w", err)
	}
	for rows.Next() {
		if len(scope.Sources) >= maxSources {
			rows.Close()
			return effectiveScope{}, fmt.Errorf("sync coverage source limit exceeded: %d", maxSources)
		}
		var item source
		if err := rows.Scan(&item.ID, &item.Name, &item.FullName); err != nil {
			rows.Close()
			return effectiveScope{}, fmt.Errorf("scan sync coverage source: %w", err)
		}
		scope.Sources = append(scope.Sources, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return effectiveScope{}, fmt.Errorf("iterate sync coverage sources: %w", err)
	}
	rows.Close()

	enabledKeys, intentRowsExist, err := loadIntentDatasetKeys(ctx, tx, config.OrgID, *config.IntegrationID)
	if err != nil {
		return effectiveScope{}, err
	}

	// A target-scoped config (source-scoped child, or any non-planner-managed
	// config) derives its datasets from sync_targets alone. That list is the
	// operator's checkbox selection expanded through the legacy-target map --
	// it says nothing about whether the dataset is actually enabled. The
	// planner is is_enabled-authoritative on every path
	// (sync/planner.py::_load_enabled_datasets filters is_enabled IS TRUE and
	// only then narrows by the requested keys), so without this intersection
	// coverage advertises gap windows and backfill buttons for datasets the
	// planner would refuse to plan (CHAOS-4106).
	//
	// The intersection deliberately does NOT run on the planner-managed parent
	// path below. There scope.DatasetKeys already comes from the enabled rows,
	// which is exactly what the planner reads; intersecting that with
	// targetDatasets would drop `blame` and `security`, neither of which is
	// derivable from an operator-selectable target (`blame` is added by
	// _planner_dataset_keys when "git" is selected, `security` by
	// _ensure_security_dataset_for_scheduled_code_host_sync), and coverage
	// would go blind on two datasets that really are syncing.
	//
	// The empty-targetDatasets case keeps the pre-existing fallback: a config
	// with no sync_targets at all scopes to every enabled dataset. Only a
	// non-empty selection is intersected, so "every selected dataset is
	// disabled" resolves to an empty scope rather than inverting into the
	// fallback and advertising everything.
	//
	// An integration with NO integration_datasets rows at all is left alone: an
	// unseeded intent plane is not a statement of intent, and reading it as one
	// would blank an otherwise working config's coverage. But rows that EXIST
	// and are all disabled ARE a statement of intent -- the operator switched
	// everything off -- and must narrow to an empty scope. Hence the test is
	// intentRowsExist, not len(enabledKeys): a query filtered to enabled rows
	// alone cannot tell those two states apart (Codex adversarial review).
	targetScoped := config.SourceID != nil || !config.PlannerManaged
	switch {
	case !targetScoped || len(targetDatasets) == 0:
		scope.DatasetKeys = enabledKeys
	case !intentRowsExist:
		scope.DatasetKeys = targetDatasets
	default:
		kept, excluded := intersectEnabledDatasets(targetDatasets, enabledKeys)
		if len(excluded) > 0 {
			datasetScopeIntentMetrics.observeExcluded(config.Provider, len(excluded))
			slog.Warn(
				"sync coverage scope excluded user-disabled datasets",
				slog.String("org_id", config.OrgID),
				slog.String("sync_config_id", config.ID.String()),
				slog.String("integration_id", config.IntegrationID.String()),
				slog.String("provider", config.Provider),
				slog.String("excluded_dataset_keys", strings.Join(excluded, ",")),
				slog.Int("excluded_count", len(excluded)),
				slog.String("reason", "integration_datasets.is_enabled is false; coverage must not advertise backfill windows the planner would refuse"),
			)
		}
		scope.DatasetKeys = kept
	}
	scope.DatasetKeys = uniqueSorted(scope.DatasetKeys)
	sort.Slice(scope.Sources, func(i, j int) bool { return scope.Sources[i].ID.String() < scope.Sources[j].ID.String() })
	if len(scope.Sources)*len(scope.DatasetKeys) > maxPairs {
		return effectiveScope{}, fmt.Errorf("sync coverage pair limit exceeded: %d", maxPairs)
	}
	return scope, nil
}

// loadIntentDatasetKeys reads the intent plane: the dataset rows an operator
// has left enabled for this integration, plus whether the integration has ANY
// dataset row at all. It is the same row set
// sync/planner.py::_load_enabled_datasets plans from, so coverage scope and
// planning agree on what "enabled" means.
//
// Both facts come from ONE query on purpose. Filtering to is_enabled IS TRUE
// makes an empty result ambiguous between "never seeded" and "all switched
// off", and those two states require opposite handling in resolveScope.
func loadIntentDatasetKeys(
	ctx context.Context,
	tx pgx.Tx,
	orgID string,
	integrationID uuid.UUID,
) (enabled []string, rowsExist bool, err error) {
	rows, err := tx.Query(ctx, `
SELECT dataset_key, is_enabled
FROM public.integration_datasets
WHERE org_id = $1 AND integration_id = $2`, orgID, integrationID)
	if err != nil {
		return nil, false, fmt.Errorf("load sync coverage datasets: %w", err)
	}
	defer rows.Close()
	enabled = make([]string, 0)
	seen := 0
	for rows.Next() {
		if seen >= maxDatasets {
			return nil, false, fmt.Errorf("sync coverage dataset limit exceeded: %d", maxDatasets)
		}
		seen++
		var key string
		var isEnabled bool
		if err := rows.Scan(&key, &isEnabled); err != nil {
			return nil, false, fmt.Errorf("scan sync coverage dataset: %w", err)
		}
		if isEnabled {
			enabled = append(enabled, key)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate sync coverage datasets: %w", err)
	}
	return enabled, seen > 0, nil
}

// intersectEnabledDatasets keeps the target-derived keys that are still
// enabled, and reports the ones dropped so the caller can say so out loud.
// Order of `targets` is preserved; `excluded` is sorted for stable logging.
func intersectEnabledDatasets(targets, enabled []string) (kept, excluded []string) {
	enabledSet := make(map[string]struct{}, len(enabled))
	for _, key := range enabled {
		enabledSet[key] = struct{}{}
	}
	kept = make([]string, 0, len(targets))
	excluded = make([]string, 0)
	for _, key := range targets {
		if _, ok := enabledSet[key]; ok {
			kept = append(kept, key)
			continue
		}
		excluded = append(excluded, key)
	}
	sort.Strings(excluded)
	return kept, excluded
}
