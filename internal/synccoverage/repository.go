package synccoverage

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

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

	if config.SourceID != nil || !config.PlannerManaged {
		scope.DatasetKeys = targetDatasets
	}
	if len(scope.DatasetKeys) == 0 {
		datasetRows, err := tx.Query(ctx, `
SELECT dataset_key
FROM public.integration_datasets
WHERE org_id = $1 AND integration_id = $2 AND is_enabled IS TRUE`, config.OrgID, *config.IntegrationID)
		if err != nil {
			return effectiveScope{}, fmt.Errorf("load sync coverage datasets: %w", err)
		}
		for datasetRows.Next() {
			if len(scope.DatasetKeys) >= maxDatasets {
				datasetRows.Close()
				return effectiveScope{}, fmt.Errorf("sync coverage dataset limit exceeded: %d", maxDatasets)
			}
			var key string
			if err := datasetRows.Scan(&key); err != nil {
				datasetRows.Close()
				return effectiveScope{}, fmt.Errorf("scan sync coverage dataset: %w", err)
			}
			scope.DatasetKeys = append(scope.DatasetKeys, key)
		}
		if err := datasetRows.Err(); err != nil {
			datasetRows.Close()
			return effectiveScope{}, fmt.Errorf("iterate sync coverage datasets: %w", err)
		}
		datasetRows.Close()
	}
	scope.DatasetKeys = uniqueSorted(scope.DatasetKeys)
	sort.Slice(scope.Sources, func(i, j int) bool { return scope.Sources[i].ID.String() < scope.Sources[j].ID.String() })
	if len(scope.Sources)*len(scope.DatasetKeys) > maxPairs {
		return effectiveScope{}, fmt.Errorf("sync coverage pair limit exceeded: %d", maxPairs)
	}
	return scope, nil
}
