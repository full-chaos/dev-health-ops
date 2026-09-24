package synccoverage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// InvalidateForIntegration ports
// sync_coverage.py::invalidate_sync_coverage_projection_sync (the
// integration_id selector variant -- the only one finalize_sync_run calls).
// Pure Postgres: an advisory transaction lock per resolved sync_config_id
// (serializes against a concurrent coverage rebuild) followed by a single
// invalidating UPDATE. Python's statement is an ORM-enabled update() of
// SyncCoverageProjection, whose updated_at has onupdate=func.now(), so the
// UPDATE sets updated_at = now() as well.
func InvalidateForIntegration(ctx context.Context, tx pgx.Tx, orgID, integrationID string) error {
	rows, err := tx.Query(ctx, `
SELECT id::text FROM public.sync_configurations
WHERE org_id = $1 AND integration_id = $2::uuid
ORDER BY id`, orgID, integrationID)
	if err != nil {
		return fmt.Errorf("read coverage config ids: %w", err)
	}
	var configIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan coverage config id: %w", err)
		}
		configIDs = append(configIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read coverage config ids: %w", err)
	}
	rows.Close()
	for _, configID := range configIDs {
		lockName := "sync-coverage:" + orgID + ":" + configID
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockName); err != nil {
			return fmt.Errorf("coverage lock: %w", err)
		}
	}
	if len(configIDs) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.sync_coverage_projections
SET invalidated_at = now(), updated_at = now()
WHERE org_id = $1 AND sync_config_id = ANY($2::uuid[])`, orgID, configIDs); err != nil {
		return fmt.Errorf("invalidate coverage projections: %w", err)
	}
	return nil
}
