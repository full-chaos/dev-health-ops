package sync

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func callLoadFailureStreaks(
	ctx context.Context, tx pgx.Tx, orgID, integrationID, provider string, datasets []PlanDataset,
) (map[string]providersync.FailureStreak, error) {
	return loadFailureStreaks(ctx, tx, orgID, integrationID, provider, datasets)
}
