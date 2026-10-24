package syncdispatchruntime

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/jackc/pgx/v5"
)

// observeRunCollect runs observeRun and returns every observation it emitted, in order.
func observeRunCollect(
	ctx context.Context, tx pgx.Tx, bridge budgetEstimator, logger *synclog.Logger,
	orgID, syncRunID string, cappedUnitIDs map[string]bool, now time.Time,
) ([]map[string]any, error) {
	var observations []map[string]any
	err := observeRun(ctx, tx, bridge, logger, orgID, syncRunID, cappedUnitIDs, now, func(observation map[string]any) {
		observations = append(observations, observation)
	})
	return observations, err
}
