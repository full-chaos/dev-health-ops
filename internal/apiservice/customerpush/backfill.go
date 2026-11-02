package customerpush

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// BackfillReport is what a backfill run counts. It holds counts only.
type BackfillReport struct {
	Candidates       int
	SourceBound      int
	OrganizationWide int
	Revoked          int
	Written          int
	Skipped          int
	Failed           int
}

// BackfillLifecycle emits the lifecycle events of tokens that have no aggregate.
func BackfillLifecycle(ctx context.Context, pool *pgxpool.Pool, schema string, apply bool, batch int) (BackfillReport, error) {
	return BackfillReport{}, errors.New("not implemented")
}

func storedLifecycleEvents(t tokenRow) ([]audit.PushLifecycleEvent, error) {
	return nil, errors.New("not implemented")
}
