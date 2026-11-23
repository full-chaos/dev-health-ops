package reconcilerservice

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// staleFinalizeRedriver is the domain-pool half of the stale-finalize stage:
// the operator redrive's locked re-verify, one run per call, never-attempted
// finalizes only, plus the survey's outbox re-check under the run lock.
type staleFinalizeRedriver struct {
	store     *daily.PostgresStore
	publisher *daily.PostgresPublisher
	clock     func() time.Time
}

func newStaleFinalizeRedriver(domainPool *pgxpool.Pool, registry joboutbox.PolicyRegistry) (staleFinalizeRedriver, error) {
	store, err := daily.NewPostgresStore(domainPool)
	if err != nil {
		return staleFinalizeRedriver{}, err
	}
	publisher, err := daily.NewPostgresPublisher(domainPool, registry)
	if err != nil {
		return staleFinalizeRedriver{}, err
	}
	return staleFinalizeRedriver{store: store, publisher: publisher, clock: time.Now}, nil
}

// RedriveStaleFinalize redrives ONE run, so its return at the first error can
// never stop another run. Under the run lock the redrive re-checks the
// survey's outbox refusals (an unrelayed row, a row younger than the horizon).
func (redriver staleFinalizeRedriver) RedriveStaleFinalize(ctx context.Context, runID, nonce string) (bool, error) {
	redriven, err := redriver.store.RedriveStaleFinalize(ctx, redriver.publisher, runID, nonce,
		redriver.clock().UTC().Add(-joboutbox.StaleFinalizeHorizon))
	if err != nil {
		if errors.Is(err, daily.ErrInvalidState) {
			return false, fmt.Errorf("%w: %w", joboutbox.ErrStaleFinalizeInvalidState, err)
		}
		return false, err
	}
	return redriven, nil
}
