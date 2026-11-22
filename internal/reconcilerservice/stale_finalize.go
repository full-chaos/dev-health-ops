package reconcilerservice

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// staleFinalizeRedriver is the domain-pool half of the stale-finalize stage:
// the existing operator redrive, one run per call, never-attempted finalizes
// only (allowPriorAttempt false).
type staleFinalizeRedriver struct {
	store     *daily.PostgresStore
	publisher *daily.PostgresPublisher
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
	return staleFinalizeRedriver{store: store, publisher: publisher}, nil
}

// RedriveStaleFinalize calls RedriveStrandedFinalize with ONE run id, so its
// return at the first error can never stop another run.
func (redriver staleFinalizeRedriver) RedriveStaleFinalize(ctx context.Context, runID, nonce string) (bool, error) {
	outcome, err := redriver.store.RedriveStrandedFinalize(ctx, redriver.publisher, []string{runID}, nonce, false)
	if err != nil {
		if errors.Is(err, daily.ErrInvalidState) {
			return false, fmt.Errorf("%w: %w", joboutbox.ErrStaleFinalizeInvalidState, err)
		}
		return false, err
	}
	return len(outcome.FinalizedRunIDs) == 1, nil
}
