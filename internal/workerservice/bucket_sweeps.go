package workerservice

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// newBucketSweepEnqueuer builds the sync.bucket_sweep enqueuer (CHAOS-14250) on an insert-only River client over the
// queue-control pool: the queue role holds INSERT on the River tables, the domain role holds none.
func newBucketSweepEnqueuer(cfg config.Config, queueControl *pgxpool.Pool) (*syncdispatchruntime.RiverBucketSweepEnqueuer, error) {
	if queueControl == nil {
		return nil, errWorkerDependencyUnavailable
	}
	client, err := river.NewClient(riverpgxv5.New(queueControl), &river.Config{
		Schema: cfg.RiverDatabaseSchema,
		Logger: slog.Default(),
	})
	if err != nil {
		return nil, errWorkerDependencyUnavailable
	}
	return syncdispatchruntime.NewRiverBucketSweepEnqueuer(client)
}
