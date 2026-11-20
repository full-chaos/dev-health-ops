//go:build integration

package syncdispatchruntime

import (
	"context"
	"sync"
	"testing"
	"time"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// CHAOS-14250: sync.bucket_sweep's River uniqueness on the pinned River schema, inserted by the queue role through the
// production enqueuer (RiverBucketSweepEnqueuer).

const (
	sweepRiverOrg      = "00000000-0000-4000-8000-000000142599"
	sweepRiverProvider = "github"
	sweepRiverClass    = "medium"
)

func withSweepRiver(t *testing.T, fn func(ctx context.Context, enqueuer *RiverBucketSweepEnqueuer, admin *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	}()
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	domainRole, err := containers.RoleName("sync_bucket_sweep_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("sync_bucket_sweep_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	defer containers.DropRole(admin, domainRole, t.Logf)
	defer containers.DropRole(admin, queueRole, t.Logf)
	for _, statement := range []string{
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE ROLE " + domainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + integrationDomainPassword + "'",
		"CREATE ROLE " + queueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + integrationQueuePassword + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + domainRole + ", " + queueRole,
		"CREATE TABLE public.worker_job_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_dispatch_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_dispatch_transport_routes (kind text PRIMARY KEY, generation bigint NOT NULL)",
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}
	queuePool, err := pgxpool.New(ctx, integrationRoleURI(t, instance.URI, queueRole))
	if err != nil {
		t.Fatal(err)
	}
	defer queuePool.Close()
	client, err := river.NewClient(riverpgxv5.New(queuePool), &river.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	enqueuer, err := NewRiverBucketSweepEnqueuer(client)
	if err != nil {
		t.Fatal(err)
	}
	fn(ctx, enqueuer, admin)
}

type sweepJobRow struct {
	id          int64
	state       string
	scheduledAt time.Time
}

func sweepJobs(t *testing.T, ctx context.Context, admin *pgxpool.Pool) []sweepJobRow {
	t.Helper()
	rows, err := admin.Query(ctx, `SELECT id, state::text, scheduled_at FROM river.river_job WHERE kind = 'sync.bucket_sweep' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sweepJobRow, error) {
		var job sweepJobRow
		return job, row.Scan(&job.id, &job.state, &job.scheduledAt)
	})
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

// S6: 50 concurrent releases of one bucket leave ONE pending sweep row.
func TestConcurrentReleasesCollapseIntoOneSweep(t *testing.T) {
	withSweepRiver(t, func(ctx context.Context, enqueuer *RiverBucketSweepEnqueuer, admin *pgxpool.Pool) {
		var group sync.WaitGroup
		errs := make(chan error, 50)
		for i := 0; i < 50; i++ {
			group.Add(1)
			go func() {
				defer group.Done()
				errs <- enqueuer.EnqueueBucketSweep(ctx, sweepRiverOrg, sweepRiverProvider, sweepRiverClass)
			}()
		}
		group.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("enqueue: %v", err)
			}
		}
		jobs := sweepJobs(t, ctx, admin)
		t.Logf("50 concurrent releases -> %d sweep rows %v", len(jobs), jobs)
		if len(jobs) != 1 || jobs[0].state != "available" {
			t.Fatalf("50 concurrent releases left %d sweep rows (%v), want 1 available", len(jobs), jobs)
		}
		if err := enqueuer.EnqueueBucketSweep(ctx, sweepRiverOrg, sweepRiverProvider, "light"); err != nil {
			t.Fatal(err)
		}
		if got := len(sweepJobs(t, ctx, admin)); got != 2 {
			t.Fatalf("a release of another bucket made %d rows in all, want 2 (one per bucket)", got)
		}
	})
}

// S7: a release makes a sweep that waits on its safety re-arm (scheduled an hour ahead) due now; a capped pass's
// ensure leaves it where it is.
func TestReleaseMakesAWaitingSweepDueNow(t *testing.T) {
	withSweepRiver(t, func(ctx context.Context, enqueuer *RiverBucketSweepEnqueuer, admin *pgxpool.Pool) {
		if err := enqueuer.EnsureBucketSweep(ctx, sweepRiverOrg, sweepRiverProvider, sweepRiverClass); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, `UPDATE river.river_job SET state = 'scheduled', scheduled_at = now() + interval '1 hour' WHERE kind = 'sync.bucket_sweep'`); err != nil {
			t.Fatal(err)
		}
		if err := enqueuer.EnsureBucketSweep(ctx, sweepRiverOrg, sweepRiverProvider, sweepRiverClass); err != nil {
			t.Fatal(err)
		}
		jobs := sweepJobs(t, ctx, admin)
		if len(jobs) != 1 || jobs[0].state != "scheduled" || time.Until(jobs[0].scheduledAt) < 50*time.Minute {
			t.Fatalf("ensure moved the waiting sweep: %v", jobs)
		}
		if err := enqueuer.EnqueueBucketSweep(ctx, sweepRiverOrg, sweepRiverProvider, sweepRiverClass); err != nil {
			t.Fatal(err)
		}
		jobs = sweepJobs(t, ctx, admin)
		t.Logf("release on a sweep scheduled +1h -> %v", jobs)
		if len(jobs) != 1 || jobs[0].state != "available" || time.Until(jobs[0].scheduledAt) > time.Minute {
			t.Fatalf("a release left the waiting sweep at %v, want one row available now", jobs)
		}
	})
}

// A finished sweep does not block the next: a release after a completed sweep inserts a new one.
func TestCompletedSweepDoesNotBlockTheNext(t *testing.T) {
	withSweepRiver(t, func(ctx context.Context, enqueuer *RiverBucketSweepEnqueuer, admin *pgxpool.Pool) {
		if err := enqueuer.EnqueueBucketSweep(ctx, sweepRiverOrg, sweepRiverProvider, sweepRiverClass); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, `UPDATE river.river_job SET state = 'completed', finalized_at = now() WHERE kind = 'sync.bucket_sweep'`); err != nil {
			t.Fatal(err)
		}
		if err := enqueuer.EnqueueBucketSweep(ctx, sweepRiverOrg, sweepRiverProvider, sweepRiverClass); err != nil {
			t.Fatal(err)
		}
		jobs := sweepJobs(t, ctx, admin)
		t.Logf("release after a completed sweep -> %v", jobs)
		if len(jobs) != 2 || jobs[0].state != "completed" || jobs[1].state != "available" {
			t.Fatalf("a completed sweep blocked the next: %v, want completed + available", jobs)
		}
	})
}
