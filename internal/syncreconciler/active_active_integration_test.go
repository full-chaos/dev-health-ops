//go:build integration

package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type activeActiveHarness struct {
	admin  *pgxpool.Pool
	domain *pgxpool.Pool
	queue  *pgxpool.Pool
	client *river.Client[pgx.Tx]
}

// startActiveActiveHarness builds one PostgreSQL instance with the production
// least-privilege roles, the River schema, and the sync-dispatch fixture. The
// subtests share it: each reconciler scenario truncates and reseeds rather than
// paying for a fresh container.
func startActiveActiveHarness(t *testing.T, ctx context.Context) activeActiveHarness {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	// CREATE ROLE is cluster-scoped, not database-scoped -- a scratch
	// database does not isolate it (CHAOS-4661). Deriving both role names
	// from this call's own database identity is what makes two successive
	// runs, and two concurrent lanes, collision-free.
	roleSuffix, err := containers.RoleSuffix(instance)
	if err != nil {
		t.Fatal(err)
	}
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	kernelDomainRole := "kernel_domain_runtime_" + roleSuffix
	kernelQueueRole := "kernel_queue_runtime_" + roleSuffix

	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err := createKernelIntegrationFixture(ctx, admin, kernelDomainRole, kernelQueueRole, dbName); err != nil {
		t.Fatal(err)
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema:     "river",
		DomainRole: kernelDomainRole,
		QueueRole:  kernelQueueRole,
	}); err != nil {
		t.Fatal(err)
	}
	domain, err := pgxpool.New(ctx, kernelRoleURI(t, instance.URI, kernelDomainRole, kernelDomainPassword))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)
	queue, err := pgxpool.New(ctx, kernelRoleURI(t, instance.URI, kernelQueueRole, kernelQueuePassword))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(queue.Close)
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, kernelDomainRole, "river"); err != nil {
		t.Fatalf("domain authorization: %v", err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, kernelQueueRole, "river"); err != nil {
		t.Fatalf("queue authorization: %v", err)
	}
	client, err := river.NewClient(riverpgxv5.New(queue), &river.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	return activeActiveHarness{admin: admin, domain: domain, queue: queue, client: client}
}

func (harness activeActiveHarness) riverJobCount(t *testing.T, ctx context.Context) int {
	t.Helper()
	var count int
	if err := harness.admin.QueryRow(ctx, "SELECT count(*) FROM river.river_job").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func (harness activeActiveHarness) outboxStatuses(
	t *testing.T,
	ctx context.Context,
) map[string]string {
	t.Helper()
	rows, err := harness.admin.Query(
		ctx,
		"SELECT id::text, status FROM public.sync_dispatch_outbox ORDER BY id",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	statuses := map[string]string{}
	for rows.Next() {
		var id, status string
		if err := rows.Scan(&id, &status); err != nil {
			t.Fatal(err)
		}
		statuses[id] = status
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return statuses
}

func TestMutationReconcilerActiveActive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	harness := startActiveActiveHarness(t, ctx)

	// TestActiveActiveReplicasClaimDisjointWork: several reconciler replicas run
	// the same window against the same outbox. Every row must be published
	// exactly once, and no two replicas may publish the same row: that is what
	// makes active/active safe rather than merely tolerated.
	t.Run("replicas claim disjoint work and publish each row once", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, harness.admin)
		now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
		const rows = 12
		identifiers := make([]string, 0, rows)
		for index := 0; index < rows; index++ {
			id := fmt.Sprintf("00000000-0000-4000-8000-0000000034%02d", index)
			identifiers = append(identifiers, id)
			seedKernelOutbox(t, ctx, harness.admin, id, now.Add(-time.Second))
		}

		var (
			mu        sync.Mutex
			published = map[string]int{}
		)
		publisher := func(publisherCtx context.Context, tx pgx.Tx, claim TransportClaim) (string, error) {
			mu.Lock()
			published[claim.ID]++
			mu.Unlock()
			inserted, err := harness.client.InsertTx(
				publisherCtx, tx, kernelRiverArgs{OutboxID: claim.ID}, &river.InsertOpts{Queue: "sync"},
			)
			if err != nil {
				return "", err
			}
			return strconv.FormatInt(inserted.Job.ID, 10), nil
		}

		const replicas = 4
		errs := make([]error, replicas)
		var finished sync.WaitGroup
		release := make(chan struct{})
		finished.Add(replicas)
		for index := 0; index < replicas; index++ {
			kernel, err := NewKernel(
				harness.domain, harness.queue,
				riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun),
				KernelModeMutation,
			)
			if err != nil {
				t.Fatal(err)
			}
			go func(index int, kernel *Kernel) {
				defer finished.Done()
				<-release
				// Each replica drains until it observes an empty window, which
				// is how a real loop behaves and what exposes a claim that was
				// leaked rather than published.
				for attempt := 0; attempt < rows+2; attempt++ {
					result, stepErr := kernel.Step(ctx, now, 3, time.Minute, publisher, nil)
					if stepErr != nil {
						errs[index] = stepErr
						return
					}
					if result.Claimed == 0 {
						return
					}
				}
			}(index, kernel)
		}
		close(release)
		finished.Wait()

		for index, err := range errs {
			if err != nil {
				t.Fatalf("replica %d: %v", index, err)
			}
		}
		for _, id := range identifiers {
			if count := published[id]; count != 1 {
				t.Fatalf("outbox row %s was published %d times", id, count)
			}
		}
		if jobs := harness.riverJobCount(t, ctx); jobs != rows {
			t.Fatalf("River holds %d jobs for %d outbox rows", jobs, rows)
		}
		for id, status := range harness.outboxStatuses(t, ctx) {
			if status != "dispatched" {
				t.Fatalf("outbox row %s ended in status %q", id, status)
			}
		}
	})

	// The publish/mark crash window: the reconciler inserted a River job and
	// died before the outbox row was marked dispatched. Because the insert and
	// the mark share one transaction, the crash must leave neither, and the
	// re-drive must produce exactly one job rather than a duplicate.
	t.Run("crash between publish and mark re-drives without duplicating", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, harness.admin)
		now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
		seedKernelOutbox(t, ctx, harness.admin, integrationDispatchID, now.Add(-time.Second))

		kernel, err := NewKernel(
			harness.domain, harness.queue,
			riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun),
			KernelModeMutation,
		)
		if err != nil {
			t.Fatal(err)
		}
		crash := errors.New("reconciler crashed after River insert")
		_, stepErr := kernel.Step(
			ctx, now, 1, time.Minute,
			func(publisherCtx context.Context, tx pgx.Tx, claim TransportClaim) (string, error) {
				if _, insertErr := harness.client.InsertTx(
					publisherCtx, tx, kernelRiverArgs{OutboxID: claim.ID}, &river.InsertOpts{Queue: "sync"},
				); insertErr != nil {
					return "", insertErr
				}
				// The process dies here, after the insert but before the mark.
				return "", crash
			},
			nil,
		)
		// A publisher failure is recorded as a retryable publish failure rather
		// than aborting the window: one bad row must not stall the others. What
		// matters here is that nothing durable survived the crash.
		_ = stepErr
		if jobs := harness.riverJobCount(t, ctx); jobs != 0 {
			t.Fatalf("crashed publish left %d River jobs; the insert must roll back with the mark", jobs)
		}

		var redriven int
		for attempt := 0; attempt < 5; attempt++ {
			result, err := kernel.Step(
				ctx, now.Add(time.Duration(attempt+1)*time.Minute), 1, time.Minute,
				func(publisherCtx context.Context, tx pgx.Tx, claim TransportClaim) (string, error) {
					inserted, insertErr := harness.client.InsertTx(
						publisherCtx, tx, kernelRiverArgs{OutboxID: claim.ID}, &river.InsertOpts{Queue: "sync"},
					)
					if insertErr != nil {
						return "", insertErr
					}
					redriven++
					return strconv.FormatInt(inserted.Job.ID, 10), nil
				},
				nil,
			)
			if err != nil {
				t.Fatalf("re-drive attempt %d: %v", attempt, err)
			}
			if result.Dispatched > 0 {
				break
			}
		}
		if redriven != 1 {
			t.Fatalf("re-drive published %d times, want exactly one", redriven)
		}
		if jobs := harness.riverJobCount(t, ctx); jobs != 1 {
			t.Fatalf("re-drive left %d River jobs, want 1", jobs)
		}
		if status := harness.outboxStatuses(t, ctx)[integrationDispatchID]; status != "dispatched" {
			t.Fatalf("re-driven outbox row status = %q", status)
		}
	})

	// A kind whose durable transport route is still Celery must never be
	// claimed or published by the Go reconciler. This is the property that lets
	// the two route planes transition independently: promoting one kind cannot
	// drag another off Celery.
	t.Run("celery-routed kinds are never claimed", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, harness.admin)
		now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
		seedKernelOutboxKind(
			t, ctx, harness.admin, integrationStaleID,
			syncdispatchcontract.KindFinalizeSyncRun, now.Add(-time.Second),
		)
		kernel, err := NewKernel(
			harness.domain, harness.queue,
			riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun),
			KernelModeMutation,
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := kernel.Step(
			ctx, now, 5, time.Minute,
			func(context.Context, pgx.Tx, TransportClaim) (string, error) {
				t.Fatal("a Celery-routed kind reached the River publisher")
				return "", nil
			},
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if result.Claimed != 0 || result.Dispatched != 0 {
			t.Fatalf("Step() = %+v for a Celery-routed kind", result)
		}
		if status := harness.outboxStatuses(t, ctx)[integrationStaleID]; status != "pending" {
			t.Fatalf("Celery-routed outbox row status = %q, want it untouched", status)
		}
		if jobs := harness.riverJobCount(t, ctx); jobs != 0 {
			t.Fatalf("a Celery-routed kind produced %d River jobs", jobs)
		}
	})
}
