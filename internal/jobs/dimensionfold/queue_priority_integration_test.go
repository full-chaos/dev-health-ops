//go:build integration

package dimensionfold

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

type providerStandInArgs struct {
	Index int `json:"index"`
}

func (providerStandInArgs) Kind() string { return "test.provider_stand_in" }

type foldStandInArgs struct{}

func (foldStandInArgs) Kind() string { return "test.fold_stand_in" }

type completionLog struct {
	mu    sync.Mutex
	order []string
	fold  chan struct{}
}

func (log *completionLog) add(kind string) int {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.order = append(log.order, kind)
	return len(log.order)
}

type providerStandInWorker struct {
	river.WorkerDefaults[providerStandInArgs]
	log *completionLog
}

func (worker *providerStandInWorker) Work(ctx context.Context, _ *river.Job[providerStandInArgs]) error {
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	worker.log.add("provider")
	return nil
}

type foldStandInWorker struct {
	river.WorkerDefaults[foldStandInArgs]
	log *completionLog
}

func (worker *foldStandInWorker) Work(context.Context, *river.Job[foldStandInArgs]) error {
	worker.log.add("fold")
	close(worker.log.fold)
	return nil
}

// TestFoldIsClaimedAheadOfAProviderBacklog pins the fold's place on the
// shared sync_provider queue with the checked-in priorities: with a provider
// backlog far larger than the queue's two workers, the fold is claimed as
// soon as a worker frees, not after the backlog drains. Its worst-case wait is
// therefore one in-flight provider unit, not the queue's depth.
func TestFoldIsClaimedAheadOfAProviderBacklog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	registry, err := jobcontract.LoadRegistry("../../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	priority := map[string]int{}
	queue := map[string]string{}
	for _, job := range registry.Jobs {
		priority[job.Kind], queue[job.Kind] = job.Priority, job.Queue
	}
	if queue[jobcontract.KindDimensionFold] != queue[jobcontract.KindSyncProviderUnit] {
		t.Fatalf("fold queue %q differs from the provider queue %q; this test's premise changed",
			queue[jobcontract.KindDimensionFold], queue[jobcontract.KindSyncProviderUnit])
	}

	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := postgres.Close(context.Background()); err != nil {
			t.Errorf("close postgres: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, "CREATE SCHEMA river"); err != nil {
		t.Fatal(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}

	log := &completionLog{fold: make(chan struct{})}
	workers := river.NewWorkers()
	river.AddWorker(workers, &providerStandInWorker{log: log})
	river.AddWorker(workers, &foldStandInWorker{log: log})
	sharedQueue := queue[jobcontract.KindSyncProviderUnit]
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Schema:  "river",
		Queues:  map[string]river.QueueConfig{sharedQueue: {MaxWorkers: 2}},
		Workers: workers,
	})
	if err != nil {
		t.Fatal(err)
	}
	const backlog = 60
	params := make([]river.InsertManyParams, 0, backlog+1)
	for index := range backlog {
		params = append(params, river.InsertManyParams{
			Args: providerStandInArgs{Index: index},
			InsertOpts: &river.InsertOpts{
				Queue: sharedQueue, Priority: priority[jobcontract.KindSyncProviderUnit],
			},
		})
	}
	params = append(params, river.InsertManyParams{
		Args: foldStandInArgs{},
		InsertOpts: &river.InsertOpts{
			Queue: sharedQueue, Priority: priority[jobcontract.KindDimensionFold],
		},
	})
	if _, err := client.InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		_ = client.Stop(stopCtx)
	})
	select {
	case <-log.fold:
	case <-ctx.Done():
		t.Fatal("fold never ran")
	}
	log.mu.Lock()
	position := len(log.order)
	log.mu.Unlock()
	// Two workers may already hold provider units when the fold becomes the
	// best candidate, and a few more may complete in the same fetch cycle.
	if position > 5 {
		t.Fatalf("fold completed at position %d of a %d-job provider backlog", position, backlog)
	}
}
