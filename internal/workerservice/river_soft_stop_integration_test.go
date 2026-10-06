//go:build integration

package workerservice

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// softStopProbe records what one job sees while the worker is told to stop.
type softStopProbe struct {
	mu          sync.Mutex
	started     chan string
	release     chan struct{} // closed by the test to let a job finish normally
	cancelledAt map[string]time.Time
	released    map[string]time.Time // the "release write" of a cancelled job
	finished    map[string]time.Time
	releaseWork time.Duration // how long a cancelled job's release write takes
}

type softStopArgs struct {
	Name string `json:"name"`
}

func (softStopArgs) Kind() string { return "soft_stop.probe" }

type softStopWorker struct {
	river.WorkerDefaults[softStopArgs]
	probe *softStopProbe
}

func (worker *softStopWorker) Work(ctx context.Context, job *river.Job[softStopArgs]) error {
	probe := worker.probe
	probe.started <- job.Args.Name
	select {
	case <-probe.release:
	case <-ctx.Done():
		probe.mu.Lock()
		probe.cancelledAt[job.Args.Name] = time.Now()
		probe.mu.Unlock()
		// The release write of PR 1's requeue path: it must run to the end even
		// though the job context is cancelled, and it takes time.
		time.Sleep(probe.releaseWork)
		probe.mu.Lock()
		probe.released[job.Args.Name] = time.Now()
		probe.mu.Unlock()
		return ctx.Err()
	}
	probe.mu.Lock()
	probe.finished[job.Args.Name] = time.Now()
	probe.mu.Unlock()
	return nil
}

func newSoftStopProbe(releaseWork time.Duration) *softStopProbe {
	return &softStopProbe{
		started: make(chan string, 8), release: make(chan struct{}),
		cancelledAt: map[string]time.Time{}, released: map[string]time.Time{}, finished: map[string]time.Time{},
		releaseWork: releaseWork,
	}
}

func (probe *softStopProbe) cancelledTime(name string) (time.Time, bool) {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	at, ok := probe.cancelledAt[name]
	return at, ok
}

func startSoftStopClient(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, probe *softStopProbe, softStop time.Duration, queue string,
) *river.Client[pgx.Tx] {
	t.Helper()
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, &softStopWorker{probe: probe}); err != nil {
		t.Fatal(err)
	}
	// The REAL builder of the shipped client configuration.
	clientConfig := riverWorkerClientConfig(
		config.Config{WorkerInstanceID: uuid.NewString(), RiverDatabaseSchema: "river"},
		map[string]river.QueueConfig{queue: {MaxWorkers: 2}},
		workers, slog.New(slog.NewTextHandler(io.Discard, nil)), softStop,
	)
	client, err := river.NewClient(riverpgxv5.New(pool), clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return client
}

func riverFixturePool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })
	domainRole, err := containers.RoleName("soft_stop_domain", postgres)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("soft_stop_queue", postgres)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, domainRole, t.Logf) })
	t.Cleanup(func() { containers.DropRole(admin, queueRole, t.Logf) })
	for _, statement := range []string{
		"CREATE SCHEMA river",
		"CREATE ROLE " + domainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + reindexRolePassword + "'",
		"CREATE ROLE " + queueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + reindexRolePassword + "'",
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
	queuePool, err := pgxpool.New(ctx, reindexRoleURI(t, postgres.URI, queueRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(queuePool.Close)
	return queuePool
}

func waitStarted(t *testing.T, probe *softStopProbe, want string) {
	t.Helper()
	select {
	case got := <-probe.started:
		if got != want {
			t.Fatalf("job %q started, want %q", got, want)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("job %q never started", want)
	}
}

// TestRiverStopSignalIsSoft is CHAOS-8783 through the REAL client
// configuration builder and a real database: with a soft stop, cancelling the
// context the client was started with (the process's SIGTERM context) leaves a
// running job alive until it returns, and the client claims NO further job
// after the signal.
//
// The control case, soft stop 0, is the defect on main: the same signal
// cancels the running job's context at once.
func TestRiverStopSignalIsSoft(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	pool := riverFixturePool(t, ctx)

	t.Run("control: no soft stop cancels the running job at once", func(t *testing.T) {
		probe := newSoftStopProbe(0)
		queue := "q" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
		startCtx, signal := context.WithCancel(ctx)
		client := startSoftStopClient(t, startCtx, pool, probe, 0, queue)
		if _, err := client.Insert(ctx, softStopArgs{Name: "control"}, &river.InsertOpts{Queue: queue}); err != nil {
			t.Fatal(err)
		}
		waitStarted(t, probe, "control")
		signalledAt := time.Now()
		signal()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if at, ok := probe.cancelledTime("control"); ok {
				if at.Sub(signalledAt) > 2*time.Second {
					t.Fatalf("control job cancelled %s after the signal, want at once", at.Sub(signalledAt))
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("control: the running job's context was NOT cancelled by the stop signal; the harness cannot observe the defect")
			}
			time.Sleep(20 * time.Millisecond)
		}
		stopCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		_ = client.Stop(stopCtx)
	})

	t.Run("soft stop: the running job lives on and nothing new is claimed", func(t *testing.T) {
		probe := newSoftStopProbe(0)
		queue := "q" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
		startCtx, signal := context.WithCancel(ctx)
		client := startSoftStopClient(t, startCtx, pool, probe, time.Minute, queue)
		if _, err := client.Insert(ctx, softStopArgs{Name: "running"}, &river.InsertOpts{Queue: queue}); err != nil {
			t.Fatal(err)
		}
		waitStarted(t, probe, "running")
		signal()
		// Inserted AFTER the signal: a pod in minute 119 of its drain must not
		// pick up a 2 h job.
		time.Sleep(500 * time.Millisecond)
		if _, err := client.Insert(ctx, softStopArgs{Name: "late"}, &river.InsertOpts{Queue: queue}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3 * time.Second)
		if _, cancelled := probe.cancelledTime("running"); cancelled {
			t.Fatal("the running job was cancelled by the stop signal despite the soft stop")
		}
		select {
		case name := <-probe.started:
			t.Fatalf("job %q was claimed after the stop signal", name)
		default:
		}
		close(probe.release)
		stopCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := client.Stop(stopCtx); err != nil {
			t.Fatalf("Stop = %v after the running job finished", err)
		}
		probe.mu.Lock()
		_, finished := probe.finished["running"]
		probe.mu.Unlock()
		if !finished {
			t.Fatal("the running job did not finish normally")
		}
	})
}

// TestRiverSoftStopSpentCancelsAndStopWaitsForTheRelease: a job still running
// when the soft stop ends is cancelled, and Stop -- given the whole shutdown
// timeout, not only the soft stop -- waits for its release write.
func TestRiverSoftStopSpentCancelsAndStopWaitsForTheRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	pool := riverFixturePool(t, ctx)

	const (
		softStop    = 2 * time.Second
		releaseWork = 2 * time.Second
	)
	for _, testCase := range []struct {
		name         string
		stopBudget   time.Duration
		wantReleased bool
	}{
		// What the component gets after PR 2: the whole timeout = soft stop +
		// the finalization buffer.
		{"stop waits the whole shutdown timeout", softStop + 10*time.Second, true},
		// What it got before (the drain budget alone): River cancels the job at
		// the very moment Stop gives up, so the release write is cut off.
		{"stop budget equal to the soft stop cuts the release off", softStop, false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			probe := newSoftStopProbe(releaseWork)
			queue := "q" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
			startCtx, signal := context.WithCancel(ctx)
			client := startSoftStopClient(t, startCtx, pool, probe, softStop, queue)
			if _, err := client.Insert(ctx, softStopArgs{Name: "slow"}, &river.InsertOpts{Queue: queue}); err != nil {
				t.Fatal(err)
			}
			waitStarted(t, probe, "slow")
			signalledAt := time.Now()
			signal()
			stopCtx, stop := context.WithTimeout(context.Background(), testCase.stopBudget)
			defer stop()
			_ = client.Stop(stopCtx)
			probe.mu.Lock()
			cancelledAt, wasCancelled := probe.cancelledAt["slow"]
			_, released := probe.released["slow"]
			probe.mu.Unlock()
			if testCase.wantReleased {
				if !wasCancelled || cancelledAt.Sub(signalledAt) < softStop-200*time.Millisecond {
					t.Fatalf("cancelled=%v after %s, want a cancel only once the soft stop (%s) was spent",
						wasCancelled, cancelledAt.Sub(signalledAt), softStop)
				}
				if !released {
					t.Fatal("Stop returned before the cancelled job's release write finished")
				}
				return
			}
			if released {
				t.Fatal("control: the release write finished inside a Stop budget equal to the soft stop; the case proves nothing")
			}
		})
	}
}
