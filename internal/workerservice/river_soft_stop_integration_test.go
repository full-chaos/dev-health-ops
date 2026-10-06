//go:build integration

package workerservice

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
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

// startSoftStopProcess builds the river process THROUGH newRiverWorkerProcess,
// the builder a production binary uses, so the last link (family.softStop ->
// riverWorkerClientConfig) is exercised too, and starts it the way the runtime
// does: with the signal context.
func startSoftStopProcess(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, probe *softStopProbe, softStop time.Duration, queue string,
) riverWorkerProcess {
	t.Helper()
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, &softStopWorker{probe: probe}); err != nil {
		t.Fatal(err)
	}
	database := &postgresWorkerDatabase{pools: &postgresstore.RuntimePools{QueueControl: pool}}
	family := workerFamily{
		queues:   []jobruntime.QueueBudget{{Queue: queue, MaxWorkers: 2}},
		softStop: softStop,
	}
	component, err := newRiverWorkerProcess(
		config.Config{WorkerInstanceID: uuid.NewString(), RiverDatabaseSchema: "river", PreclaimReadinessTimeout: 30 * time.Second},
		database, workers, family, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	process, ok := component.(riverWorkerProcess)
	if !ok {
		t.Fatalf("component = %T", component)
	}
	if err := process.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return process
}

// startSoftStopControl is the defect on main: the shipped client configuration
// with no soft stop. newRiverWorkerProcess now refuses that, so the control
// builds the client directly.
func startSoftStopControl(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, probe *softStopProbe, queue string,
) *river.Client[pgx.Tx] {
	t.Helper()
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, &softStopWorker{probe: probe}); err != nil {
		t.Fatal(err)
	}
	clientConfig := riverWorkerClientConfig(
		config.Config{WorkerInstanceID: uuid.NewString(), RiverDatabaseSchema: "river"},
		map[string]river.QueueConfig{queue: {MaxWorkers: 2}},
		workers, slog.New(slog.NewTextHandler(io.Discard, nil)), 0,
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
		client := startSoftStopControl(t, startCtx, pool, probe, queue)
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
		process := startSoftStopProcess(t, startCtx, pool, probe, time.Minute, queue)
		if _, err := process.client.Insert(ctx, softStopArgs{Name: "running"}, &river.InsertOpts{Queue: queue}); err != nil {
			t.Fatal(err)
		}
		waitStarted(t, probe, "running")
		signal()
		// Inserted AFTER the signal: River starts no NEW fetch once stopping, so a
		// pod in minute 119 of its drain does not pick up a 2 h job. "Not claimed"
		// is asserted only for a job that appears after any fetch already in flight
		// at the signal has landed: River runs such a fetch under WithoutCancel
		// (producer.go:818), so at most that one fetch can still deliver jobs, and
		// they run inside the soft stop. The pause is what keeps the assertion
		// deterministic instead of racing that fetch.
		time.Sleep(2 * time.Second)
		if _, err := process.client.Insert(ctx, softStopArgs{Name: "late"}, &river.InsertOpts{Queue: queue}); err != nil {
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
		if err := process.Shutdown(stopCtx); err != nil {
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
		// What the component gets: Stop keeps the drain budget and the soft stop
		// ends workerReleaseBuffer before it.
		{"stop budget = soft stop + the release buffer", softStop + workerReleaseBuffer, true},
		// The plant for the buffer (buffer 0): River cancels the job at the very
		// moment Stop gives up, so the release write is cut off.
		{"stop budget equal to the soft stop cuts the release off", softStop, false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			probe := newSoftStopProbe(releaseWork)
			queue := "q" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
			startCtx, signal := context.WithCancel(ctx)
			process := startSoftStopProcess(t, startCtx, pool, probe, softStop, queue)
			if _, err := process.client.Insert(ctx, softStopArgs{Name: "slow"}, &river.InsertOpts{Queue: queue}); err != nil {
				t.Fatal(err)
			}
			waitStarted(t, probe, "slow")
			signalledAt := time.Now()
			signal()
			stopCtx, stop := context.WithTimeout(context.Background(), testCase.stopBudget)
			defer stop()
			_ = process.Shutdown(stopCtx)
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

// TestUnsetShutdownTimeoutRuntimeOutlivesTheDefaultAndReleases (CHAOS-8783 r1 P1,
// reproduced by gwc-round): with --shutdown-timeout UNSET the composition
// derives grace, drain budget and soft stop from the selected queues, and the
// runtime the shell builds must run on that same derived value. Real River and
// Postgres, the real composition, the real shell.RuntimeShutdownTimeout and the
// real lifecycle runtime. A running job outlives the 30 s package default after
// the stop signal and finishes normally; the runtime returns only after it.
//
// Plant: shell.RuntimeShutdownTimeout returns the configured value (the
// pre-fix shell.go): the runtime gives up at 30 s with the job still running.
func TestUnsetShutdownTimeoutRuntimeOutlivesTheDefaultAndReleases(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	pool := riverFixturePool(t, ctx)
	probe := newSoftStopProbe(0)

	t.Chdir(filepath.Join("..", ".."))
	runtimeRegistry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	queues := []string{"coverage", "heartbeat", "retention", "webhooks"}
	sources := productionWorkerDependencySources
	sources.contractRoot = defaultContractRoot
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) { return &fakeWorkerDatabase{}, nil }
	sources.loadRuntimeRegistry = func(string) (*jobruntime.Registry, error) { return runtimeRegistry, nil }
	sources.buildReports, sources.buildDaily, sources.buildProviderSync = nil, nil, nil
	sources.buildSyncCoordinator, sources.buildWorkgraph = nil, nil
	concurrency := map[string]int{}
	budgets := make([]jobruntime.QueueBudget, 0, len(queues))
	for _, queue := range queues {
		concurrency[queue] = 1
		budgets = append(budgets, jobruntime.QueueBudget{Queue: queue, MaxWorkers: 1})
	}
	sources.buildOperational = fakeHandlerBuilder("ops", mustSelectedQueueSpecs(t, runtimeRegistry, queues...), budgets...)
	// The REAL process builder, over the real queue pool, with the probe worker.
	sources.buildRiverProcess = func(
		cfg config.Config, _ workerDatabase, workers *river.Workers, family workerFamily, logger *slog.Logger,
	) (lifecycle.Component, error) {
		if err := river.AddWorkerSafely(workers, &softStopWorker{probe: probe}); err != nil {
			return nil, err
		}
		database := &postgresWorkerDatabase{pools: &postgresstore.RuntimePools{QueueControl: pool}}
		return newRiverWorkerProcess(cfg, database, workers, family, logger)
	}
	cfg := config.Config{
		Queues: queues, WorkerQueueConcurrency: concurrency, WorkerGroup: "ops",
		ShutdownTimeout: config.DefaultShutdownTimeout, ShutdownTimeoutExplicit: false, // the flag is UNSET
		RiverDatabaseSchema: "river", WorkerInstanceID: uuid.NewString(),
		DomainDatabaseMaxConns: 4, QueueDatabaseMaxConns: 2, PreclaimReadinessTimeout: 30 * time.Second,
	}
	components, err := configureWorkerDependenciesWithSources(ctx, cfg, health.NewRegistry(time.Second), sources)
	if err != nil {
		t.Fatal(err)
	}
	riverWorkers, ok := components[len(components)-1].(workerProcessComponent)
	if !ok {
		t.Fatalf("last component = %T", components[len(components)-1])
	}
	timeout := shell.RuntimeShutdownTimeout(cfg.ShutdownTimeout, components)
	if want := 900*time.Second + workerFinalizationBuffer + workerReleaseBuffer; timeout != want || timeout <= config.DefaultShutdownTimeout {
		t.Fatalf("runtime shutdown timeout = %s, want the derived %s (not the %s default)", timeout, want, config.DefaultShutdownTimeout)
	}

	runtime, err := lifecycle.New(lifecycle.Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), ShutdownTimeout: timeout,
		// The composition's own budgets and grace; only the worker-presence
		// heartbeat (a fake database here) is left out.
		Components: []lifecycle.Component{workerProcessComponent{
			components: riverWorkers.components, budget: riverWorkers.budget, grace: riverWorkers.grace,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	signalCtx, signal := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(signalCtx) }()

	process := riverWorkers.components[0].(riverWorkerProcess)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := process.client.Insert(ctx, softStopArgs{Name: "long"}, &river.InsertOpts{Queue: "heartbeat"}); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("could not insert the job")
		}
		time.Sleep(200 * time.Millisecond)
	}
	select {
	case got := <-probe.started:
		if got != "long" {
			t.Fatalf("job %q started", got)
		}
	case err := <-runDone:
		t.Fatalf("the runtime returned early: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("job \"long\" never started")
	}
	signalledAt := time.Now()
	signal()
	// The job needs longer than the 30 s default after the signal.
	time.AfterFunc(33*time.Second, func() { close(probe.release) })

	select {
	case err := <-runDone:
		returnedAfter := time.Since(signalledAt)
		probe.mu.Lock()
		finishedAt, finished := probe.finished["long"]
		_, cancelled := probe.cancelledAt["long"]
		probe.mu.Unlock()
		if err != nil {
			t.Fatalf("runtime returned %v after %s", err, returnedAfter)
		}
		if cancelled || !finished || returnedAfter < 33*time.Second || finishedAt.After(time.Now()) {
			t.Fatalf("runtime returned after %s: job finished=%v cancelled=%v; it must outlive the %s default and return only after the job",
				returnedAfter, finished, cancelled, config.DefaultShutdownTimeout)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("the runtime never returned")
	}
}
