package workerservice

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// blockingReadinessRun returns a check run that signals its start, then holds until its context
// ends, counting each end in ended.
func blockingReadinessRun(ended *atomic.Int32) (func(context.Context) error, <-chan struct{}) {
	started := make(chan struct{})
	return func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		ended.Add(1)
		return ctx.Err()
	}, started
}

func warmBlockingProbe(probe *postgres.LazyProbeCheck, name string, ended *atomic.Int32) {
	run, started := blockingReadinessRun(ended)
	probe.Warm(name, run, postgres.PostureCheckOptions{RunTimeout: time.Hour})
	<-started
}

func warmBlockingPosture(ended *atomic.Int32) *postgres.CachedPostureCheck {
	run, started := blockingReadinessRun(ended)
	check := postgres.NewCachedRunCheck("domain_postgres", run, postgres.PostureCheckOptions{RunTimeout: time.Hour})
	check.Warm()
	<-started
	return check
}

// CHAOS-8654: the warmed readiness checks ran on context.Background() and logged after the process
// had shut down. Close stops every check the database holds, and returns only once each running
// execution has ended, before it closes the pools those checks run on.
func TestWorkerDatabaseCloseStopsItsReadinessChecksFirst(t *testing.T) {
	var ended atomic.Int32
	database := &postgresWorkerDatabase{}
	warmBlockingProbe(&database.queueProbe, "queue_postgres", &ended)
	warmBlockingProbe(&database.riverSchemaProbe, "river_schema", &ended)
	database.postures = append(database.postures, warmBlockingPosture(&ended))

	database.Close()
	if got := ended.Load(); got != 3 {
		t.Fatalf("Close returned with %d of 3 readiness checks ended", got)
	}
}

// The queued-contract-versions census runs on the queue pool but lives outside the database, so the
// database lifecycle stops it too, before the pools close.
func TestWorkerDatabaseLifecycleShutdownStopsTheContractVersionsCensus(t *testing.T) {
	var ended atomic.Int32
	database := &postgresWorkerDatabase{}
	warmBlockingProbe(&database.queueProbe, "queue_postgres", &ended)
	var contractVersions postgres.LazyProbeCheck
	warmBlockingProbe(&contractVersions, "queued_contract_versions", &ended)

	component := workerDatabaseLifecycle{database: database, contractVersions: &contractVersions}
	if err := component.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := ended.Load(); got != 2 {
		t.Fatalf("Shutdown returned with %d of 2 readiness checks ended", got)
	}
}

func TestWorkerDependenciesCloseStopsTheContractVersionsCensus(t *testing.T) {
	var ended atomic.Int32
	dependencies := &workerDependencies{}
	warmBlockingProbe(&dependencies.contractVersionsProbe, "queued_contract_versions", &ended)

	dependencies.close()
	if got := ended.Load(); got != 1 {
		t.Fatal("close returned while the queued-contract-versions census still ran")
	}
}

// countingWriter counts the log lines written to it.
type countingWriter struct{ lines *atomic.Int32 }

func (w countingWriter) Write(p []byte) (int, error) {
	w.lines.Add(1)
	return len(p), nil
}

// blockingWriter holds the first log write until release closes.
type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w blockingWriter) Write(p []byte) (int, error) {
	select {
	case <-w.entered:
	default:
		close(w.entered)
	}
	<-w.release
	return len(p), nil
}

func refusingDomainPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://review:x@127.0.0.1/review?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("in-memory dial refused")
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// CHAOS-8654 r1: Close seals the domain posture list, so a check asked for after Close answers
// ErrUnavailable and runs nothing on the closed pool.
func TestWorkerDatabaseDomainPostureCheckAfterCloseRunsNothing(t *testing.T) {
	var lines atomic.Int32
	logger := slog.New(slog.NewTextHandler(countingWriter{&lines}, nil))
	database := &postgresWorkerDatabase{pools: &postgres.RuntimePools{Domain: refusingDomainPool(t)}, domainRole: "review", riverSchema: "river"}
	database.Close()
	check := database.DomainPostureCheck(logger)
	if check == nil {
		t.Fatal("DomainPostureCheck after Close = nil; a nil check makes the caller fall back to a direct query")
	}
	if err := check(context.Background()); !errors.Is(err, postgres.ErrUnavailable) {
		t.Fatalf("check after Close = %v, want ErrUnavailable", err)
	}
	if got := lines.Load(); got != 0 {
		t.Fatalf("a check asked for after Close logged %d execution(s)", got)
	}
}

// A second Close returns only after the first has stopped every check: the first holds the domain
// posture snapshot, so a second that skipped the wait would close the pools under a running check.
func TestWorkerDatabaseSecondCloseWaitsForTheFirst(t *testing.T) {
	database := &postgresWorkerDatabase{pools: &postgres.RuntimePools{Domain: refusingDomainPool(t)}, domainRole: "review", riverSchema: "river"}
	entered, release := make(chan struct{}), make(chan struct{})
	var released atomic.Bool
	held := slog.New(slog.NewTextHandler(blockingWriter{entered: entered, release: release}, nil))
	check := database.DomainPostureCheck(held)
	go func() { _ = check(context.Background()) }()
	<-entered

	first := make(chan struct{})
	go func() {
		database.Close()
		close(first)
	}()
	for sealed := false; !sealed; runtime.Gosched() {
		database.posturesMu.Lock()
		sealed = database.posturesClosed
		database.posturesMu.Unlock()
	}
	secondSawRelease := make(chan bool, 1)
	go func() {
		database.Close()
		secondSawRelease <- released.Load()
	}()
	for range 100 {
		runtime.Gosched()
	}
	released.Store(true)
	close(release)
	<-first
	if !<-secondSawRelease {
		t.Fatal("the second Close returned while the first was still stopping the domain posture check")
	}
}

// The runtime releases the database owner it never started (a cancelled start), so the warmed
// readiness checks end before Run returns.
func TestWorkerDatabaseWarmedChecksEndWhenTheRuntimeNeverStartsTheOwner(t *testing.T) {
	var ended atomic.Int32
	database := &postgresWorkerDatabase{}
	warmBlockingProbe(&database.queueProbe, "queue_postgres", &ended)
	warmBlockingProbe(&database.riverSchemaProbe, "river_schema", &ended)
	engine, err := lifecycle.New(lifecycle.Options{ShutdownTimeout: time.Second, Components: []lifecycle.Component{workerDatabaseLifecycle{database: database}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := engine.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want context.Canceled", err)
	}
	if got := ended.Load(); got != 2 {
		t.Fatalf("Run returned with %d of 2 warmed checks ended", got)
	}
}
