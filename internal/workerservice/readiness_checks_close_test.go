package workerservice

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

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
