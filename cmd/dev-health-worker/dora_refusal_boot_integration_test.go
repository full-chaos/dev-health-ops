//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestDORARefusalDoesNotTakeDownTheRemainingFamily pins the R1 boot policy.
//
// An earlier version returned from buildDailyWorker when the native DORA
// executor refused, which left capacity, complexity, membership,
// recommendations and release-impact unregistered even though their own
// dependencies were healthy and they still run on the compatibility bridge. A
// transient ClickHouse inspection failure was therefore enough to take six
// working kinds offline -- one component's construction failure downing every
// healthy sibling.
//
// The policy is now: the refusal is scoped to the dora kind. This test exists
// because that is a POLICY, not an implementation detail, and policies drift
// silently. It asserts all three halves together, because any one alone can
// hold while the policy is broken:
//
//  1. dora is NOT registered  (fail-closed: it must not compute wrong numbers)
//  2. the other kinds ARE     (the blast radius is one kind, not the family)
//  3. the refusal is COUNTED  (absence is not a signal -- a metric that never
//     moves cannot be distinguished from a quiet day)
func TestDORARefusalDoesNotTakeDownTheRemainingFamily(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	// A contract-2 store, read under contract 1: the mismatch the guard exists
	// to catch, produced by real migrations rather than a stubbed error, so
	// this test fails if the guard stops recognising the real thing.
	clickhouse := startContractTwoClickHouse(t, ctx)
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "1")

	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	prepareMultiReplicaDatabase(t, ctx, admin)

	domain, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)
	queue, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(queue.Close)
	database := &postgresWorkerDatabase{
		pools: &postgresstore.RuntimePools{Domain: domain, QueueControl: queue},
	}

	registry, err := jobruntime.Load(filepath.Join("contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Service:                  "dev-health-worker",
		Queues:                   []string{metricsQueue},
		RiverDatabaseSchema:      "river",
		OperationalBridgeURL:     "http://127.0.0.1:1/",
		OperationalBridgeToken:   secrets.NewValue("boot-test-token"),
		OperationalBridgeTimeout: 20 * time.Second,
		ClickHouseURI:            secrets.NewValue(clickhouse),
	}
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	family, err := buildDailyWorker(
		cfg, database, registry, collector, logger, river.NewWorkers())
	if err != nil {
		t.Fatalf(
			"a DORA-only refusal must not fail the whole family build -- the "+
				"other remaining kinds run on the compatibility bridge and "+
				"their dependencies are healthy: %v", err,
		)
	}

	registered := map[string]bool{}
	for _, spec := range family.handlers {
		registered[spec.Kind] = true
	}

	if registered[jobcontract.KindRemainingDORA] {
		t.Error(
			"dora was registered despite the native executor refusing. " +
				"Registering it would claim partitions and fail each one, " +
				"turning a clean not-served into a retry loop",
		)
	}

	// The siblings. If this list is ever empty the assertion above passes
	// vacuously, so require that we actually observed some.
	siblings := []string{
		jobcontract.KindRemainingCapacity,
		jobcontract.KindRemainingComplexity,
	}
	observed := 0
	for _, kind := range siblings {
		if registered[kind] {
			observed++
			continue
		}
		t.Errorf(
			"%s was not registered. A DORA-only fault must not take down a "+
				"sibling whose own dependencies are healthy", kind,
		)
	}
	if observed == 0 {
		t.Fatal(
			"no sibling kinds were registered at all, so the assertion above " +
				"proves nothing about blast radius",
		)
	}

	exposition := collector.PrometheusText()
	want := `worker_dora_native_refused_total{reason="ordering_contract_mismatch"} 1`
	if !strings.Contains(exposition, want) {
		t.Errorf(
			"the refusal produced no positive signal (%s). A flat "+
				"partitions counter is indistinguishable from a quiet day, so "+
				"without this an operator learns dora stopped only by noticing "+
				"an absence", want,
		)
	}
}

// startContractTwoClickHouse migrates a scratch store to the contract-2 schema
// and returns a DSN for it.
func startContractTwoClickHouse(t *testing.T, ctx context.Context) string {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })

	previous, had := os.LookupEnv("OPERATIONAL_ORDERING_CONTRACT")
	if err := os.Setenv("OPERATIONAL_ORDERING_CONTRACT", "2"); err != nil {
		t.Fatal(err)
	}
	chschema.Apply(ctx, t, instance)
	if had {
		_ = os.Setenv("OPERATIONAL_ORDERING_CONTRACT", previous)
	} else {
		_ = os.Unsetenv("OPERATIONAL_ORDERING_CONTRACT")
	}

	// Instance.URI, NOT ClickHouseHTTPDSN: the worker opens ClickHouse through
	// clickhousestore, which speaks the NATIVE protocol. Handing it the HTTP
	// port fails the handshake, and the family build would then refuse for a
	// reason that has nothing to do with the policy under test -- a green-for-
	// the-wrong-reason that would equally have "passed" had the policy been
	// broken. chschema uses the HTTP DSN internally; the two are not
	// interchangeable.
	return instance.URI
}
