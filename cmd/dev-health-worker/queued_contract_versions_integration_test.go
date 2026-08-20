//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// queuedKind is one row this deployment can legitimately leave in a River
// queue, and the plane that declares it.
type queuedKind struct {
	queue   string
	kind    string
	version int
	plane   string
}

// TestQueuedContractVersionsReadinessResolvesEveryPlane is the CHAOS-3938
// regression, and the guard that makes the class impossible rather than fixing
// one kind.
//
// It enumerates BOTH route planes that can put rows in river.river_job -- the
// bounded jobs registry and the sync-dispatch transport routes -- inserts one
// state='available' row for every kind at its declared contract version, and
// then runs the production startup readiness check against that non-empty
// queue. A kind that any plane can enqueue but the check cannot resolve fails
// this test the day it is added, which is what a single registry line for
// dispatch_sync_run would not have done.
//
// A NON-EMPTY queue is the whole point: the bug only exists in that state, and
// every earlier restart survived purely because the queue happened to be empty
// at that moment.
func TestQueuedContractVersionsReadinessResolvesEveryPlane(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)

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

	registry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	queued := everyQueuedKind(t, registry)
	if len(queued) == 0 {
		t.Fatal("enumerated no queued kinds; the assertions below would be vacuous")
	}
	queues := queuesOf(queued)

	for _, entry := range queued {
		insertAvailableRiverJob(t, ctx, admin, entry.queue, entry.kind, entry.version)
	}
	assertAvailableRows(t, ctx, admin, len(queued))

	if err := queuedContractVersionsReadiness(t, ctx, postgres.URI, queues); err != nil {
		t.Fatalf("startup readiness refused with every plane's kinds queued: %v", err)
	}

	// Mutation control. A row whose contract version is genuinely outside the
	// declared window must still refuse, and the refusal must name the queue,
	// the kind, and the version -- production got only
	// "failed_checks=queued_contract_versions" and could not tell which.
	insertAvailableRiverJob(t, ctx, admin, syncdispatchcontract.RiverQueue,
		syncdispatchcontract.KindDispatchSyncRun, 99)
	err = queuedContractVersionsReadiness(t, ctx, postgres.URI, queues)
	var unsupported *riverstore.UnsupportedContractVersionError
	if !errors.As(err, &unsupported) {
		t.Fatalf("a genuinely unsupported contract version was accepted: %v", err)
	}
	want := syncdispatchcontract.RiverQueue + "/" + syncdispatchcontract.KindDispatchSyncRun + "@99"
	if !containsString(unsupported.Offenders, want) {
		t.Fatalf("refusal named %v, want it to name %q", unsupported.Offenders, want)
	}
}

// everyQueuedKind unions the two planes. Both halves are derived from the
// checked-in contracts, so a kind added to either one is covered here without
// a second edit.
func everyQueuedKind(t *testing.T, registry *jobruntime.Registry) []queuedKind {
	t.Helper()
	queued := make([]queuedKind, 0, len(registry.Descriptors()))
	for _, descriptor := range registry.Descriptors() {
		queued = append(queued, queuedKind{
			queue:   descriptor.Queue,
			kind:    descriptor.Kind,
			version: descriptor.CurrentVersion,
			plane:   "worker_job_routes",
		})
	}
	for _, occupancy := range syncdispatchruntime.RiverQueueOccupancy() {
		for _, version := range occupancy.SupportedVersions {
			queued = append(queued, queuedKind{
				queue:   occupancy.Queue,
				kind:    occupancy.Kind,
				version: version,
				plane:   "sync_dispatch_transport_routes",
			})
		}
	}
	return queued
}

func queuesOf(queued []queuedKind) []string {
	seen := make(map[string]struct{}, len(queued))
	queues := make([]string, 0, len(queued))
	for _, entry := range queued {
		if _, ok := seen[entry.queue]; ok {
			continue
		}
		seen[entry.queue] = struct{}{}
		queues = append(queues, entry.queue)
	}
	sort.Strings(queues)
	return queues
}

// queuedContractVersionsReadiness runs the production startup check. Only the
// pool source is substituted: the registry load, the telemetry configuration,
// and the check itself are the shipped code paths.
func queuedContractVersionsReadiness(
	t *testing.T,
	ctx context.Context,
	postgresURI string,
	queues []string,
) error {
	t.Helper()
	domain, err := pgxpool.New(ctx, postgresURI)
	if err != nil {
		t.Fatal(err)
	}
	queuePool, err := pgxpool.New(ctx, postgresURI)
	if err != nil {
		domain.Close()
		t.Fatal(err)
	}
	database := &postgresWorkerDatabase{
		pools: &postgresstore.RuntimePools{Domain: domain, QueueControl: queuePool},
	}
	t.Cleanup(database.Close)

	concurrency := make(map[string]int, len(queues))
	for _, queue := range queues {
		concurrency[queue] = 1
	}
	cfg := config.Config{
		Service:                "dev-health-worker",
		Queues:                 append([]string(nil), queues...),
		WorkerQueueConcurrency: concurrency,
		RiverDatabaseSchema:    "river",
		// Left unset so the drain budget is derived from the selected kinds'
		// longest timeout, exactly as an operator who configured nothing gets.
	}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.newRiverClientID = func() string { return uuid.NewString() }

	dependencies := buildWorkerDependencies(ctx, cfg, sources)
	dependencies.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	if dependencies.startupErr != nil {
		t.Fatalf("worker dependencies did not compose: %v", dependencies.startupErr)
	}
	if !dependencies.queueTelemetryRequired {
		t.Fatal("queue telemetry was not required; the readiness check would be a no-op")
	}
	if dependencies.queueTelemetryErr != nil {
		t.Fatalf("queue telemetry sampler was not constructed: %v", dependencies.queueTelemetryErr)
	}
	return dependencies.queueTelemetry.CheckAvailableContractVersions(ctx)
}

func insertAvailableRiverJob(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	queue string,
	kind string,
	version int,
) {
	t.Helper()
	args, err := json.Marshal(map[string]any{"contract_version": version})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(
		ctx,
		`INSERT INTO river.river_job (state, max_attempts, args, kind, queue, scheduled_at)
		 VALUES ('available', 3, $1::jsonb, $2, $3, now() - interval '1 minute')`,
		string(args), kind, queue,
	); err != nil {
		t.Fatalf("insert available %s/%s: %v", queue, kind, err)
	}
}

func assertAvailableRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int) {
	t.Helper()
	var available int
	if err := pool.QueryRow(
		ctx, `SELECT count(*) FROM river.river_job WHERE state = 'available'`,
	).Scan(&available); err != nil {
		t.Fatal(err)
	}
	if available != want {
		t.Fatalf("queue holds %d available rows, want %d; the readiness check "+
			"only inspects state='available' so an empty queue proves nothing", available, want)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
