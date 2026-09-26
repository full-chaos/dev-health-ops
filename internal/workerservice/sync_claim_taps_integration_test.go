//go:build integration

package workerservice

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// CHAOS-6883: the `sync` queue's workers (dispatch, finalize, post-sync, reference
// discovery, team autoimport, ownership derivation) are bare River workers, not
// jobruntime.Adapter, so the claim-liveness taps (HandlerInvoked/HandlerReturned)
// never fired for them. execution_liveness then read every busy sync queue as
// wedged: inHandler stayed 0 (every running job "outside a handler") and the claim
// clock was never refreshed, so a backlog with running work turned readiness red
// on a healthy worker (prod, go-sync, 2026-09-26).
//
// Every bare kind on the queue is driven (dispatch, finalize, post-sync,
// reference discovery, team autoimport, ownership derivation): the checked-in
// contract routes autoimport to river, so the composition registers it.
//
// This drives the PRODUCTION composition (composeSelectedWorkerFamilies with the
// production claimLivenessObserver, the same call configureWorkerDependencies
// makes) and a real River client against a real PostgreSQL row, then asks the
// claim tracker whether the job that just ran left claim evidence.
func TestSyncCoordinatorWorkersFeedClaimLiveness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
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
	clickhouse, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickhouse.Close(context.Background()) })
	valkey, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valkey.Close(context.Background()) })

	domain, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	queuePool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		domain.Close()
		t.Fatal(err)
	}
	database := &postgresWorkerDatabase{pools: &postgresstore.RuntimePools{Domain: domain, QueueControl: queuePool}}
	t.Cleanup(database.Close)

	const queue = "sync"
	cfg := validGitHubWorkItemsRuntimeConfig(t)
	cfg.Service = "dev-health-worker"
	cfg.Queues = []string{queue}
	cfg.WorkerQueueConcurrency = map[string]int{queue: 1}
	cfg.WorkerInstanceID = uuid.NewString()
	cfg.RiverDatabaseSchema = "river"
	cfg.ClickHouseURI = secrets.NewValue(clickhouse.URI)
	cfg.ValkeyURI = secrets.NewValue(valkey.URI)
	cfg.SettingsEncryptionKey = secrets.NewValue("sync-claim-taps-encryption-key")
	cfg.OperationalBridgeTimeout = 20 * time.Second
	registryTree, err := jobruntime.Load(filepath.Join("contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := buildWorkerMetrics(ctx, cfg, registryTree)
	if err != nil {
		t.Fatal(err)
	}

	// The tracker, seeded long ago: any evidence it now holds came from a job.
	claim := newClaimLiveness(time.Now().Add(-time.Hour), []string{queue})
	observer := &tapRecorder{
		claimLivenessObserver: claimLivenessObserver{MetricsCollector: metrics, liveness: claim},
		invoked:               map[string]int{}, returned: map[string]int{},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	workers := river.NewWorkers()
	family, err := composeSelectedWorkerFamilies(
		ctx, cfg, database, registryTree, observer, logger, workers, productionWorkerDependencySources,
	)
	if err != nil {
		t.Fatalf("the sync family did not compose: %v", err)
	}
	t.Cleanup(func() { _ = closeWorkerFamily(family) })

	const clientID = "sync-claim-taps"
	client, err := river.NewClient(riverpgxv5.New(queuePool), &river.Config{
		FetchCooldown:     100 * time.Millisecond,
		FetchPollInterval: 100 * time.Millisecond,
		ID:                clientID,
		Logger:            logger,
		Queues:            map[string]river.QueueConfig{queue: {MaxWorkers: 1}},
		Schema:            "river",
		TestOnly:          true,
		Workers:           workers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		_ = client.StopAndCancel(stopCtx)
	})

	transport := func() syncdispatchruntime.TransportArgs {
		return syncdispatchruntime.TransportArgs{
			Version: syncdispatchruntime.ContractVersionV1, OrgID: uuid.NewString(), RunID: uuid.NewString(),
			DispatchOutbox: uuid.NewString(), DeliveryAttempt: 1, RouteGeneration: 1,
		}
	}
	runID := uuid.NewString()
	inserts := map[string]river.JobArgs{
		syncdispatchcontract.KindDispatchSyncRun:    syncdispatchruntime.DispatchSyncRunArgs{TransportArgs: transport()},
		syncdispatchcontract.KindFinalizeSyncRun:    syncdispatchruntime.FinalizeSyncRunArgs{TransportArgs: transport()},
		syncdispatchcontract.KindPostSync:           syncdispatchruntime.PostSyncArgs{TransportArgs: transport()},
		syncdispatchcontract.KindReferenceDiscovery: syncdispatchruntime.ReferenceDiscoveryArgs{TransportArgs: transport()},
		jobcontract.KindTeamAutoimport: syncdispatchruntime.TeamAutoimportJobArgs{
			Version: jobcontract.ContractVersionV1, OrgID: uuid.NewString(), CorrelationID: "corr-sync-taps-autoimport",
			Idempotency: "sync-taps-autoimport", Domain: jobcontract.DomainLink{Type: "sync_run", ID: runID},
			Payload: jobcontract.TeamAutoimportPayload{SyncRunID: runID},
		},
		jobcontract.KindTeamRepoOwnershipDerivation: syncdispatchruntime.TeamRepoOwnershipDerivationJobArgs{
			Version: jobcontract.ContractVersionV1, OrgID: uuid.NewString(), CorrelationID: "corr-sync-taps",
			Idempotency: "sync-taps-derivation", Domain: jobcontract.DomainLink{Type: "sync_run", ID: runID},
			Payload: jobcontract.TeamRepoOwnershipDerivationPayload{SyncRunID: runID},
		},
	}
	for kind, args := range inserts {
		if _, err := client.Insert(ctx, args, &river.InsertOpts{Queue: queue}); err != nil {
			t.Fatalf("insert %s: %v", kind, err)
		}
	}
	// Every kind must actually be worked by a real River client: wait for the
	// worker to report each one.
	deadline := time.Now().Add(90 * time.Second)
	for {
		observer.mu.Lock()
		missing := []string{}
		for kind := range inserts {
			if observer.invoked[kind] == 0 {
				missing = append(missing, kind)
			}
		}
		observer.mu.Unlock()
		if len(missing) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no HandlerInvoked tap for sync kinds %v: the bare sync workers bypass the claim-liveness taps, "+
				"so execution_liveness reads a busy sync queue as wedged (invoked=%v)", missing, observer.invoked)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The returns balance the invocations once the running jobs settle.
	for {
		observer.mu.Lock()
		balanced := true
		for kind, n := range observer.invoked {
			if observer.returned[kind] < n {
				balanced = false
			}
		}
		observer.mu.Unlock()
		if balanced && claim.handlersInside(queue) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("HandlerReturned never balanced HandlerInvoked: invoked=%v returned=%v inside=%d",
				observer.invoked, observer.returned, claim.handlersInside(queue))
		}
		time.Sleep(100 * time.Millisecond)
	}

	if since := claim.since(queue, time.Now()); since > 30*time.Second {
		t.Fatalf("a dispatch_sync_run job ran on queue %q but left no claim evidence (last claim %s ago): "+
			"the sync workers bypass the claim-liveness taps, so execution_liveness reads a busy sync queue as wedged",
			queue, since.Round(time.Second))
	}
}

// tapRecorder is the production observer plus a record of which kinds reached a
// handler, so the test can tell every kind's tap apart on one shared queue.
type tapRecorder struct {
	claimLivenessObserver
	mu       sync.Mutex
	invoked  map[string]int
	returned map[string]int
}

func (r *tapRecorder) HandlerInvoked(ctx context.Context, labels jobruntime.JobLabels) {
	r.mu.Lock()
	r.invoked[labels.Kind]++
	r.mu.Unlock()
	r.claimLivenessObserver.HandlerInvoked(ctx, labels)
}

func (r *tapRecorder) HandlerReturned(ctx context.Context, labels jobruntime.JobLabels) {
	r.mu.Lock()
	r.returned[labels.Kind]++
	r.mu.Unlock()
	r.claimLivenessObserver.HandlerReturned(ctx, labels)
}
