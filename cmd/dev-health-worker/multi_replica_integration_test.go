//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

const multiReplicaRetryAttempts = 3

func TestExplicitQueueMultiReplicaClaimDrainRestart(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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

	// The heartbeat's fleet-wide concurrency budget (registry.json: scope
	// "fleet", limit 1) is enforced by jobruntime's Acquire loop BEFORE the
	// handler ever runs (internal/jobruntime/budget_postgres.go): the loser
	// polls every 100ms and stays in River's "running" state the whole time
	// it waits, bounded only by the job's own 30s contract deadline -- it
	// never reaches this handler at all until it actually wins the lease.
	// So exactly one replica's telemetry receiver ever gets a request during
	// the parked phase below, and river_job.state cannot tell the two jobs
	// apart (both legitimately read "running" for as long as one is parked).
	// Each of first/second gets its OWN receiver so identity comes from
	// WHICH receiver fired, never from decoding the request.
	firstBridge := newMultiReplicaBridge()
	firstServer := httptest.NewServer(http.HandlerFunc(firstBridge.serveHTTP))
	t.Cleanup(firstServer.Close)
	secondBridge := newMultiReplicaBridge()
	secondServer := httptest.NewServer(http.HandlerFunc(secondBridge.serveHTTP))
	t.Cleanup(secondServer.Close)
	registry, err := jobruntime.Load(filepath.Join("contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	first := newOperationalReplica(t, ctx, postgres.URI, firstServer.URL, registry, logger)
	second := newOperationalReplica(t, ctx, postgres.URI, secondServer.URL, registry, logger)
	t.Cleanup(func() { first.close(t); second.close(t) })
	assertReplicaQueueParity(t, first, second)
	first.start(t, ctx)
	second.start(t, ctx)
	assertWorkerPresence(t, ctx, admin, "operations-shared", []string{"coverage", "heartbeat", "retention", "webhooks"}, 2, 0)

	firstHeartbeat := insertHeartbeat(t, ctx, first.client, 1)
	secondHeartbeat := insertHeartbeat(t, ctx, first.client, 2)

	var activeReplica, waitingReplica *operationalReplica
	var activeBridge *multiReplicaBridge
	select {
	case <-firstBridge.firstStarted:
		activeReplica, waitingReplica = first, second
		activeBridge = firstBridge
	case <-secondBridge.firstStarted:
		activeReplica, waitingReplica = second, first
		activeBridge = secondBridge
	case <-time.After(20 * time.Second):
		t.Fatal("no heartbeat reached either replica's telemetry receiver")
	}
	// Insertion order names neither job's claimant: River hands each of
	// job1/job2 to whichever replica polls it first, independent of which
	// bridge that replica owns. The bridge signal above only says WHICH
	// REPLICA is active; which of the two logical jobs that replica is
	// running still has to come from AttemptedBy, not from an assumed
	// 1:1 pairing with insertion order.
	var activeJobID, waitingJobID int64
	waitFor(t, 20*time.Second, func() (bool, error) {
		firstRow, err := readRiverJob(ctx, admin, firstHeartbeat.Job.ID)
		if err != nil {
			return false, err
		}
		secondRow, err := readRiverJob(ctx, admin, secondHeartbeat.Job.ID)
		if err != nil {
			return false, err
		}
		if len(firstRow.AttemptedBy) != 1 || len(secondRow.AttemptedBy) != 1 {
			return false, nil
		}
		switch activeReplica.client.ID() {
		case firstRow.AttemptedBy[0]:
			activeJobID, waitingJobID = firstHeartbeat.Job.ID, secondHeartbeat.Job.ID
		case secondRow.AttemptedBy[0]:
			activeJobID, waitingJobID = secondHeartbeat.Job.ID, firstHeartbeat.Job.ID
		default:
			return false, nil
		}
		return waitingReplica.client.ID() == firstRow.AttemptedBy[0] ||
			waitingReplica.client.ID() == secondRow.AttemptedBy[0], nil
	})
	if activeJobID == 0 {
		t.Fatal("neither heartbeat job's AttemptedBy matched the replica whose receiver fired")
	}
	waitingReplica.stopAndCancel(t, ctx)

	// restarted's receiver must NOT park its first request: by the time the
	// waiting job's retry lands here, the contention phase is over and it
	// should complete like any ordinary heartbeat.
	restartedBridge := newUnblockedMultiReplicaBridge()
	restartedServer := httptest.NewServer(http.HandlerFunc(restartedBridge.serveHTTP))
	t.Cleanup(restartedServer.Close)
	restarted := newOperationalReplica(t, ctx, postgres.URI, restartedServer.URL, registry, logger)
	t.Cleanup(func() { restarted.close(t) })
	assertReplicaQueueParity(t, activeReplica, restarted)
	restarted.start(t, ctx)
	assertWorkerPresence(t, ctx, admin, "operations-shared", []string{"coverage", "heartbeat", "retention", "webhooks"}, 2, 0)
	waitFor(t, 20*time.Second, func() (bool, error) {
		row, err := readRiverJob(ctx, admin, waitingJobID)
		return err == nil && row.Attempt >= 2 && slices.Contains(row.AttemptedBy, restarted.client.ID()), err
	})
	activeBridge.release()
	waitForCompleted(t, ctx, admin, firstHeartbeat.Job.ID, secondHeartbeat.Job.ID)

	if total := activeBridge.total() + restartedBridge.total(); total != 2 {
		t.Fatalf("heartbeat telemetry POSTs = %d, want exactly one per logical job (2 total)", total)
	}
	retried, err := readRiverJob(ctx, admin, waitingJobID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Attempt < 2 || len(retried.AttemptedBy) < 2 ||
		retried.AttemptedBy[0] != waitingReplica.client.ID() ||
		!slices.Contains(retried.AttemptedBy[1:], restarted.client.ID()) {
		t.Fatalf("cancelled claim attribution = %#v", retried)
	}

	if _, err := admin.Exec(ctx, `
		INSERT INTO public.provider_rate_limit_observations (
			id, org_id, provider, integration_id, sync_run_id, sync_run_unit_id, observed_at
		) VALUES ($1, 'org-1', 'github', $2, $2, $2, $3)`,
		uuid.New(), uuid.New(), time.Now().UTC().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := restarted.client.QueuePause(ctx, "retention", nil); err != nil {
		t.Fatal(err)
	}
	retentionJobs := []int64{
		insertRetention(t, ctx, restarted.client, 1).Job.ID,
		insertRetention(t, ctx, restarted.client, 2).Job.ID,
	}
	waitFor(t, 10*time.Second, func() (bool, error) {
		count, err := countRiverStates(ctx, admin, retentionJobs, "available", "scheduled")
		return count == 2, err
	})
	activeStopped := activeReplica.client.Stopped()
	activeReplica.stopGracefully(t, ctx)
	select {
	case <-activeStopped:
	default:
		t.Fatal("production drain returned before its River client stopped")
	}
	got, err := countRiverStates(ctx, admin, retentionJobs, "available", "scheduled")
	if err != nil {
		t.Fatal(err)
	}
	if got != 2 {
		t.Fatalf("work remaining after one replica stopped = %d, want 2", got)
	}
	if err := restarted.client.QueueResume(ctx, "retention", nil); err != nil {
		t.Fatal(err)
	}
	waitForCompleted(t, ctx, admin, retentionJobs...)
	for _, jobID := range retentionJobs {
		row, err := readRiverJob(ctx, admin, jobID)
		if err != nil {
			t.Fatal(err)
		}
		if len(row.AttemptedBy) != 1 || row.AttemptedBy[0] != restarted.client.ID() {
			t.Fatalf("drained job %d attribution = %v", jobID, row.AttemptedBy)
		}
	}
	assertWorkerPresence(t, ctx, admin, "operations-shared", []string{"coverage", "heartbeat", "retention", "webhooks"}, 1, 0)

	var claims, successfulClaims, effectRows, remainingLeases int
	if err := admin.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE status = 'succeeded' AND attempt_count = 1) FROM public.worker_job_runs`).Scan(&claims, &successfulClaims); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM public.multi_replica_retention_effects`).Scan(&effectRows); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM public.worker_concurrency_leases`).Scan(&remainingLeases); err != nil {
		t.Fatal(err)
	}
	measuredJobs := 2 + len(retentionJobs)
	if claims != measuredJobs || successfulClaims != measuredJobs || effectRows != 1 || remainingLeases != 0 {
		t.Fatalf("durable results claims=%d successful=%d effects=%d leases=%d measured=%d",
			claims, successfulClaims, effectRows, remainingLeases, measuredJobs)
	}
	assertFleetMetrics(t, first, second, restarted)
	writeMultiReplicaProof(t, measuredJobs)
}

type operationalReplica struct {
	database *postgresWorkerDatabase
	family   workerFamily
	process  riverWorkerProcess
	metrics  *jobruntime.MetricsCollector
	presence *jobruntime.WorkerPresence
	client   *river.Client[pgx.Tx]
	group    string
	queues   []string
	active   bool
}

func newOperationalReplica(
	t *testing.T,
	ctx context.Context,
	postgresURI string,
	telemetryURL string,
	registry *jobruntime.Registry,
	logger *slog.Logger,
) *operationalReplica {
	t.Helper()
	domain, err := pgxpool.New(ctx, postgresURI)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := pgxpool.New(ctx, postgresURI)
	if err != nil {
		domain.Close()
		t.Fatal(err)
	}
	database := &postgresWorkerDatabase{pools: &postgresstore.RuntimePools{Domain: domain, QueueControl: queue}}
	instanceID := uuid.NewString()
	cfg := config.Config{
		Service: "dev-health-worker", Queues: []string{"coverage", "heartbeat", "retention", "webhooks"}, WorkerInstanceID: instanceID,
		WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
		RiverDatabaseSchema:    "river",
		// The heartbeat's phone-home effect is native Go now: TelemetryEndpoint
		// (not OperationalBridgeURL/Token, which no kind on this queue set
		// still uses) is what the test's fake receiver below observes.
		TelemetryEndpoint:        telemetryURL,
		TelemetryInstanceID:      "multi-replica-instance",
		OperationalBridgeTimeout: 20 * time.Second,
	}
	metrics, err := buildWorkerMetrics(ctx, cfg, registry)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	workers := river.NewWorkers()
	family, err := buildOperationalWorker(cfg, database, registry, metrics, logger, workers)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	component, err := newRiverWorkerProcess(cfg, database, workers, family, logger)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	process, ok := component.(riverWorkerProcess)
	if !ok || process.client == nil {
		database.Close()
		t.Fatal("production explicit-queue builder did not construct its River process")
	}
	presence, err := jobruntime.NewWorkerPresence(domain, "operations-shared", cfg.Queues, instanceID)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if process.client.ID() != instanceID {
		database.Close()
		t.Fatalf("River client identity = %q", process.client.ID())
	}
	return &operationalReplica{
		database: database, family: family, process: process,
		metrics: metrics, presence: presence, client: process.client,
		group: "operations-shared", queues: append([]string(nil), cfg.Queues...),
	}
}

func (replica *operationalReplica) start(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := replica.presence.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := replica.process.Start(ctx); err != nil {
		_ = replica.presence.Shutdown(ctx)
		t.Fatal(err)
	}
	replica.active = true
}

func (replica *operationalReplica) stopGracefully(t *testing.T, ctx context.Context) {
	t.Helper()
	if !replica.active {
		return
	}
	if err := replica.presence.BeginDrain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := replica.process.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := replica.presence.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	replica.active = false
}

func (replica *operationalReplica) stopAndCancel(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := replica.presence.BeginDrain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := replica.client.StopAndCancel(ctx); err != nil {
		t.Fatal(err)
	}
	if err := closeWorkerFamily(replica.family); err != nil {
		t.Fatal(err)
	}
	if err := replica.presence.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	replica.active = false
}

func (replica *operationalReplica) close(t *testing.T) {
	t.Helper()
	if replica == nil {
		return
	}
	if replica.active {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := replica.process.Shutdown(ctx); err != nil {
			t.Errorf("stop replica: %v", err)
		}
		if err := replica.presence.Shutdown(ctx); err != nil {
			t.Errorf("remove replica presence: %v", err)
		}
		cancel()
		replica.active = false
	}
	if replica.database != nil {
		replica.database.Close()
		replica.database = nil
	}
}

func assertReplicaQueueParity(t *testing.T, replicas ...*operationalReplica) {
	t.Helper()
	wantKinds := []string{
		jobcontract.KindBillingNotification,
		jobcontract.KindHeartbeat,
		jobcontract.KindRetentionCleanup,
		jobcontract.KindSyncCoverageRefresh,
		jobcontract.KindWebhookDelivery,
	}
	slices.Sort(wantKinds)
	for _, replica := range replicas {
		gotKinds := make([]string, 0, len(replica.family.handlers))
		for _, handler := range replica.family.handlers {
			gotKinds = append(gotKinds, handler.Kind)
		}
		slices.Sort(gotKinds)
		if !slices.Equal(gotKinds, wantKinds) {
			t.Fatalf("production operational coverage = %v, want %v", gotKinds, wantKinds)
		}
	}
	if !slices.Equal(replicas[0].family.queues, replicas[1].family.queues) {
		t.Fatalf("replica queue sets differ: %#v %#v", replicas[0].family.queues, replicas[1].family.queues)
	}
	for _, replica := range replicas {
		if replica.process.client != replica.client {
			t.Fatal("worker process did not retain its single River client")
		}
		if !slices.Equal(replica.queues, []string{"coverage", "heartbeat", "retention", "webhooks"}) {
			t.Fatalf("selected queues = %v", replica.queues)
		}
		if replica.group != "operations-shared" {
			t.Fatalf("worker group = %q", replica.group)
		}
	}
}

type riverJobSnapshot struct {
	State       string
	Attempt     int
	AttemptedBy []string
	Errors      string
}

func readRiverJob(ctx context.Context, pool *pgxpool.Pool, jobID int64) (riverJobSnapshot, error) {
	var snapshot riverJobSnapshot
	err := pool.QueryRow(ctx, `
		SELECT state::text, attempt, attempted_by, COALESCE(errors::text, '')
		FROM river.river_job WHERE id = $1`, jobID,
	).Scan(&snapshot.State, &snapshot.Attempt, &snapshot.AttemptedBy, &snapshot.Errors)
	return snapshot, err
}

func waitForCompleted(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobIDs ...int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		count, err := countRiverStates(ctx, pool, jobIDs, "completed")
		if err != nil {
			t.Fatal(err)
		}
		if count == len(jobIDs) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	snapshots := make(map[int64]riverJobSnapshot, len(jobIDs))
	for _, jobID := range jobIDs {
		snapshot, err := readRiverJob(ctx, pool, jobID)
		if err != nil {
			t.Fatal(err)
		}
		snapshots[jobID] = snapshot
	}
	t.Fatalf("timed out waiting for completed jobs: %#v", snapshots)
}

func countRiverStates(
	ctx context.Context,
	pool *pgxpool.Pool,
	jobIDs []int64,
	states ...string,
) (int, error) {
	var count int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM river.river_job
		WHERE id = ANY($1::bigint[]) AND state::text = ANY($2::text[])`, jobIDs, states,
	).Scan(&count)
	return count, err
}

func waitFor(t *testing.T, timeout time.Duration, ready func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok, err := ready()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for multi-replica state")
}

func insertHeartbeat(
	t *testing.T,
	ctx context.Context,
	client *river.Client[pgx.Tx],
	ordinal int,
) *rivertype.JobInsertResult {
	t.Helper()
	result, err := client.Insert(ctx, jobruntime.HeartbeatArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.HeartbeatPayload]{
		ContractVersion: 1,
		CorrelationID:   fmt.Sprintf("multi-replica-heartbeat-%d", ordinal),
		IdempotencyKey:  fmt.Sprintf("multi-replica:heartbeat:%d", ordinal),
		Domain: jobcontract.DomainLink{
			Type: "schedule_occurrence", ID: fmt.Sprintf("00000000-0000-4000-8000-%012d", ordinal),
		},
		Payload: jobcontract.HeartbeatPayload{ScheduledFor: heartbeatSchedule(ordinal)},
	}}, &river.InsertOpts{Queue: "heartbeat", Priority: 2, MaxAttempts: multiReplicaRetryAttempts})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func heartbeatSchedule(ordinal int) string {
	return time.Date(2026, 8, 15, 12, ordinal, 0, 0, time.UTC).Format(time.RFC3339)
}

func insertRetention(
	t *testing.T,
	ctx context.Context,
	client *river.Client[pgx.Tx],
	ordinal int,
) *rivertype.JobInsertResult {
	t.Helper()
	result, err := client.Insert(ctx, jobruntime.RetentionCleanupArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.RetentionCleanupPayload]{
		ContractVersion: 2,
		CorrelationID:   fmt.Sprintf("multi-replica-retention-%d", ordinal),
		IdempotencyKey:  fmt.Sprintf("multi-replica:retention:%d", ordinal),
		Domain: jobcontract.DomainLink{
			Type: "maintenance_run", ID: fmt.Sprintf("00000000-0000-4000-9000-%012d", ordinal),
		},
		Payload: jobcontract.RetentionCleanupPayload{
			BatchSize: 10, DeleteBefore: time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339),
			RetentionPolicy: jobcontract.RetentionRateLimitObservations,
		},
	}}, &river.InsertOpts{Queue: "retention", Priority: 3, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// multiReplicaBridge stands in for the heartbeat's phone-home receiver
// (TELEMETRY_ENDPOINT) now that the effect is native Go
// (internal/jobs/system/heartbeat_native.go) with no HTTP call into the
// Python API on the way there. It carries no job/schedule identity in its
// request body -- the native dispatcher's outbound payload is the
// phone-home snapshot (instance/version/counts/tier/...), not a job
// reference, matching the Python body it replaced -- so ordering is
// established by which request arrives FIRST, not by decoding which
// schedule it belongs to.
type multiReplicaBridge struct {
	firstStarted chan struct{}
	releaseFirst chan struct{}
	first        sync.Once
	releaseOnce  sync.Once
	mu           sync.Mutex
	count        int
	// parked is true for exactly as long as the first handler is blocked
	// inside serveHTTP below, waiting on releaseFirst or its own request
	// context. A caller that depends on "the first handler is still parked"
	// (CHAOS-4235's forced-contention setup) should assert stillParked()
	// rather than infer it from elapsed time.
	parked atomic.Bool
}

func newMultiReplicaBridge() *multiReplicaBridge {
	return &multiReplicaBridge{
		firstStarted: make(chan struct{}, 1), releaseFirst: make(chan struct{}),
	}
}

// newUnblockedMultiReplicaBridge returns a bridge that never parks a
// request: its once-only block slot is pre-consumed with a no-op, so every
// real call always takes serveHTTP's non-blocking path. Used for a replica
// that only takes over work AFTER a contention phase is over, where parking
// its first request would wrongly stall the rest of the test.
func newUnblockedMultiReplicaBridge() *multiReplicaBridge {
	bridge := newMultiReplicaBridge()
	bridge.first.Do(func() {})
	return bridge
}

func (bridge *multiReplicaBridge) serveHTTP(output http.ResponseWriter, request *http.Request) {
	block := false
	bridge.first.Do(func() {
		block = true
		// parked must be true BEFORE the send: a receiver on firstStarted
		// (waitForFirst) can observe the value and a caller can then check
		// stillParked() before this goroutine gets scheduled again. Storing
		// after the send left a real window where that check saw the zero
		// value even though this handler was about to block (codex
		// adversarial review, CHAOS-4235).
		bridge.parked.Store(true)
		bridge.firstStarted <- struct{}{}
	})
	if block {
		defer bridge.parked.Store(false)
		select {
		case <-bridge.releaseFirst:
		case <-request.Context().Done():
			return
		}
	}
	bridge.mu.Lock()
	bridge.count++
	bridge.mu.Unlock()
	output.WriteHeader(http.StatusAccepted)
}

func (bridge *multiReplicaBridge) waitForFirst(t *testing.T) {
	t.Helper()
	select {
	case <-bridge.firstStarted:
	case <-time.After(20 * time.Second):
		t.Fatal("no heartbeat reached the native telemetry receiver")
	}
}

func (bridge *multiReplicaBridge) release() {
	bridge.releaseOnce.Do(func() { close(bridge.releaseFirst) })
}

// stillParked reports whether the first handler is, right now, still
// blocked inside serveHTTP. A caller that needs job 1's replica to still
// have no free worker slot must check this instead of assuming elapsed
// time was short enough (CHAOS-4235).
func (bridge *multiReplicaBridge) stillParked() bool {
	return bridge.parked.Load()
}

// total is the count of heartbeat telemetry POSTs the receiver has seen.
// Fleet-wide heartbeat concurrency is 1 and each logical schedule occurrence
// completes exactly once (proven independently by the River state/attempt
// assertions around each call site), so this reaching exactly the expected
// count is "one native effect per logical job", the same property the old
// per-schedule bucketed count proved when the request body carried a
// schedule to bucket by.
func (bridge *multiReplicaBridge) total() int {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	return bridge.count
}

func assertWorkerPresence(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	workerGroup string,
	queues []string,
	live, draining int,
) {
	t.Helper()
	summary, err := jobruntime.ReadWorkerPresence(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary) != 1 || summary[0].WorkerGroup != workerGroup ||
		!slices.Equal(summary[0].Queues, queues) || summary[0].Live != live || summary[0].Draining != draining {
		t.Fatalf("worker presence = %#v, want group=%s queues=%v live=%d draining=%d", summary, workerGroup, queues, live, draining)
	}
}

func assertFleetMetrics(t *testing.T, replicas ...*operationalReplica) {
	t.Helper()
	var output bytes.Buffer
	for _, replica := range replicas {
		if err := replica.metrics.WritePrometheus(&output); err != nil {
			t.Fatal(err)
		}
	}
	text := output.String()
	for _, sample := range []string{
		`worker_concurrency_budget_capacity{kind="system.heartbeat",scope="fleet"} 1`,
		`worker_job_cancellations_total{kind="system.heartbeat",reason="cancelled"} 1`,
		`worker_job_attempts_total{kind="system.heartbeat",result="success",error_category="none"}`,
	} {
		if !strings.Contains(text, sample) {
			t.Fatalf("fleet metrics lack %q", sample)
		}
	}
}

func writeMultiReplicaProof(t *testing.T, measuredJobs int) {
	t.Helper()
	if measuredJobs < 1 {
		t.Fatal("multi-replica gate measured zero jobs")
	}
	path := os.Getenv("DEV_HEALTH_MULTI_REPLICA_PROOF")
	if path == "" {
		return
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(measuredJobs)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func prepareMultiReplicaDatabase(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := pool.Exec(ctx, `CREATE SCHEMA river`); err != nil {
		t.Fatal(err)
	}
	migrator, err := rivermigrate.New(
		riverpgxv5.New(pool), &rivermigrate.Config{Logger: logger, Schema: "river"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY, job_kind text NOT NULL, idempotency_key text NOT NULL,
			org_id uuid NULL, domain_type text NOT NULL, domain_id uuid NOT NULL,
			status text NOT NULL, claim_token uuid NULL, lease_expires_at timestamptz NULL,
			attempt_count integer NOT NULL, started_at timestamptz NOT NULL,
			finished_at timestamptz NULL, result text NULL, error_category text NULL,
			created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
			UNIQUE (job_kind, idempotency_key)
		);
		CREATE TABLE public.worker_concurrency_leases (
			id uuid PRIMARY KEY, budget_key varchar(320) NOT NULL,
			job_kind varchar(96) NOT NULL, concurrency_scope varchar(16) NOT NULL,
			organization_id uuid NULL, owner_token uuid NOT NULL UNIQUE,
			lease_expires_at timestamptz NOT NULL, created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL
		);
		CREATE TABLE public.worker_instances (
			instance_id uuid PRIMARY KEY, worker_group varchar(64) NOT NULL,
			queues text NOT NULL CHECK (length(queues) > 2),
			state varchar(16) NOT NULL CHECK (state IN ('accepting', 'draining')),
			started_at timestamptz NOT NULL, heartbeat_at timestamptz NOT NULL,
			expires_at timestamptz NOT NULL
		);
		CREATE TABLE public.provider_rate_limit_observations (
			id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL,
			integration_id uuid NOT NULL, sync_run_id uuid NOT NULL,
			sync_run_unit_id uuid NOT NULL, observed_at timestamptz NOT NULL
		);
		CREATE TABLE public.multi_replica_retention_effects (
			observation_id uuid PRIMARY KEY, deleted_at timestamptz NOT NULL DEFAULT statement_timestamp()
		);
		-- Tables the native heartbeat's compute reads (internal/jobs/system/
		-- heartbeat_native.go queryHeartbeatFacts): counts and the org
		-- license lookup. Left empty on purpose -- an empty organizations
		-- table means no audit_logs row is attempted (matching
		-- phone_home_heartbeat's own org_id_for_audit=None branch), which
		-- this test does not otherwise need a schema for.
		CREATE TABLE public.organizations (id uuid PRIMARY KEY, name text NOT NULL);
		CREATE TABLE public.users (id uuid PRIMARY KEY, email text NOT NULL);
		CREATE TABLE public.org_licenses (
			id uuid PRIMARY KEY, org_id uuid NOT NULL,
			license_key text, tier text NOT NULL DEFAULT 'community'
		);
		CREATE FUNCTION public.record_multi_replica_retention_effect() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN
			INSERT INTO public.multi_replica_retention_effects (observation_id) VALUES (OLD.id);
			RETURN OLD;
		END $$;
		CREATE TRIGGER multi_replica_retention_effect
		AFTER DELETE ON public.provider_rate_limit_observations
		FOR EACH ROW EXECUTE FUNCTION public.record_multi_replica_retention_effect();
	`); err != nil {
		t.Fatal(err)
	}
}
