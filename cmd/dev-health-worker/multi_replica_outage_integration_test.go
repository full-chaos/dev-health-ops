//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// databaseOutageWindow is the simulated domain-database outage. It is longer
// than the presence TTL's renewal interval and long enough for several budget
// and idempotency renewal attempts to fail, which is the point: none of those
// failures may terminalize a job or kill a process.
// It stays well inside the parked job's 30s contract timeout: a handler held
// past that legitimately exhausts its single attempt, which would prove
// nothing about lease handling.
const databaseOutageWindow = 15 * time.Second

// workerPresenceTTLProof is longer than the 30s presence TTL, so surviving
// presence rows prove renewal is still running rather than merely un-expired.
const workerPresenceTTLProof = 35 * time.Second

// TestMultiReplicaFleetSurvivesDatabaseOutage is the CHAOS-3864/3865/3866/3873
// lane gate. Two replicas run a queue group with jobs in flight, the domain
// database disappears for 15 seconds, and the fleet must come back with zero
// terminalized jobs, no process exit, and a clean drain.
//
// Before this lane, each of those three failures fired on its own: one failed
// budget-lease renewal cancelled the handler, the resulting cancellation was
// stamped "terminal" so the River retry was auto-cancelled forever, and the
// first failed presence heartbeat was fatal to the process.
func TestMultiReplicaFleetSurvivesDatabaseOutage(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)

	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })

	// The admin pool bypasses the proxy so the test can still observe state
	// while the workers' database link is severed.
	admin, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	prepareMultiReplicaDatabase(t, ctx, admin)

	outage := startDatabaseCutover(t, postgres.URI)
	bridge := newMultiReplicaBridge()
	server := httptest.NewServer(http.HandlerFunc(bridge.serveHTTP))
	t.Cleanup(server.Close)
	registry, err := jobruntime.Load(filepath.Join("contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	first := newOperationalReplica(t, ctx, outage.uri, server.URL, registry, logger)
	second := newOperationalReplica(t, ctx, outage.uri, server.URL, registry, logger)
	t.Cleanup(func() { first.close(t); second.close(t) })
	first.start(t, ctx)
	second.start(t, ctx)
	assertWorkerPresence(t, ctx, admin, "operations-shared",
		[]string{"coverage", "heartbeat", "retention", "webhooks"}, 2, 0)

	firstJob := insertHeartbeat(t, ctx, first.client, 1)
	secondJob := insertHeartbeat(t, ctx, first.client, 2)
	// One handler is parked inside the bridge for the whole outage; the other
	// is claimed by the sibling replica.
	bridge.waitForFirst(t)
	waitFor(t, 30*time.Second, func() (bool, error) {
		row, err := readRiverJob(ctx, admin, firstJob.Job.ID)
		return err == nil && len(row.AttemptedBy) == 1, err
	})

	outage.cut()
	time.Sleep(databaseOutageWindow)
	outage.restore()

	// Presence is observability-only, so a failed heartbeat must never reach
	// lifecycle.Runtime, which treats any component error as fatal. One error
	// here is a fleet-wide restart in production.
	for _, replica := range []*operationalReplica{first, second} {
		select {
		case err := <-replica.presence.Errors():
			t.Fatalf("presence reported a fatal error during the outage: %v", err)
		default:
		}
	}

	bridge.release()
	waitForCompletedAfterOutage(t, ctx, admin, firstJob.Job.ID, secondJob.Job.ID)

	// Zero terminalized jobs: no domain run may be stamped terminal, and no
	// River job may have been cancelled or discarded.
	var terminalRuns int
	if err := admin.QueryRow(ctx,
		`SELECT count(*)::integer FROM public.worker_job_runs WHERE status = 'terminal'`,
	).Scan(&terminalRuns); err != nil {
		t.Fatal(err)
	}
	if terminalRuns != 0 {
		t.Fatalf("database outage terminalized %d job runs, want 0", terminalRuns)
	}
	var cancelled int
	if err := admin.QueryRow(ctx,
		`SELECT count(*)::integer FROM river.river_job WHERE state IN ('cancelled', 'discarded')`,
	).Scan(&cancelled); err != nil {
		t.Fatal(err)
	}
	if cancelled != 0 {
		t.Fatalf("database outage cancelled or discarded %d River jobs, want 0", cancelled)
	}

	// Each logical job produced exactly one external effect despite the outage.
	if effects := bridge.effects(); effects[heartbeatSchedule(1)] != 1 || effects[heartbeatSchedule(2)] != 1 {
		t.Fatalf("effects = %#v, want exactly one per logical job", effects)
	}

	// The fleet is still alive. Waiting past the presence TTL before asserting
	// is what makes this meaningful: a renewal loop that exited during the
	// outage would leave rows that are merely not yet expired, and only a
	// still-running renewer keeps them live this long. This runs after the
	// jobs finish so no handler is parked past its 30s contract timeout.
	time.Sleep(workerPresenceTTLProof)
	assertWorkerPresence(t, ctx, admin, "operations-shared",
		[]string{"coverage", "heartbeat", "retention", "webhooks"}, 2, 0)

	// ...and the fleet drains cleanly.
	first.stopAndCancel(t, ctx)
	second.stopAndCancel(t, ctx)
}

// databaseCutover is a TCP proxy in front of PostgreSQL. Cutting it severs
// every worker connection and refuses new ones, which is what a failover or a
// PgBouncer restart looks like to the worker: the container keeps running, so
// the test can still observe state through a direct admin pool.
type databaseCutover struct {
	uri      string
	target   string
	listener net.Listener

	mu    sync.Mutex
	open  bool
	conns []net.Conn
}

func startDatabaseCutover(t *testing.T, upstream string) *databaseCutover {
	t.Helper()
	parsed, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxied := *parsed
	proxied.Host = listener.Addr().String()
	cutover := &databaseCutover{
		uri: proxied.String(), target: parsed.Host, listener: listener, open: true,
	}
	t.Cleanup(func() { _ = listener.Close() })
	go cutover.serve()
	return cutover
}

func (cutover *databaseCutover) serve() {
	for {
		client, err := cutover.listener.Accept()
		if err != nil {
			return
		}
		cutover.mu.Lock()
		open := cutover.open
		cutover.mu.Unlock()
		if !open {
			_ = client.Close()
			continue
		}
		upstream, err := net.Dial("tcp", cutover.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		cutover.mu.Lock()
		cutover.conns = append(cutover.conns, client, upstream)
		cutover.mu.Unlock()
		go func() { _, _ = io.Copy(upstream, client); _ = upstream.Close() }()
		go func() { _, _ = io.Copy(client, upstream); _ = client.Close() }()
	}
}

// completerRetryCeiling bounds how long River's completer can take to land a
// completion whose first attempt failed, which is exactly what a mid-flight
// database outage produces. From river@v0.40.0
// internal/jobcompleter/job_completer.go, above `const numRetries = 3`:
//
//	As configured, total time asleep from initial attempt is ~7 seconds
//	(1 + 2 + 4) (not including jitter). However, if each attempt times out,
//	that's up to ~37 seconds (7 seconds + 3 * 10 seconds).
//
// The shared waitForCompleted uses 30s, which is fine for the non-outage
// multi-replica test where no completion ever fails -- but it sits BELOW this
// ceiling, so this test could fail while River was still correctly retrying.
// That is what it did on CI: the sibling replica's job stayed `running` with
// no error recorded, the signature of a completion still inside the retry
// loop rather than a lost one.
//
// Measuring locally does not bound this: both jobs completed 0.3s after
// release there, because whether the outage window actually covers the sibling
// job's completion is timing-dependent. When it does not, no retry happens at
// all and the wait is irrelevant; when it does, the full ceiling is in play.
//
// The budget is deliberately above the ceiling rather than at it. If a job is
// still incomplete after this, the completer has given up and the job is
// stranded until the rescuer reclaims it -- a real finding this test should
// fail on, not wait out.
const completerRetryCeiling = 90 * time.Second

func waitForCompletedAfterOutage(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobIDs ...int64,
) {
	t.Helper()
	deadline := time.Now().Add(completerRetryCeiling)
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
	t.Fatalf(
		"jobs still incomplete %s after release, past River's completer retry ceiling: %#v",
		completerRetryCeiling, snapshots,
	)
}

func (cutover *databaseCutover) cut() {
	cutover.mu.Lock()
	defer cutover.mu.Unlock()
	cutover.open = false
	for _, conn := range cutover.conns {
		_ = conn.Close()
	}
	cutover.conns = nil
}

func (cutover *databaseCutover) restore() {
	cutover.mu.Lock()
	defer cutover.mu.Unlock()
	cutover.open = true
}
