//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
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
	// Wide enough that the outage-recovery backstop below, not this context, is
	// always what reports a fleet that never recovers: a context expiry here
	// would surface as an unrelated query error from the admin pool. The worst
	// passing path is setup plus the 15s outage, the full 4m budget, the 35s
	// presence proof and the drain, so this leaves several minutes of slack on
	// top of it. `go test -timeout` remains the outer guard.
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
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

// Recovering from a mid-flight database outage is a cascade of *scheduled*
// steps, not one operation:
//
//   - River's BatchCompleter gives up on a batch after three failed attempts
//     -- jittered 1 + 2 + 4s of backoff, and up to 10s per attempt, so ~38s
//     at worst -- then requeues that batch and starts over
//     (river@v0.40.0 internal/jobcompleter/job_completer.go, `numRetries`).
//     Until one of those rounds lands, the job sits in `running` with no
//     error recorded, which is the state CI reported.
//   - Only once that transition lands is the job rescheduled. Our adapter
//     supplies River's optional NextRetry, but jobruntime.NextRetryAt returns
//     a zero time for any contract whose retry_policy is not
//     "bounded_exponential_jitter" -- system.heartbeat's is "none" -- and
//     River falls back to its own schedule for a zero: `attempt^4` seconds
//     +/-10% jitter after the previous attempt (retry_policy.go
//     `DefaultClientRetryPolicy`). These jobs are inserted with
//     multiReplicaRetryAttempts, so only the ~1s and ~16s retries are
//     reachable -- a third failure is discarded, which the assertions below
//     already catch.
//   - The rescheduled handler then waits on our own fleet concurrency lease
//     (internal/jobruntime/budget_postgres.go), polling every 100ms inside
//     the job's 30s contract deadline, until the parked handler releases it.
//
// None of those intervals is a product promise about outage recovery, and how
// many of them are in play depends on exactly where inside the outage window
// each transition fell -- which is a function of runner speed, not of
// correctness. A wall-clock ceiling built out of them cannot tell "recovery
// did not happen" from "recovery has not happened yet", which is why the
// previous 90s ceiling reddened PRs whose diffs could not reach this package
// (CHAOS-3959).
//
// So this asserts convergence, and keeps exactly one deadline: a hang
// detector, set above the whole cascade rather than near any step in it. Those
// completer rounds only repeat for as long as the database is unreachable, and
// databaseOutageWindow fixes that at 15s, so at most one round of them can sit
// between two observable changes: the longest quiet gap a healthy cascade here
// can contain is ~38s. Four minutes is roughly six times that, which is the
// margin that makes this a hang detector rather than the old flake with a
// larger constant. A job that is genuinely stranded is not waited out: River's
// rescuer only reclaims after an hour, far outside this budget, so it fails
// here as it should.
const outageRecoveryBudget = 4 * time.Minute

// outageRecovery is everything about the jobs that a working cascade changes.
// Equality of the fingerprint is the "nothing moved" test; the snapshots are
// what a failure has to report.
type outageRecovery struct {
	snapshots   map[int64]riverJobSnapshot
	fingerprint string
	completed   int
}

func observeOutageRecovery(
	ctx context.Context, pool *pgxpool.Pool, jobIDs []int64,
) (outageRecovery, error) {
	observation := outageRecovery{snapshots: make(map[int64]riverJobSnapshot, len(jobIDs))}
	fingerprint := &strings.Builder{}
	for _, jobID := range jobIDs {
		snapshot, err := readRiverJob(ctx, pool, jobID)
		if err != nil {
			return outageRecovery{}, fmt.Errorf("read river job %d: %w", jobID, err)
		}
		observation.snapshots[jobID] = snapshot
		fmt.Fprintf(fingerprint, "%d=%#v;", jobID, snapshot)
		if snapshot.State == "completed" {
			observation.completed++
		}
	}
	observation.fingerprint = fingerprint.String()
	return observation, nil
}

// waitForCompletedAfterOutage waits for the released jobs to reach their
// terminal shape. The one thing it must never do is report a slow cascade as a
// broken one, so the deadline only bounds a hang -- and when it fires, the
// message says which of the two failures it is: rows that never moved are a
// lost transition, rows that kept moving without terminalizing are a cascade
// that cannot finish. Both carry the snapshots that prove it.
func waitForCompletedAfterOutage(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobIDs ...int64,
) {
	t.Helper()
	released := time.Now()
	observation, err := observeOutageRecovery(ctx, pool, jobIDs)
	if err != nil {
		t.Fatalf("reading jobs %v at release: %v", jobIDs, err)
	}
	changed := released
	for observation.completed < len(jobIDs) {
		if elapsed := time.Since(released); elapsed >= outageRecoveryBudget {
			if changed.Equal(released) {
				t.Fatalf(
					"outage recovery never started: no job changed state, attempt, or "+
						"error in the %s since the first reading after release -- the "+
						"transition was lost, not delayed: %#v",
					elapsed.Round(time.Second), observation.snapshots,
				)
			}
			t.Fatalf(
				"outage recovery never converged in %s: jobs were still changing "+
					"(last change %s ago) but %d of %d reached completed -- the cascade "+
					"is progressing without terminating: %#v",
				elapsed.Round(time.Second), time.Since(changed).Round(time.Second),
				observation.completed, len(jobIDs), observation.snapshots,
			)
		}
		time.Sleep(50 * time.Millisecond)
		next, err := observeOutageRecovery(ctx, pool, jobIDs)
		if err != nil {
			t.Fatalf(
				"reading jobs %v %s after release: %v -- last seen %#v",
				jobIDs, time.Since(released).Round(time.Second), err, observation.snapshots,
			)
		}
		if next.fingerprint != observation.fingerprint {
			changed = time.Now()
		}
		observation = next
	}
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
