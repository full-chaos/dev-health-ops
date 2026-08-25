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
	"sync/atomic"
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
//
// CHAOS-4235 -- REACHABILITY FIX. The two heartbeat jobs used to be inserted
// back-to-back before either was claimed, which let River route both to the
// SAME replica when its producer simply polled first (that replica's own
// heartbeat queue has one worker slot, so the second job then just waited
// its turn locally after the first one finished -- no fleet-lease contention
// at all). Ad hoc local investigation loops of that shape (dozens of runs,
// not checked into this tree -- see AGENTS.md on why a trial harness does
// not belong in the repo) never once exercised the cross-replica race CI
// hit, because the test could pass while never reaching it. Job 2 is now
// inserted only after job 1 is claimed AND parked in the bridge, which
// occupies its replica's one heartbeat worker slot and forces job 2 onto
// the sibling -- genuine fleet-lease contention, deterministically, every
// run: this ordering, not a trial count, is what makes contention no
// longer optional (CHAOS-4235). The reachability guarantee is enforced
// below by two runtime assertions, not just this comment: stillParked()
// before job 2 is inserted, and an AttemptedBy-owner mismatch check after.
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
	// completerProgress is shared by both replicas' loggers: it records the
	// last time EITHER replica's River BatchCompleter logged retry/requeue
	// activity, which is the only in-process signal of a completer round
	// that is still working but has not yet landed (CHAOS-4235; see
	// waitForCompletedAfterOutage and completerProgressHandler).
	completerProgress := newCompleterProgressTracker()
	logger := slog.New(completerProgress.wrap(slog.NewTextHandler(io.Discard, nil)))

	first := newOperationalReplica(t, ctx, outage.uri, server.URL, registry, logger)
	second := newOperationalReplica(t, ctx, outage.uri, server.URL, registry, logger)
	t.Cleanup(func() { first.close(t); second.close(t) })
	first.start(t, ctx)
	second.start(t, ctx)
	assertWorkerPresence(t, ctx, admin, "operations-shared",
		[]string{"coverage", "heartbeat", "retention", "webhooks"}, 2, 0)

	// job 1: inserted alone, and claimed and parked in the bridge before job 2
	// exists at all. That occupies its replica's one heartbeat worker slot for
	// the rest of the test.
	firstJob := insertHeartbeat(t, ctx, first.client, 1)
	bridge.waitForFirst(t)
	waitFor(t, 30*time.Second, func() (bool, error) {
		row, err := readRiverJob(ctx, admin, firstJob.Job.ID)
		return err == nil && len(row.AttemptedBy) == 1, err
	})

	// The reachability guarantee below depends on job 1's handler still
	// occupying its replica's one heartbeat worker slot. That handler's own
	// HTTP call is bounded by OperationalBridgeTimeout (20s); under enough
	// load for setup to have taken that long, it could already have exited
	// on its own request context and freed the slot, silently defeating the
	// forced distribution. Assert the barrier explicitly instead of assuming
	// elapsed time was short enough (codex adversarial review, CHAOS-4235).
	if !bridge.stillParked() {
		t.Fatal("setup took long enough that job 1's handler is no longer parked " +
			"in the bridge -- it likely hit its own OperationalBridgeTimeout before " +
			"job 2 could be inserted, which would silently defeat the forced " +
			"cross-replica distribution below")
	}

	// job 2: only inserted now. The parked replica has no free heartbeat
	// slot, so this is forced onto the sibling -- guaranteed cross-replica
	// fleet-lease contention every run (CHAOS-4235; see the reachability
	// comment on this test above).
	secondJob := insertHeartbeat(t, ctx, first.client, 2)
	waitFor(t, 30*time.Second, func() (bool, error) {
		row, err := readRiverJob(ctx, admin, secondJob.Job.ID)
		return err == nil && len(row.AttemptedBy) == 1, err
	})
	firstRow, err := readRiverJob(ctx, admin, firstJob.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	secondRow, err := readRiverJob(ctx, admin, secondJob.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if firstRow.AttemptedBy[0] == secondRow.AttemptedBy[0] {
		t.Fatalf("forced-distribution setup failed: both jobs claimed by the same replica %q", firstRow.AttemptedBy[0])
	}

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
	waitForCompletedAfterOutage(t, ctx, admin, completerProgress, firstJob.Job.ID, secondJob.Job.ID)

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
	// generation increments on every cut() and never decreases. It exists
	// because `open` alone cannot detect a full cut()+restore() cycle that
	// happens entirely while a connection is mid-Dial: `open` would read
	// true again by the time that dial finishes, even though a cut()
	// happened in between and should have closed this connection. A
	// captured generation that no longer matches the current one means
	// exactly that: a cut() landed during this dial, regardless of what
	// open reads afterward (codex adversarial review, CHAOS-4235).
	generation int
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
		generation := cutover.generation
		cutover.mu.Unlock()
		if !open {
			_ = client.Close()
			continue
		}
		// Dialing happens without the lock held: it is the slow step, and
		// cut() must not block on it.
		upstream, err := net.Dial("tcp", cutover.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		// CHAOS-4235 (codex adversarial review): re-check UNDER THE SAME LOCK
		// as the append below, not the earlier snapshot -- cut() also takes
		// this lock to flip open, bump generation, and close+clear conns.
		// Comparing generation, not just open, additionally catches a full
		// cut()+restore() cycle that ran entirely during the Dial above:
		// open would read true again by now even though a cut() happened in
		// between, so this connection was never in the slice cut() closed
		// and would otherwise silently keep bridging through the simulated
		// outage.
		cutover.mu.Lock()
		if !cutover.open || cutover.generation != generation {
			cutover.mu.Unlock()
			_ = client.Close()
			_ = upstream.Close()
			continue
		}
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
//     -- jittered 1 + 2 + 4s of backoff, and up to 10s per attempt, so ~37s
//     at worst per round (river@v0.44.0 internal/jobcompleter/job_completer.go
//     lines 706-709, `numRetries`) -- then requeues that batch and starts
//     over via the SAME 50ms-ticker outer loop (job_completer.go:361-420),
//     with NO cap on how many rounds it may repeat and NO escalating
//     backoff between rounds (requeueBatch, job_completer.go:580-601, just
//     re-enqueues into the completer's in-memory backlog for the next tick).
//     That requeue never touches river_job at all: until some round's write
//     finally lands, the job sits in `running` with `attempt`, `errors`, and
//     every other river_job column completely unchanged. This is
//     architecturally unbounded, by design (River's durability guarantee
//     never gives up on a claimed job's completion write).
//
//     This is CHAOS-4235's best-supported hypothesis for the CI failure,
//     not a proven-live root cause: it is the only mechanism found that
//     reproduces every observed symptom (Attempt frozen at 1, zero row
//     changes, no error ever recorded, for 258+ seconds) and it is
//     code-verified against the vendored library's own source and its own
//     documented worst-case timing -- but no local trial (85 across four
//     configurations, including a simulated loaded/shared 2-vCPU runner)
//     ever caught a completer round actually failing to land for that long,
//     so causation was never observed directly, only inferred. What CHAOS-
//     4235's isolated diagnostic DID rule out directly: a context-
//     cancellation hang in our own runtime (30 trials driving the same
//     TCP-level outage against a blocked budget.Acquire poll near its
//     deadline, 0 hangs -- every trial failed fast on a real DB error well
//     inside its deadline). See completerProgressHandler below: it is the
//     one in-process signal available today that a completer round is
//     still working even though river_job has not moved -- it does not
//     make a stuck round land, and CHAOS-4249 tracks giving production the
//     same visibility.
//
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
//
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
// So this asserts convergence against TWO signals, not a wall clock alone:
// river_job's own fingerprint (state/attempt/attempted_by/errors), and
// whether either replica's BatchCompleter has logged retry or requeue
// activity recently (completerProgressHandler). Progress on EITHER signal
// resets the quiet timer; only silence on BOTH for completerProgressGrace
// trips the hang detector early, well inside outageRecoveryBudget. That
// closes exactly the blind spot the unbounded completer loop above creates:
// a river_job-only observer cannot tell "the completer is still retrying" from
// "recovery is truly stuck" -- both look like a frozen row. The completer's
// own log activity, sampled every 50ms-to-~10s while a round works, does not
// have that blind spot. Four minutes remains the outer, absolute ceiling for
// a job that never reaches even that log-level progress: River's rescuer only
// reclaims after an hour, far outside this budget, so a genuinely stranded
// job is not waited out.
const outageRecoveryBudget = 4 * time.Minute

// completerProgressGrace bounds how long BOTH signals -- a river_job
// fingerprint change and a BatchCompleter retry/requeue log line -- may stay
// silent before waitForCompletedAfterOutage treats that as the hang, rather
// than waiting the full outageRecoveryBudget. It is set comfortably above one
// completer round's worst case (~37s, see above) plus scheduling slack, and
// comfortably below outageRecoveryBudget so a genuine hang is reported in
// well under a minute instead of four.
const completerProgressGrace = 60 * time.Second

// completerProgressHandler is an slog.Handler decorator that records the last
// time it saw a River jobcompleter.BatchCompleter log line -- "Completer
// error (will retry after sleep)" or "Too many errors; giving up" are both
// logged at ERROR level (job_completer.go withRetries), so they reach this
// handler regardless of the configured log level. This is the CHAOS-4235
// fix for the harness's blind spot: requeueBatch never writes to river_job,
// so a completer round that is failing and retrying is otherwise
// indistinguishable, to a river_job-only observer, from a job that is
// permanently wedged.
type completerProgressHandler struct {
	slog.Handler
	lastActivity *atomic.Int64 // UnixNano, shared across WithAttrs/WithGroup derivations
}

// completerProgressTracker owns the shared timestamp both replicas' loggers
// report into, and answers "how long has it been quiet" for the wait loop.
type completerProgressTracker struct {
	lastActivity atomic.Int64
}

func newCompleterProgressTracker() *completerProgressTracker {
	tracker := &completerProgressTracker{}
	tracker.lastActivity.Store(time.Now().UnixNano())
	return tracker
}

// wrap decorates a base handler so any BatchCompleter log record it receives
// updates the tracker, then forwards to the base handler unchanged (this
// test discards output either way; the decoration exists only to observe).
func (tracker *completerProgressTracker) wrap(base slog.Handler) slog.Handler {
	return &completerProgressHandler{Handler: base, lastActivity: &tracker.lastActivity}
}

func (tracker *completerProgressTracker) lastActivityTime() time.Time {
	return time.Unix(0, tracker.lastActivity.Load())
}

func (tracker *completerProgressTracker) quietFor() time.Duration {
	return time.Since(tracker.lastActivityTime())
}

func (handler *completerProgressHandler) Handle(ctx context.Context, record slog.Record) error {
	if strings.Contains(record.Message, "BatchCompleter") {
		handler.lastActivity.Store(time.Now().UnixNano())
	}
	return handler.Handler.Handle(ctx, record)
}

func (handler *completerProgressHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &completerProgressHandler{Handler: handler.Handler.WithAttrs(attrs), lastActivity: handler.lastActivity}
}

func (handler *completerProgressHandler) WithGroup(name string) slog.Handler {
	return &completerProgressHandler{Handler: handler.Handler.WithGroup(name), lastActivity: handler.lastActivity}
}

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
// broken one, so progress on EITHER the river_job fingerprint or the
// completer's own retry log activity resets the quiet timer -- and when it
// fires, the message says which of the three failures it is: rows that never
// moved (and the completer never logged) are a lost transition, rows that
// kept moving without terminalizing are a cascade that cannot finish, and a
// long silence on both signals is the CHAOS-4235 shape -- a completer round
// that stopped landing. All three carry the snapshots that prove it.
func waitForCompletedAfterOutage(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	completerProgress *completerProgressTracker, jobIDs ...int64,
) {
	t.Helper()
	released := time.Now()
	observation, err := observeOutageRecovery(ctx, pool, jobIDs)
	if err != nil {
		t.Fatalf("reading jobs %v at release: %v", jobIDs, err)
	}
	changed := released
	for observation.completed < len(jobIDs) {
		elapsed := time.Since(released)
		quiet := changed
		if completerLast := completerProgress.lastActivityTime(); completerLast.After(quiet) {
			quiet = completerLast
		}
		quietFor := time.Since(quiet)
		switch {
		case elapsed >= outageRecoveryBudget:
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
					"(last river_job change %s ago, last completer retry log %s ago) but "+
					"%d of %d reached completed -- the cascade is progressing without "+
					"terminating: %#v",
				elapsed.Round(time.Second), time.Since(changed).Round(time.Second),
				completerProgress.quietFor().Round(time.Second),
				observation.completed, len(jobIDs), observation.snapshots,
			)
		case quietFor >= completerProgressGrace:
			t.Fatalf(
				"outage recovery stalled: neither river_job nor the BatchCompleter's own "+
					"retry log has shown activity in %s (river_job last changed %s ago, "+
					"completer last logged %s ago) -- a completer round stopped landing "+
					"without any observable retry, which outageRecoveryBudget's remaining "+
					"%s would not have caught for a while yet: %#v",
				quietFor.Round(time.Second), time.Since(changed).Round(time.Second),
				completerProgress.quietFor().Round(time.Second),
				(outageRecoveryBudget - elapsed).Round(time.Second), observation.snapshots,
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
	cutover.generation++
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
