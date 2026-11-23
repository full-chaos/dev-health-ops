package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// transportReferencePredicate is the start guard: the row is dispatched to
// this delivery's route, at the outbox attempt the relay claimed for this
// delivery ($6; any later claim bumps it), and the route is still River at the
// delivery's generation.
const transportReferencePredicate = `
    WHERE outbox.id = $1::uuid
      AND outbox.sync_run_id = $2::uuid
      AND outbox.org_id = $3
      AND outbox.kind = $5
      AND outbox.status = 'dispatched'
      AND outbox.dispatched_transport = 'river'
      AND outbox.dispatched_route_generation = $4
      AND outbox.attempts = $6
      AND route.transport = 'river'
      AND route.generation = $4
      AND route.paused = false`

// commitBarrierSQL is the statement right before commitSnapshotSQL, in the same
// (sub)transaction. syncroute.Controller (Pause, Resume, ApplyCheckedIn) and the
// river migrate route converge take the route row FOR UPDATE and then SHARE ROW
// EXCLUSIVE on this table; ROW EXCLUSIVE conflicts with that, so no route
// mutation can commit between the snapshot and the pass's COMMIT (CHAOS-14523).
// A mutation that holds or waits for its barrier fails this lock (55P03); after
// it commits, the snapshot sees its route. The domain role cannot lock routes,
// but its table INSERT/UPDATE grant on sync_dispatch_outbox allows this lock.
// NOWAIT, for the same reason as the snapshot: no wait-for edge.
const commitBarrierSQL = `LOCK TABLE public.sync_dispatch_outbox IN ROW EXCLUSIVE MODE NOWAIT`

// commitSnapshotSQL is the pass's last statement before COMMIT: it locks the
// delivery's outbox row and returns the row and route images that decideCommit
// judges, so the decision and the reason a refusal reports come from one
// snapshot. The lock is held to COMMIT or to the refusal's rollback, so neither
// a wake nor a relay claim can move the row in between. NOWAIT: the pass, which
// already holds its unit, run and sibling outbox locks, never waits for this
// row, so the lock adds no wait-for edge. The route row is read, not locked:
// commitBarrierSQL holds route mutations off.
const commitSnapshotSQL = `
SELECT outbox.status, outbox.dispatched_transport, outbox.dispatched_route_generation, outbox.attempts,
       outbox.claim_token IS NULL,
       route.transport, route.generation, route.paused
FROM public.sync_dispatch_outbox AS outbox
LEFT JOIN public.sync_dispatch_transport_routes AS route ON route.kind = outbox.kind
WHERE outbox.id = $1::uuid AND outbox.sync_run_id = $2::uuid AND outbox.org_id = $3 AND outbox.kind = $4
FOR NO KEY UPDATE OF outbox NOWAIT`

// commitSnapshot is the row images commitSnapshotSQL returns; found is false
// when the outbox row is gone (then nothing is locked).
type commitSnapshot struct {
	found                bool
	status               string
	dispatchedTransport  *string
	dispatchedGeneration *int64
	attempts             int64
	unclaimed            bool
	routeTransport       *string
	routeGeneration      *int64
	routePaused          *bool
}

func (snapshot commitSnapshot) currentRouteGeneration() int64 {
	if snapshot.routeGeneration == nil {
		return -1
	}
	return *snapshot.routeGeneration
}

// decideCommit is the one commit-time rule: the delivery commits unless it was
// superseded. The attempt first (a claim or redelivery bumps it), then the route
// (still River, unpaused, at the delivery's generation), then the row: still
// dispatched to that route, or 'pending' and unclaimed at the same attempt (a
// wake re-armed it; the wake stays armed for the next pass). A refusal's reason
// is the first clause that fails.
func decideCommit(snapshot commitSnapshot, generation, attempt int64) (bool, string) {
	switch {
	case !snapshot.found:
		return false, "outbox_row_missing"
	case snapshot.attempts != attempt:
		return false, "delivery_attempt_mismatch"
	case snapshot.routeTransport == nil:
		return false, "route_row_missing"
	case *snapshot.routeTransport != "river":
		return false, "route_other_transport"
	case snapshot.routePaused == nil || *snapshot.routePaused:
		return false, "route_paused"
	case snapshot.routeGeneration == nil || *snapshot.routeGeneration != generation:
		return false, "route_generation_mismatch"
	case snapshot.status == "dispatched":
		switch {
		case snapshot.dispatchedTransport == nil || *snapshot.dispatchedTransport != "river":
			return false, "outbox_other_transport"
		case snapshot.dispatchedGeneration == nil || *snapshot.dispatchedGeneration != generation:
			return false, "route_generation_mismatch"
		}
		return true, ""
	case snapshot.status == "pending" && snapshot.unclaimed:
		return true, ""
	}
	return false, "outbox_not_dispatched"
}

// transportFence binds one River delivery to its pass transaction. A nil fence
// (an in-process caller with no delivery, e.g. FinalizeRun) checks nothing.
type transportFence struct {
	args        Args
	kind        string
	unavailable error
	logger      *synclog.Logger
}

// commitSnapshotWindow is the retry window of a fenced commit whose outbox row
// another transaction holds (55P03). The usual holder is a wake of the same run
// (an autocommit upsert of the row, CHAOS-14265), whose wall time measured
// p99 = 5.2 ms (bigboy PG, 4-connection pool, 3 concurrent wakers, 200
// samples). The window, 315 ms, is k = 60 times that p99. It is a deadline on
// one monotonic clock, from the moment the first snapshot query is sent (after
// its savepoint opened, so a slow first savepoint does not spend it); later
// round trips, savepoints included, spend it. No query is sent at or after the
// deadline; a query sent before it counts even when it returns after it, so
// the true bound is "deadline + one in-flight read round trip". A variable so the tests can change it;
// production never does.
var commitSnapshotWindow = 315 * time.Millisecond

// commitSnapshotLockBackoff is the wait before each re-read (cut short at the
// deadline). Its sum, 310 ms, is below the window, so with fast reads the cap
// of len(commitSnapshotLockBackoff)+1 = 7 reads ends the window first. A
// variable so the tests can change it; production never does.
var commitSnapshotLockBackoff = []time.Duration{
	5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond,
	40 * time.Millisecond, 80 * time.Millisecond, 155 * time.Millisecond,
}

// commitSnapshotClock is the retry window's clock (Go's time.Now carries a
// monotonic reading). A variable so the tests can inject one; production never
// changes it.
var commitSnapshotClock = struct {
	now   func() time.Time
	sleep func(ctx context.Context, wait time.Duration) error
}{now: time.Now, sleep: sleepContext}

func sleepContext(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// commitSnapshotReadMayStart is the deadline rule: a read is sent only when it
// starts before the deadline.
func commitSnapshotReadMayStart(start, deadline time.Time) bool {
	return start.Compare(deadline) < 0
}

// commitSnapshotReadCounts is the in-flight rule, in this one place: a
// successful read counts when it STARTED before the deadline, even when it
// finished after it (a NOWAIT read is not cut mid-flight).
func commitSnapshotReadCounts(started, _ /* finished */, deadline time.Time) bool {
	return commitSnapshotReadMayStart(started, deadline)
}

// FenceCommitSnapshotBusyCounter counts fenced commits that gave up because
// the delivery's outbox row stayed held to the end of the retry window, with
// the attributes kind (the outbox kind) and reason.
const FenceCommitSnapshotBusyCounter = "sync_fence_commit_snapshot_busy_total"

// commitSnapshotBusyReason names a fenced commit that gave up on a held row.
const commitSnapshotBusyReason = "outbox_row_held_past_bound"

// commitBarrierBusyReason names a fenced commit that gave up because a route
// mutation held (or waited for) its barrier to the end of the retry window.
const commitBarrierBusyReason = "outbox_route_barrier_held_past_bound"

// errCommitBarrierHeld marks a 55P03 of commitBarrierSQL; the PostgreSQL error
// stays in the chain, so isLockNotAvailable still sees it.
var errCommitBarrierHeld = errors.New("route mutation barrier held")

// commitBusyReason is the give-up reason for the last 55P03 of the window.
func commitBusyReason(err error) string {
	if errors.Is(err, errCommitBarrierHeld) {
		return commitBarrierBusyReason
	}
	return commitSnapshotBusyReason
}

// commitOutcome is a fenced commit's result: a refusal carries the reason and
// the route generation from the same snapshot that refused it.
type commitOutcome struct {
	committed       bool
	reason          string
	routeGeneration int64
}

func (fence *transportFence) current(ctx context.Context, tx pgx.Tx) (bool, error) {
	if fence == nil {
		return true, nil
	}
	return currentTransportReference(ctx, tx, fence.args, fence.kind)
}

// commit commits tx only while the delivery is not superseded. A refusal
// (committed false, nil error) commits nothing: the caller's deferred Rollback
// discards the pass and releases the row. A row held by another transaction, or
// a route barrier a route mutation holds (SQLSTATE 55P03), is read again within
// commitSnapshotLockBackoff; one still held after it, or any other read error,
// returns the service's unavailable error, so River retries the same delivery.
func (fence *transportFence) commit(ctx context.Context, tx pgx.Tx) (commitOutcome, error) {
	return fence.commitAfter(ctx, tx, nil)
}

// passTail is the last step of a pass, run in its transaction after the commit
// snapshot accepted the delivery and before COMMIT. The snapshot holds the
// delivery's outbox row, so no newer delivery can own the row while the tail
// writes it.
type passTail func(ctx context.Context, tx pgx.Tx) error

// commitAfter is commit with a tail: a tail error is returned as it is and
// nothing commits (the caller's deferred Rollback discards the whole pass), so
// River retries an attempt that committed nothing.
func (fence *transportFence) commitAfter(ctx context.Context, tx pgx.Tx, tail passTail) (commitOutcome, error) {
	if fence == nil {
		if tail != nil {
			if err := tail(ctx, tx); err != nil {
				return commitOutcome{}, err
			}
		}
		return commitOutcome{committed: true}, tx.Commit(ctx)
	}
	snapshot, err := fence.lockCommitSnapshot(ctx, tx)
	if err != nil {
		return commitOutcome{}, err
	}
	commit, reason := decideCommit(snapshot, fence.args.RouteGeneration(), fence.args.Attempt())
	if !commit {
		return commitOutcome{reason: reason, routeGeneration: snapshot.currentRouteGeneration()}, nil
	}
	if tail != nil {
		if err := tail(ctx, tx); err != nil {
			return commitOutcome{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return commitOutcome{}, fence.unavailable
	}
	return commitOutcome{committed: true}, nil
}

// lockCommitSnapshot reads the commit snapshot inside a savepoint. A 55P03
// aborts only the savepoint: it is rolled back, and after the next backoff step
// the snapshot is read again, never reused. NOWAIT stays, so the pass still
// adds no wait-for edge; it only polls. The deadline is checked after each
// savepoint opens, right before its query: a savepoint that opens at or after
// it is rolled back and no query is sent. On success the savepoint is released
// and the row lock stays with the pass to its COMMIT. The window ends at the
// read cap or at the deadline, whichever comes first; then the pass gives up.
func (fence *transportFence) lockCommitSnapshot(ctx context.Context, tx pgx.Tx) (commitSnapshot, error) {
	clock := commitSnapshotClock
	var deadline time.Time
	var lastErr error
	maxReads := len(commitSnapshotLockBackoff) + 1
	for read := 1; ; read++ {
		savepoint, err := tx.Begin(ctx)
		if err != nil {
			return commitSnapshot{}, fence.unavailable
		}
		queryStart := clock.now()
		if read == 1 {
			deadline = queryStart.Add(commitSnapshotWindow)
		} else if !commitSnapshotReadMayStart(queryStart, deadline) {
			if rollbackErr := savepoint.Rollback(ctx); rollbackErr != nil {
				return commitSnapshot{}, fence.unavailable
			}
			return commitSnapshot{}, fence.commitSnapshotBusy(ctx, read-1, lastErr)
		}
		snapshot, err := readCommitSnapshot(ctx, savepoint, fence.args, fence.kind)
		if err == nil {
			if !commitSnapshotReadCounts(queryStart, clock.now(), deadline) {
				if rollbackErr := savepoint.Rollback(ctx); rollbackErr != nil {
					return commitSnapshot{}, fence.unavailable
				}
				return commitSnapshot{}, fence.commitSnapshotBusy(ctx, read, nil)
			}
			if err := savepoint.Commit(ctx); err != nil {
				return commitSnapshot{}, fence.unavailable
			}
			return snapshot, nil
		}
		if !isLockNotAvailable(err) {
			return commitSnapshot{}, fence.unavailable
		}
		if rollbackErr := savepoint.Rollback(ctx); rollbackErr != nil {
			return commitSnapshot{}, fence.unavailable
		}
		if read >= maxReads {
			return commitSnapshot{}, fence.commitSnapshotBusy(ctx, read, err)
		}
		lastErr = err
		if remaining := deadline.Sub(clock.now()); remaining > 0 {
			if clock.sleep(ctx, min(commitSnapshotLockBackoff[read-1], remaining)) != nil {
				return commitSnapshot{}, fence.unavailable
			}
		}
	}
}

// commitSnapshotBusy reports a fenced commit that gave up on a held outbox row or a held route barrier and returns
// its error: the service's unavailable error naming the reason.
func (fence *transportFence) commitSnapshotBusy(ctx context.Context, reads int, err error) error {
	reason := commitBusyReason(err)
	fence.reportCommitSnapshotBusy(ctx, reads, commitSnapshotWindow, reason, err)
	return fmt.Errorf("%w: commit snapshot: %s (SQLSTATE 55P03 after %d reads in a %s window)",
		fence.unavailable, reason, reads, commitSnapshotWindow)
}

// reportCommitSnapshotBusy counts and logs one fenced commit that gave up on a held outbox row or route barrier.
func (fence *transportFence) reportCommitSnapshotBusy(ctx context.Context, tries int, bound time.Duration, reason string, err error) {
	if counter, counterErr := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime").Int64Counter(
		FenceCommitSnapshotBusyCounter,
		metric.WithUnit("{delivery}"),
		metric.WithDescription("fenced commits that gave up because another transaction held the delivery's outbox row or a route mutation held the outbox barrier past the retry bound, by outbox kind and reason")); counterErr == nil {
		counter.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", fence.kind), attribute.String("reason", reason)))
	}
	fence.logger.Warn(ctx, synclog.MsgSyncDispatchCommitSnapshotBusy,
		synclog.Run(synclog.ParseID(fence.args.SyncRunID())),
		synclog.Text(synclog.KeyOutboxKind, synclog.ParseLabel(fence.kind)),
		synclog.Text(synclog.KeyReason, synclog.ParseLabel(reason)),
		synclog.Count(synclog.KeyTries, tries),
		synclog.Elapsed(synclog.KeyLockWaitBudget, bound),
		synclog.Failure(err))
}

func isLockNotAvailable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

// readCommitSnapshot takes the route barrier (commitBarrierSQL), then runs commitSnapshotSQL for one delivery: it
// locks the outbox row and returns the images decideCommit judges. A missing row is the zero snapshot (found false,
// only the barrier is held). A 55P03 of the barrier is returned wrapped in errCommitBarrierHeld; any other error, a
// 55P03 of the row included, is returned as it is.
func readCommitSnapshot(ctx context.Context, tx pgx.Tx, args Args, kind string) (commitSnapshot, error) {
	if _, err := tx.Exec(ctx, commitBarrierSQL); err != nil {
		if isLockNotAvailable(err) {
			return commitSnapshot{}, fmt.Errorf("%w: %w", errCommitBarrierHeld, err)
		}
		return commitSnapshot{}, err
	}
	snapshot := commitSnapshot{found: true}
	err := tx.QueryRow(ctx, commitSnapshotSQL, args.OutboxID(), args.SyncRunID(), args.OrganizationID(), kind).
		Scan(&snapshot.status, &snapshot.dispatchedTransport, &snapshot.dispatchedGeneration, &snapshot.attempts,
			&snapshot.unclaimed, &snapshot.routeTransport, &snapshot.routeGeneration, &snapshot.routePaused)
	if errors.Is(err, pgx.ErrNoRows) {
		return commitSnapshot{}, nil
	}
	if err != nil {
		return commitSnapshot{}, err
	}
	return snapshot, nil
}

// postCommitLogs holds log lines that state a write of the pass: they are
// emitted only after the fenced commit succeeded, never for a refused or
// failed pass.
type postCommitLogs struct {
	mu    sync.Mutex
	lines []func()
}

type postCommitLogsKey struct{}

func withPostCommitLogs(ctx context.Context) context.Context {
	return context.WithValue(ctx, postCommitLogsKey{}, &postCommitLogs{})
}

// logAfterCommit defers line to the owning pass's commit; without a buffer in
// ctx (a caller that owns no fenced pass) it logs at once.
func logAfterCommit(ctx context.Context, line func()) {
	buffer, ok := ctx.Value(postCommitLogsKey{}).(*postCommitLogs)
	if !ok || buffer == nil {
		line()
		return
	}
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	buffer.lines = append(buffer.lines, line)
}

func flushPostCommitLogs(ctx context.Context) {
	buffer, ok := ctx.Value(postCommitLogsKey{}).(*postCommitLogs)
	if !ok || buffer == nil {
		return
	}
	buffer.mu.Lock()
	lines := buffer.lines
	buffer.lines = nil
	buffer.mu.Unlock()
	for _, line := range lines {
		line()
	}
}
