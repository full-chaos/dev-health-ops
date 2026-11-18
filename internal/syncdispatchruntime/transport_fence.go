package syncdispatchruntime

import (
	"context"
	"errors"
	"sync"

	"github.com/jackc/pgx/v5"
)

// transportDeliveryClauses tie a delivery to its outbox row and its live route:
// $6 is the outbox attempt the relay claimed for this delivery (any later claim
// bumps it), and the route must still be River at the delivery's generation.
const transportDeliveryClauses = `
    WHERE outbox.id = $1::uuid
      AND outbox.sync_run_id = $2::uuid
      AND outbox.org_id = $3
      AND outbox.kind = $5
      AND outbox.attempts = $6
      AND route.transport = 'river'
      AND route.generation = $4
      AND route.paused = false`

const outboxDispatchedToThisRoute = `outbox.status = 'dispatched'
          AND outbox.dispatched_transport = 'river'
          AND outbox.dispatched_route_generation = $4`

// transportReferencePredicate is the start guard: the row must be dispatched to
// this delivery's route.
const transportReferencePredicate = transportDeliveryClauses + `
      AND ` + outboxDispatchedToThisRoute

// commitCheckSQL re-proves the delivery as the pass's last statement and holds
// the outbox row until COMMIT. It refuses a SUPERSEDED delivery: a new claim or
// redelivery (attempt bumped) or a route change. A wake that only re-armed the
// row ('pending', unclaimed, same attempt) does not refuse the pass: the wake
// stays armed for the next pass, and the row lock keeps the relay (SKIP LOCKED)
// off it until this pass committed. NOWAIT: the pass, which already holds its
// unit, run and sibling outbox locks, never waits for this row, so the check
// adds no wait-for edge. Only the outbox row is locked: the domain role cannot
// lock routes.
const commitCheckSQL = `
SELECT 1
    FROM public.sync_dispatch_outbox AS outbox
    JOIN public.sync_dispatch_transport_routes AS route
      ON route.kind = outbox.kind` + transportDeliveryClauses + `
      AND ((` + outboxDispatchedToThisRoute + `)
        OR (outbox.status = 'pending' AND outbox.claim_token IS NULL))
FOR NO KEY UPDATE OF outbox NOWAIT`

// transportFence binds one River delivery to its pass transaction. A nil fence
// (an in-process caller with no delivery, e.g. FinalizeRun) checks nothing.
type transportFence struct {
	args        Args
	kind        string
	unavailable error
}

func (fence *transportFence) current(ctx context.Context, tx pgx.Tx) (bool, error) {
	if fence == nil {
		return true, nil
	}
	return currentTransportReference(ctx, tx, fence.args, fence.kind)
}

// commit commits tx only while the delivery is not superseded. committed=false
// with a nil error is a refusal: nothing is committed and the caller's deferred
// Rollback discards the pass. A row held by another transaction returns the
// service's unavailable error, so River retries the same delivery.
func (fence *transportFence) commit(ctx context.Context, tx pgx.Tx) (bool, error) {
	if fence == nil {
		return true, tx.Commit(ctx)
	}
	var one int
	err := tx.QueryRow(ctx, commitCheckSQL, fence.args.OutboxID(), fence.args.SyncRunID(), fence.args.OrganizationID(),
		fence.args.RouteGeneration(), fence.kind, fence.args.Attempt()).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		// Includes SQLSTATE 55P03 (NOWAIT: the row is held elsewhere).
		return false, fence.unavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fence.unavailable
	}
	return true, nil
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
