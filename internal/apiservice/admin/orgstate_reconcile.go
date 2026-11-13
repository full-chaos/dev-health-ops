package admin

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
)

const (
	orgStateReconcileInterval = 5 * time.Minute
	orgStateReconcilePage     = 200
	// orgStateReconcileCallsPerSecond paces the pass's auth state reads (row reads and fix re-reads) well under the
	// state route's default bucket (20/s, burst 40, per auth pod), so other callers and replicas keep headroom.
	orgStateReconcileCallsPerSecond = 5
	orgStateReconcileCallGap        = time.Second / orgStateReconcileCallsPerSecond
	// orgStateReconcileRetryAfterMax bounds the wait a 429's Retry-After asks for.
	orgStateReconcileRetryAfterMax = 10 * time.Second
	// orgStateReconcileRateLimitWait is the wait after a 429 that names no Retry-After.
	orgStateReconcileRateLimitWait = time.Second
	// orgStateReconcileRateLimitTries bounds the calls of one read that auth answers 429; then the pass stops and the
	// next one resumes at that row.
	orgStateReconcileRateLimitTries = 5
)

// OrgStateReconciler keeps organizations.is_active equal to auth-service's state: a PATCH stores what it read under the
// row lock, and a change after that read reaches the row here. It runs only with AUTH_PROXY_ORG_PROVISIONING on.
type OrgStateReconciler struct {
	pool   *pgxpool.Pool
	state  orgreactivate.StateReader
	logger *slog.Logger
	now    func() time.Time
	// wait sleeps d or until ctx ends; with now it is the pacer's clock.
	wait func(ctx context.Context, d time.Duration) error

	// next is the earliest time of the next auth call; cursor is the last row a stopped pass covered (zero after a
	// full walk). Only the pass loop touches them.
	next   time.Time
	cursor uuid.UUID

	cancel context.CancelFunc
	done   chan struct{}
}

// NewOrgStateReconciler builds the reconciler over go-api's pool and its auth state client.
func NewOrgStateReconciler(pool *pgxpool.Pool, state orgreactivate.StateReader, logger *slog.Logger) *OrgStateReconciler {
	return &OrgStateReconciler{pool: pool, state: state, logger: logger, now: time.Now, wait: sleepOrDone}
}

func sleepOrDone(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Name is the lifecycle component name.
func (r *OrgStateReconciler) Name() string { return "organization-state-reconciler" }

// Start runs a pass every orgStateReconcileInterval; the first one comes after one interval.
func (r *OrgStateReconciler) Start(ctx context.Context) error {
	ctx, r.cancel = context.WithCancel(ctx)
	r.done = make(chan struct{})
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(orgStateReconcileInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.pass(ctx)
			}
		}
	}()
	return nil
}

// Shutdown stops the loop and waits for a running pass, until ctx ends.
func (r *OrgStateReconciler) Shutdown(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type orgStateRow struct {
	id     uuid.UUID
	active bool
}

// orgStateRead sorts one read of auth's state. Exactly one of the fields after active is set on a read with no answer.
// A 429 (rateLimited) is neither refused nor covered: the same read is made again.
type orgStateRead struct {
	active      bool
	refused     bool
	stop        bool
	rateLimited bool
	retryAfter  time.Duration
}

// pass compares every organization with auth once, from the row after the cursor. A row auth refuses is skipped and
// counted; a 429 waits and reads the row again; a read or store failure, or a row still answered 429 after
// orgStateReconcileRateLimitTries calls, ends the pass, and the next one resumes after the last row covered. A walk
// to the end of the table resets the cursor, and so does a resumed pass that stops before it covers any row: a row
// that stops every pass then costs the rows before it at most every second pass. It logs one WARN at most, counts only.
func (r *OrgStateReconciler) pass(ctx context.Context) {
	refused, rateLimited, covered := 0, 0, 0
	stopped := false
	after := r.cursor
walk:
	for {
		rows, err := r.page(ctx, after)
		if err != nil {
			stopped = true
			break
		}
		for _, row := range rows {
			read, _ := r.paced(ctx, &rateLimited, func() (orgStateRead, error) { return r.read(ctx, row.id), nil })
			switch {
			case read.stop:
				stopped = true
				break walk
			case read.refused:
				refused++
			case read.active != row.active:
				again, err := r.paced(ctx, &rateLimited, func() (orgStateRead, error) { return r.fix(ctx, row.id) })
				switch {
				case err != nil || again.stop:
					stopped = true
					break walk
				case again.refused:
					refused++
				}
			}
			after = row.id
			covered++
		}
		if len(rows) < orgStateReconcilePage {
			after = uuid.Nil
			break
		}
	}
	if stopped && covered == 0 {
		after = uuid.Nil
	}
	r.cursor = after
	if ctx.Err() == nil && (refused > 0 || stopped) {
		r.logger.WarnContext(ctx, "admin: organization state reconcile did not cover every row",
			"refused", refused, "rate_limited", rateLimited, "stopped", stopped)
	}
}

// paced makes one auth call after the pacer allows it. A 429 is counted in limited; the call is made again after the
// wait the answer asks for (bounded), up to orgStateReconcileRateLimitTries calls, then the answer is stop.
func (r *OrgStateReconciler) paced(ctx context.Context, limited *int, call func() (orgStateRead, error)) (orgStateRead, error) {
	for try := 1; ; try++ {
		if err := r.pace(ctx); err != nil {
			return orgStateRead{stop: true}, nil
		}
		read, err := call()
		if err != nil || !read.rateLimited {
			return read, err
		}
		*limited++
		if try >= orgStateReconcileRateLimitTries {
			return orgStateRead{stop: true}, nil
		}
		if err := r.wait(ctx, rateLimitWait(read.retryAfter)); err != nil {
			return orgStateRead{stop: true}, nil
		}
	}
}

// pace waits until the next call is due, then books the one after it orgStateReconcileCallGap later.
func (r *OrgStateReconciler) pace(ctx context.Context) error {
	if d := r.next.Sub(r.now()); d > 0 {
		if err := r.wait(ctx, d); err != nil {
			return err
		}
	}
	start := r.now()
	if start.Before(r.next) {
		start = r.next
	}
	r.next = start.Add(orgStateReconcileCallGap)
	return nil
}

// rateLimitWait is the wait after a 429: its Retry-After up to orgStateReconcileRetryAfterMax, else the fixed wait.
func rateLimitWait(retryAfter time.Duration) time.Duration {
	switch {
	case retryAfter <= 0:
		return orgStateReconcileRateLimitWait
	case retryAfter > orgStateReconcileRetryAfterMax:
		return orgStateReconcileRetryAfterMax
	}
	return retryAfter
}

func (r *OrgStateReconciler) page(ctx context.Context, after uuid.UUID) ([]orgStateRow, error) {
	rows, err := r.pool.Query(ctx, `SELECT id, is_active FROM organizations WHERE id > $1 ORDER BY id LIMIT $2`, after, orgStateReconcilePage)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (orgStateRow, error) {
		var out orgStateRow
		return out, row.Scan(&out.id, &out.active)
	})
}

func (r *OrgStateReconciler) read(ctx context.Context, id uuid.UUID) orgStateRead {
	readCtx, cancel := context.WithTimeout(ctx, orgAuthReReadTimeout)
	defer cancel()
	state, err := r.state.State(readCtx, id)
	var limited *orgreactivate.RateLimited
	switch {
	case errors.As(err, &limited):
		return orgStateRead{rateLimited: true, retryAfter: limited.RetryAfter}
	case errors.Is(err, orgreactivate.ErrRefused):
		return orgStateRead{refused: true}
	case err != nil:
		return orgStateRead{stop: true}
	}
	active, known := authStatusActive(state.Status)
	if !known {
		return orgStateRead{refused: true}
	}
	return orgStateRead{active: active}
}

// fix re-reads auth with the row locked, as a PATCH does, so the last writer read auth last; a row a PATCH holds is
// skipped, and that PATCH reads auth itself. FOR NO KEY UPDATE leaves foreign key inserts free during the read. A 429
// returns at once, so the lock is released before the caller waits.
func (r *OrgStateReconciler) fix(ctx context.Context, id uuid.UUID) (orgStateRead, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return orgStateRead{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var stored bool
	err = tx.QueryRow(ctx, `SELECT is_active FROM organizations WHERE id = $1 FOR NO KEY UPDATE SKIP LOCKED`, id).Scan(&stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return orgStateRead{}, nil
	}
	if err != nil {
		return orgStateRead{}, err
	}
	read := r.read(ctx, id)
	if read.stop || read.refused || read.rateLimited || read.active == stored {
		return read, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE organizations SET is_active = $2, updated_at = $3 WHERE id = $1`, id, read.active, r.now().UTC()); err != nil {
		return orgStateRead{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return orgStateRead{}, err
	}
	r.logger.InfoContext(ctx, "admin: organization is_active reconciled to auth state", "org_id", id.String(), "from", stored, "to", read.active)
	return read, nil
}
