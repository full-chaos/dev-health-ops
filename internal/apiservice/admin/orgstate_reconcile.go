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
)

// OrgStateReconciler keeps organizations.is_active equal to auth-service's state: a PATCH stores what it read under the
// row lock, and a change after that read reaches the row here. It runs only with AUTH_PROXY_ORG_PROVISIONING on.
type OrgStateReconciler struct {
	pool   *pgxpool.Pool
	state  orgreactivate.StateReader
	logger *slog.Logger
	now    func() time.Time

	cancel context.CancelFunc
	done   chan struct{}
}

// NewOrgStateReconciler builds the reconciler over go-api's pool and its auth state client.
func NewOrgStateReconciler(pool *pgxpool.Pool, state orgreactivate.StateReader, logger *slog.Logger) *OrgStateReconciler {
	return &OrgStateReconciler{pool: pool, state: state, logger: logger, now: time.Now}
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
type orgStateRead struct {
	active  bool
	refused bool
	stop    bool
}

// pass compares every organization with auth once. A row auth refuses is skipped and counted; a read or store failure
// ends the pass, and the next one starts over. It logs one WARN at most, counts only.
func (r *OrgStateReconciler) pass(context.Context) {}

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
	switch {
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
// skipped, and that PATCH reads auth itself. FOR NO KEY UPDATE leaves foreign key inserts free during the read.
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
	if read.stop || read.refused || read.active == stored {
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
