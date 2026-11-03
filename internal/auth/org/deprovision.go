package org

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// LockForUpdate takes the organization row FOR UPDATE inside the caller's
// audit.Commit: step 3 of the auth lock order (membership.Store readPairs),
// exclusive, so no membership or invite verb of the organization (they take it
// FOR SHARE) runs until the commit. active is false for a suspended
// organization; a missing one is ErrNotFound.
func (s *Store) LockForUpdate(ctx context.Context, tx audit.TxOps, id uuid.UUID) (active bool, err error) {
	err = tx.QueryRow(ctx, `SELECT disabled_at IS NULL FROM `+s.q+`.organizations WHERE id = $1 FOR UPDATE`, id).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	return active, err
}

// ApplySuspend sets disabled_at inside the caller's audit.Commit, on a row the
// caller locked with LockForUpdate. A suspended organization keeps its
// disabled_at. While auth-service owns organizations, go-api's is_active goes
// false in the same commit.
func (s *Store) ApplySuspend(ctx context.Context, tx audit.TxOps, id uuid.UUID) error {
	_, err := tx.Exec(ctx, `UPDATE `+s.q+`.organizations SET disabled_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE id = $1 AND disabled_at IS NULL`, id)
	if err != nil || !s.ownsOrgs {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE public.organizations SET is_active = false, updated_at = clock_timestamp() WHERE id = $1 AND is_active`, id)
	return err
}

// SuspendedEvent is the organization.suspended provisioning event.
func SuspendedEvent(id uuid.UUID) audit.OutboxEvent { return eventFor(verbSuspend, id, "") }
