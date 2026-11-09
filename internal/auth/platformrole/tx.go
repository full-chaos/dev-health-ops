package platformrole

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// LockWrites takes the store's write lock exclusively on the caller's transaction. A caller that grants or revokes
// inside its own audit.Commit (GrantOn, RevokeOn) takes it as the first statement of its Apply, before any principal
// or users row: step -1 of the auth lock order. Grant and Revoke take it again on that transaction, which holds it
// already (an advisory transaction lock a session holds is granted to it again at once).
func LockWrites(ctx context.Context, tx audit.TxOps) error { return lockWrites(ctx, tx) }

type outerTxKey struct{}

// GrantOn is Grant inside the caller's audit.Commit, after the caller took LockWrites: the same statements, locks and
// checks run on tx, and the grant's audit row and outbox event are queued on tx, so they commit or roll back with the
// caller's own change. A refusal is returned as itself.
func (s *Store) GrantOn(ctx context.Context, tx audit.TxOps, in GrantInput) (uuid.UUID, error) {
	return s.Grant(context.WithValue(ctx, outerTxKey{}, tx), in)
}

// RevokeOn is Revoke inside the caller's audit.Commit, after LockWrites: the assignment is read on tx too.
func (s *Store) RevokeOn(ctx context.Context, tx audit.TxOps, in RevokeInput) error {
	return s.Revoke(context.WithValue(ctx, outerTxKey{}, tx), in)
}

func outerTx(ctx context.Context) (audit.TxOps, bool) {
	tx, ok := ctx.Value(outerTxKey{}).(audit.TxOps)
	return tx, ok
}

// commit runs m in its own audit.Commit, or, inside GrantOn or RevokeOn, applies it on the caller's transaction and
// queues its audit row and events there.
func (s *Store) commit(ctx context.Context, m audit.Mutation) error {
	tx, ok := outerTx(ctx)
	if !ok {
		return audit.Commit(ctx, s.pool, s.schema, m)
	}
	if err := m.Apply(ctx, tx); err != nil {
		return err
	}
	if err := tx.AddAudit(append([]audit.AuditEvent{m.Audit}, m.Also...)...); err != nil {
		return err
	}
	return tx.AddEvent(m.Events...)
}

// reader is the caller's transaction inside GrantOn or RevokeOn, else the pool.
func (s *Store) reader(ctx context.Context) rowQuerier {
	if tx, ok := outerTx(ctx); ok {
		return tx
	}
	return s.pool
}

// IsRefusal reports whether err is one of the store's domain refusals (not a store failure).
func IsRefusal(err error) bool {
	for _, refusal := range refusals {
		if errors.Is(err, refusal) {
			return true
		}
	}
	return false
}

// RefuseLastPlatformAdmin is the guard of an erase of principal (an admin user delete) on the caller's transaction,
// after the caller took LockWrites: when principal holds an active platform_admin assignment and no other active one
// exists, it is ErrLastPlatformAdmin, the refusal Revoke gives (pm D6878: kept on demote and on erase). The principal
// rows of every holder are share-locked first, as Revoke locks them, so the count reads a clock after the waits.
func (s *Store) RefuseLastPlatformAdmin(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
	if err := lockPrincipals(ctx, tx, s.q); err != nil {
		return err
	}
	var holds bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+s.q+`.platform_role_assignments r JOIN `+s.q+`.principals p ON p.id = r.principal_id
WHERE r.principal_id = $1 AND r.role_key = $2 AND `+activeSQL+`)`, principal, PlatformAdmin).Scan(&holds); err != nil {
		return err
	}
	if !holds {
		return nil
	}
	admins, err := s.activeAdmins(ctx, tx)
	if err != nil {
		return err
	}
	if admins <= 1 {
		return ErrLastPlatformAdmin
	}
	return nil
}
