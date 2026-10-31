package platformrole

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// operatorAudit is the audit row of an auth-migrate owner verb: no actor
// principal, the operator CLI as its source.
func operatorAudit(eventType, reason, subject string) audit.AuditEvent {
	return audit.AuditEvent{EventType: eventType, Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
		Attributes: map[string]any{"reason_code": reason, "target_kind": "principal", "target_id": subject, "source": audit.SourceOperatorCLI}}
}

// refuseOwner records an API grant or revoke of platform_owner that Grant or
// Revoke refused. The refusal stands if the audit row cannot be written.
func (s *Store) refuseOwner(ctx context.Context, eventType string, actor, principal uuid.UUID) {
	err := audit.Append(ctx, s.pool, s.schema, ownerRefusalAudit(eventType, actor, principal))
	if err != nil {
		s.logger.ErrorContext(ctx, "platformrole: the platform_owner refusal could not be audited", "event_type", eventType)
	}
}

// GrantOwner gives principal the platform_owner role, permanent, granted by the
// principal itself (never a NULL granter), in one audit.Commit with no actor
// principal. It is the only grant path of the role and has no authority check:
// the caller is the operator, running as the migration role. It does not look at
// platform_admin: a principal may hold both roles as separate rows.
func (s *Store) GrantOwner(ctx context.Context, principal uuid.UUID) (uuid.UUID, error) {
	id := uuid.New()
	subject := principal.String()
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := lockWrites(ctx, tx); err != nil {
				return err
			}
			if err := lockPrincipals(ctx, tx, s.q, principal); err != nil {
				return err
			}
			var kind string
			var enabled bool
			err := tx.QueryRow(ctx, `SELECT kind, disabled_at IS NULL FROM `+s.q+`.principals WHERE id = $1`, principal).Scan(&kind, &enabled)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				return ErrPrincipalMissing
			case err != nil:
				return err
			case kind != "user":
				return ErrNotHumanUser
			case !enabled:
				return ErrPrincipalOff
			}
			var inserted uuid.UUID
			err = tx.QueryRow(ctx, `INSERT INTO `+s.q+`.platform_role_assignments (id, principal_id, role_key, granted_by)
VALUES ($1, $2, $3, $2) ON CONFLICT (principal_id, role_key) DO NOTHING RETURNING id`, id, principal, PlatformOwner).Scan(&inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrAlreadyHeld
			}
			return err
		},
		Audit:  operatorAudit("auth.platform_role.granted", "role_granted", subject),
		Events: []audit.OutboxEvent{roleEvent(id, "platform_role_assignment.granted", "active")},
	})
	if err != nil {
		return uuid.Nil, s.writeErr(ctx, "grant owner", err)
	}
	return id, nil
}

// RevokeOwner removes the platform_owner role from principal in one
// audit.Commit with no actor principal. There is no last-owner guard: a
// compromised owner is removed at once, and with no owner left the owner
// exemption is off. The principal may be disabled or expired.
func (s *Store) RevokeOwner(ctx context.Context, principal uuid.UUID) error {
	var target uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT id FROM `+s.q+`.platform_role_assignments WHERE principal_id = $1 AND role_key = $2`,
		principal, PlatformOwner).Scan(&target)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotHeld
	}
	if err != nil {
		return s.storeErr(ctx, "read owner revoke target", err)
	}
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := lockWrites(ctx, tx); err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `DELETE FROM `+s.q+`.platform_role_assignments WHERE id = $1 AND principal_id = $2 AND role_key = $3`,
				target, principal, PlatformOwner)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return ErrNotHeld
			}
			return nil
		},
		Audit:  operatorAudit("auth.platform_role.revoked", "role_revoked", principal.String()),
		Events: []audit.OutboxEvent{roleEvent(target, "platform_role_assignment.revoked", "revoked")},
	})
	if err != nil {
		return s.writeErr(ctx, "revoke owner", err)
	}
	return nil
}

// ActiveOwners counts the platform_owner assignments that are active now. Zero
// is a safe state: the owner exemption is off.
func (s *Store) ActiveOwners(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+s.q+`.platform_role_assignments r JOIN `+s.q+`.principals p ON p.id = r.principal_id
WHERE r.role_key = $1 AND `+activeSQL, PlatformOwner).Scan(&n)
	if err != nil {
		return 0, s.storeErr(ctx, "count owners", err)
	}
	return n, nil
}

func ownerRefusalAudit(eventType string, actor, principal uuid.UUID) audit.AuditEvent {
	actorID, subject := actor.String(), principal.String()
	return audit.AuditEvent{
		EventType: eventType, Outcome: audit.OutcomeDenied, ActorPrincipalID: &actorID, SubjectPrincipalID: &subject,
		Attributes: map[string]any{"reason_code": "denied_policy", "target_kind": "principal", "target_id": subject},
	}
}
