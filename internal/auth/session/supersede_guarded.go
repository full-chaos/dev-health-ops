package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// supersedeRefusals are the refusals of a guarded supersede: nothing is
// written for any of them.
var supersedeRefusals = []error{ErrPredecessorNotLive, ErrPredecessorOtherPrincipal, ErrPredecessorDelegated,
	ErrPrincipalUnavailable, ErrOrganizationUnavailable, ErrGuardRefused}

// CreateSupersedingGuarded is the organization switch auth-service serves
// itself (POST /api/v1/auth/switch-org): CreateSupersedingAudited with guard
// run in the same transaction, after the predecessor's row lock and before
// anything is written, and with the context's PublicRow (WithPublicRow) run
// after the successor is inserted.
//
// Lock order: the predecessor's session row FOR UPDATE (as an exchange takes
// it), then the principal and the organization FOR KEY SHARE (steps 1 and 3
// of the auth lock order; the successor's foreign keys take the same locks),
// then guard, which may lock the membership (step 5). A member removal holds
// the principal FOR UPDATE before the membership and ends the pair's sessions
// NOWAIT, so it never waits on this transaction in a cycle.
//
// A guard that returns ErrGuardRefused refuses the switch: the predecessor
// stays live and no session is created. Any other guard error fails it.
func (s *Store) CreateSupersedingGuarded(ctx context.Context, principalID uuid.UUID, organizationID *uuid.UUID, amr []string,
	predecessor uuid.UUID, event audit.AuditEvent, guard Guard) (Issued, error) {
	if !validAMR(amr) {
		return Issued{}, ErrInvalidAMR
	}
	if guard == nil {
		return Issued{}, fmt.Errorf("%w: a guarded supersede needs its guard", ErrStore)
	}
	if event.RequestID == nil {
		event.RequestID = requestid.Audit(ctx)
	}
	principal, predecessorID := principalID.String(), predecessor.String()
	revoked := audit.AuditEvent{
		EventType: EventSessionRevoked, Outcome: audit.OutcomeAllowed,
		ActorPrincipalID: event.ActorPrincipalID, SubjectPrincipalID: &principal,
		RequestID: event.RequestID,
		Attributes: map[string]any{
			"reason_code": string(ReasonSessionRevoked), "target_kind": "session", "target_id": predecessorID,
		},
	}
	public := publicRowOf(ctx)
	var out Issued
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			locked, err := s.lockSession(ctx, tx, predecessorID)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				return ErrPredecessorNotLive
			case err != nil:
				return err
			case locked.PrincipalID != principalID:
				return ErrPredecessorOtherPrincipal
			case locked.RevokedAt != nil || locked.expired:
				return ErrPredecessorNotLive
			case locked.Delegation != nil:
				return ErrPredecessorDelegated
			}
			if _, err := tx.Exec(ctx, `SELECT 1 FROM `+s.q+`.principals WHERE id = $1 FOR KEY SHARE`, principal); err != nil {
				return err
			}
			if organizationID != nil {
				if _, err := tx.Exec(ctx, `SELECT 1 FROM `+s.q+`.organizations WHERE id = $1 FOR KEY SHARE`, organizationID.String()); err != nil {
					return err
				}
			}
			if err := runGuard(ctx, tx, locked.Record, guard); err != nil {
				return err
			}
			if err := s.revokeSessions(ctx, tx, []string{predecessorID}, revokedByCaller); err != nil {
				return err
			}
			out, err = s.insertSession(ctx, tx, principalID, organizationID, amr, contracts.SecretString{})
			if err := s.stampSignIn(ctx, tx, out.Session.ID, err); err != nil {
				return err
			}
			if public != nil {
				return public(ctx, tx, out.Session.PrincipalID, out.Session.OrganizationID)
			}
			return nil
		},
		Audit:     event,
		Also:      []audit.AuditEvent{revoked},
		OffStream: audit.OffStreamSessionLifecycle,
		Refusals:  supersedeRefusals,
	})
	if err != nil {
		for _, refusal := range supersedeRefusals {
			if errors.Is(err, refusal) {
				s.logger.WarnContext(ctx, "guarded superseding session issue refused",
					"principal_id", principal, "predecessor_session_id", predecessorID, "reason", refusal.Error())
				return Issued{}, refusal
			}
		}
		return Issued{}, fmt.Errorf("%w: superseding a session: %w", ErrStore, err)
	}
	s.revocations.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", revokedByCaller)))
	s.logger.InfoContext(ctx, "session superseded", "principal_id", principal,
		"predecessor_session_id", predecessorID, "session_id", out.Session.ID.String())
	return out, nil
}
