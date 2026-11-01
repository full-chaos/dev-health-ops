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

var (
	// ErrPredecessorNotLive refuses a superseding issue whose predecessor
	// session is unknown, revoked or past its effective expiry: the caller's
	// own session has ended (a concurrent switch from it already superseded
	// it), so no successor is issued.
	ErrPredecessorNotLive = errors.New("session: the predecessor session is not live")
	// ErrPredecessorOtherPrincipal refuses a predecessor that belongs to
	// another principal: a session is only ever superseded by its own user.
	ErrPredecessorOtherPrincipal = errors.New("session: the predecessor session belongs to another principal")
	// ErrPredecessorDelegated refuses a predecessor that is a delegated
	// session: a switch from it would mint the target a full session with a
	// refresh credential.
	ErrPredecessorDelegated = errors.New("session: the predecessor session is delegated")
)

// EventSessionRevoked is the audit event type of a session revoke.
const EventSessionRevoked = "auth.session.revoked"

// CreateSupersedingAudited is CreateAudited for a sign-in that replaces the
// caller's own session (an organization switch, CHAOS-12085): under the
// predecessor's row lock it revokes the predecessor and every credential of
// it, then creates the successor, with event (the successor's issue) and one
// auth.session.revoked row for the predecessor, in ONE audit.Commit
// transaction. Either both happen or neither does.
//
// A predecessor that is unknown, revoked or expired refuses the whole issue
// (ErrPredecessorNotLive), so two concurrent switches from one session leave
// at most one live successor: the second waits on the lock, then finds the
// predecessor revoked. A predecessor of another principal is
// ErrPredecessorOtherPrincipal. No other session of the principal is touched.
func (s *Store) CreateSupersedingAudited(ctx context.Context, principalID uuid.UUID, organizationID *uuid.UUID, amr []string,
	predecessor uuid.UUID, event audit.AuditEvent) (Issued, error) {
	if !validAMR(amr) {
		return Issued{}, ErrInvalidAMR
	}
	if event.RequestID == nil {
		event.RequestID = requestid.Audit(ctx)
	}
	principal, predecessorID := principalID.String(), predecessor.String()
	revoked := audit.AuditEvent{
		EventType: EventSessionRevoked, Outcome: audit.OutcomeAllowed,
		ActorPrincipalID: event.ActorPrincipalID, SubjectPrincipalID: &principal, OrganizationID: nil,
		RequestID: event.RequestID,
		Attributes: map[string]any{
			"reason_code": string(ReasonSessionRevoked), "target_kind": "session", "target_id": predecessorID,
			"source": "go-api-delegated",
		},
	}
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
			if err := s.revokeSessions(ctx, tx, []string{predecessorID}, revokedByCaller); err != nil {
				return err
			}
			out, err = s.insertSession(ctx, tx, principalID, organizationID, amr, contracts.SecretString{})
			return s.stampSignIn(ctx, tx, out.Session.ID, err)
		},
		Audit:     event,
		Also:      []audit.AuditEvent{revoked},
		OffStream: audit.OffStreamSessionLifecycle,
		Refusals:  []error{ErrPredecessorNotLive, ErrPredecessorOtherPrincipal, ErrPredecessorDelegated, ErrPrincipalUnavailable},
	})
	if err != nil {
		for _, refusal := range []error{ErrPredecessorNotLive, ErrPredecessorOtherPrincipal, ErrPredecessorDelegated, ErrPrincipalUnavailable} {
			if errors.Is(err, refusal) {
				s.logger.WarnContext(ctx, "superseding session issue refused",
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
