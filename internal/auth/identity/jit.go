package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// OrgJoin is the organization an account joins and the role it joins with.
type OrgJoin struct {
	OrgID uuid.UUID
	Role  string
}

// AfterJoin runs inside the join's transaction after the membership row is
// written; an error rolls everything back.
type AfterJoin func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error

const insertMembershipSQL = `INSERT INTO public.memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, clock_timestamp(), clock_timestamp(), clock_timestamp())`

func insertMembership(ctx context.Context, tx audit.TxOps, principal uuid.UUID, join OrgJoin) error {
	_, err := tx.Exec(ctx, insertMembershipSQL, uuid.New(), join.OrgID, principal, join.Role)
	return err
}

// ProvisionAndJoin is ProvisionFederated plus the account's membership in
// join.OrgID and after, all in one audit.Commit: a failure writes no principal,
// user, identity, membership or after row. It is available only after the
// users cutover (ErrSignupNotAvailable before) and refuses before any write.
func (s *Store) ProvisionAndJoin(ctx context.Context, req ProvisionRequest, join OrgJoin, after AfterJoin) (uuid.UUID, error) {
	if !s.ownsUsers {
		return uuid.Nil, ErrSignupNotAvailable
	}
	return s.provisionFederated(ctx, req, func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
		if err := insertMembership(ctx, tx, principal, join); err != nil {
			return err
		}
		if after != nil {
			return after(ctx, tx, principal)
		}
		return nil
	})
}

// JoinOrg adds an existing account to join.OrgID, then runs after, in one
// audit.Commit (reason membership_added). An account that already is a member
// is left as it is and after does not run. It is gated like ProvisionFederated:
// before the users cutover it is ErrSignupNotAvailable and writes nothing. A
// missing or disabled account is ErrPrincipalUnavailable.
func (s *Store) JoinOrg(ctx context.Context, principal uuid.UUID, join OrgJoin, actor Actor, after AfterJoin) error {
	if !s.ownsUsers {
		return ErrSignupNotAvailable
	}
	var member bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.memberships WHERE user_id = $1 AND org_id = $2)`,
		principal, join.OrgID).Scan(&member); err != nil {
		return s.storeErr(ctx, "read membership", err)
	}
	if member {
		return nil
	}
	subject := principal.String()
	event := audit.AuditEvent{
		EventType: "auth.membership.added", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject, RequestID: actor.RequestID,
		Attributes: map[string]any{"reason_code": "membership_added", "target_kind": "principal", "target_id": subject},
	}
	if actor.self {
		event.ActorPrincipalID = &subject
	}
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.lockPrincipal(ctx, tx, principal, true); err != nil {
				return err
			}
			if err := insertMembership(ctx, tx, principal, join); err != nil {
				return err
			}
			if after != nil {
				return after(ctx, tx, principal)
			}
			return nil
		},
		Audit:     event,
		OffStream: audit.OffStreamMembershipJoin,
	})
	if errors.Is(err, ErrPrincipalUnavailable) {
		return ErrPrincipalUnavailable
	}
	if err != nil {
		return s.storeErr(ctx, "join organization", err)
	}
	return nil
}
