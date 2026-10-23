package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

// ErrMembershipNotActive reports a join onto an organization where the account
// already has a membership that is not active; a join never reactivates it.
var ErrMembershipNotActive = errors.New("identity: the account has a membership in the organization that is not active")

// OrgJoin is the organization an account joins and the role it joins with.
type OrgJoin struct {
	OrgID uuid.UUID
	Role  string
}

// AfterJoin runs inside the join's transaction after the membership row is
// written; an error rolls everything back.
type AfterJoin func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error

func (s *Store) insertMembership(ctx context.Context, tx audit.TxOps, id, principal uuid.UUID, join OrgJoin) error {
	_, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.memberships (id, organization_id, principal_id, status, role, joined_at)
VALUES ($1, $2, $3, 'active', $4, clock_timestamp())`, id, join.OrgID, principal, join.Role)
	return err
}

func membershipActivated(id uuid.UUID) audit.OutboxEvent {
	return audit.OutboxEvent{
		AggregateType: "membership", AggregateID: provisioning.WireID("membership", id.String()),
		EventType: "membership.activated", Change: map[string]any{"status": "active"},
	}
}

func membershipAudit(principal, membership uuid.UUID, org uuid.UUID, actor Actor) audit.AuditEvent {
	subject, target, orgText := principal.String(), membership.String(), org.String()
	event := audit.AuditEvent{
		EventType: "auth.membership.added", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
		OrganizationID: &orgText, RequestID: actor.RequestID,
		Attributes: map[string]any{"reason_code": "membership_added", "target_kind": "membership", "target_id": target},
	}
	if actor.self {
		a := subject
		event.ActorPrincipalID = &a
	}
	return event
}

// ProvisionAndJoin is ProvisionFederated plus the account's active membership
// in join.OrgID and after, all in one audit.Commit: a failure writes no
// principal, user, identity, membership or after row. It is available only
// after the users cutover (ErrSignupNotAvailable before) and refuses before
// any write. The organization is projected into the auth schema first (its own
// idempotent commit, not part of the rollback).
func (s *Store) ProvisionAndJoin(ctx context.Context, req ProvisionRequest, join OrgJoin, after AfterJoin) (uuid.UUID, error) {
	if !s.ownsUsers {
		return uuid.Nil, ErrSignupNotAvailable
	}
	if err := s.ProjectOrganization(ctx, join.OrgID); err != nil {
		return uuid.Nil, err
	}
	membership := uuid.New()
	return s.provisionFederated(ctx, req, &extraWrite{
		events: []audit.OutboxEvent{membershipActivated(membership)},
		apply: func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
			if err := s.insertMembership(ctx, tx, membership, principal, join); err != nil {
				return err
			}
			if after != nil {
				return after(ctx, tx, principal)
			}
			return nil
		},
	})
}

// JoinOrg adds an existing account to join.OrgID, then runs after, in one
// audit.Commit (reason membership_added). An account that already is an active
// member is left as it is and after does not run; one whose membership is not
// active is ErrMembershipNotActive. It is gated like ProvisionFederated:
// before the users cutover it is ErrSignupNotAvailable and writes nothing. A
// missing or disabled account is ErrPrincipalUnavailable.
func (s *Store) JoinOrg(ctx context.Context, principal uuid.UUID, join OrgJoin, actor Actor, after AfterJoin) (joined bool, err error) {
	if !s.ownsUsers {
		return false, ErrSignupNotAvailable
	}
	if err := s.ProjectOrganization(ctx, join.OrgID); err != nil {
		return false, err
	}
	membership := uuid.New()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.lockPrincipal(ctx, tx, principal, true); err != nil {
				return err
			}
			var status string
			switch err := tx.QueryRow(ctx, `SELECT status FROM `+s.q+`.memberships WHERE organization_id = $1 AND principal_id = $2`,
				join.OrgID, principal).Scan(&status); {
			case err == nil && status == "active":
				return errAlreadyMember
			case err == nil:
				return ErrMembershipNotActive
			case !errors.Is(err, pgx.ErrNoRows):
				return err
			}
			if err := s.insertMembership(ctx, tx, membership, principal, join); err != nil {
				return err
			}
			if after != nil {
				return after(ctx, tx, principal)
			}
			return nil
		},
		Audit:  membershipAudit(principal, membership, join.OrgID, actor),
		Events: []audit.OutboxEvent{membershipActivated(membership)},
	})
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, errAlreadyMember):
		return false, nil
	case errors.Is(err, ErrPrincipalUnavailable), errors.Is(err, ErrMembershipNotActive):
		return false, err
	}
	return false, s.storeErr(ctx, "join organization", err)
}

var errAlreadyMember = errors.New("identity: already a member")
