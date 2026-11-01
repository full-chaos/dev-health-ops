package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// ErrMemberElsewhere refuses a sign-in join of an account that holds a
// membership in another organization. Nothing is written.
var ErrMemberElsewhere = errors.New("identity: the account is a member of another organization")

// ErrAlreadyMember reports a sign-in join of an account that became a member of
// the organization after the caller read it. Nothing is written.
var ErrAlreadyMember = errors.New("identity: the account already is a member of the organization")

// SSOJoin is the organization a federated sign-in joins and the role it joins with.
type SSOJoin struct {
	OrgID uuid.UUID
	Role  string
}

// AfterJoin runs in the join's transaction after its rows are written; an error
// rolls everything back.
type AfterJoin func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error

// SignInRole is the role a provider row's default_role gives a sign-in: empty is
// member, viewer, member and admin are themselves. A sign-in never grants owner
// or an unknown value: ok is false.
func SignInRole(raw string) (role string, ok bool) {
	switch raw {
	case "":
		return "member", true
	case "viewer", "member", "admin":
		return raw, true
	}
	return "", false
}

// JoinRefusalBranch names the log branch of a join refused as not a member; ""
// for any other error.
func JoinRefusalBranch(err error) string {
	switch {
	case errors.Is(err, ErrMemberElsewhere):
		return "member_elsewhere"
	case errors.Is(err, ErrOrganizationSuspended):
		return "organization_suspended"
	case errors.Is(err, ErrMembershipDiverged):
		return "membership_status_kept"
	}
	return ""
}

// signInJoinRefusals are the expected ends of a join; each one rolls back.
var signInJoinRefusals = []error{ErrMemberElsewhere, ErrAlreadyMember, ErrMembershipDiverged, ErrOrganizationSuspended,
	ErrOrganizationNotFound, ErrAccountInactive, ErrLegacyProjection}

func signInJoinRecords(membership, principal, org uuid.UUID, role string, actor Actor) (audit.AuditEvent, audit.OutboxEvent) {
	subject, target, orgText := principal.String(), membership.String(), org.String()
	added := audit.AuditEvent{EventType: "auth.membership.added", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
		OrganizationID: &orgText, RequestID: actor.RequestID,
		Attributes: map[string]any{"reason_code": "membership_added", "target_kind": "membership", "target_id": target, "new_status": "active"}}
	if actor.self {
		added.ActorPrincipalID = &subject
	}
	return added, membershipEvent(membership, "membership.activated", map[string]any{"status": "active", "role_id": "rol_" + role})
}

// JoinOnSignIn adds an existing account to join.OrgID and runs after, in one
// audit.Commit. An account that became a member since the caller read is
// ErrAlreadyMember. A membership in another organization is ErrMemberElsewhere,
// a disabled account ErrAccountInactive, a suspended organization
// ErrOrganizationSuspended, and an auth row with no public row (a status other
// than active) ErrMembershipDiverged: a sign-in never undoes a status. Before the
// users cutover it is ErrSignupNotAvailable. Each of these writes nothing.
func (s *Store) JoinOnSignIn(ctx context.Context, principal uuid.UUID, join SSOJoin, actor Actor, after AfterJoin) error {
	if !s.ownsUsers {
		return ErrSignupNotAvailable
	}
	if err := s.ProjectOrganization(ctx, join.OrgID); err != nil {
		return err
	}
	membership := uuid.New()
	added, activated := signInJoinRecords(membership, principal, join.OrgID, join.Role, actor)
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			err := s.insertJoinMembership(ctx, tx, membershipJoin{ID: membership, Principal: principal, OrgID: join.OrgID, Role: join.Role, SignIn: true})
			if err != nil {
				return err
			}
			if after != nil {
				return after(ctx, tx, principal)
			}
			return nil
		},
		Audit: added, Events: []audit.OutboxEvent{activated}, Refusals: signInJoinRefusals,
	})
	return s.signInJoinResult(ctx, err)
}

func (s *Store) signInJoinResult(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errJoinAlreadyMember) {
		return ErrAlreadyMember
	}
	for _, refusal := range signInJoinRefusals {
		if errors.Is(err, refusal) {
			return refusal
		}
	}
	return s.storeErr(ctx, "join an organization at sign-in", err)
}

// ProvisionAndJoin is ProvisionFederated with the account's membership in
// join.OrgID and after in its commit: a failure writes no principal, user,
// identity, membership or after row.
func (s *Store) ProvisionAndJoin(ctx context.Context, req ProvisionRequest, join SSOJoin, after AfterJoin) (uuid.UUID, error) {
	if !s.ownsUsers {
		return uuid.Nil, ErrSignupNotAvailable
	}
	if err := s.ProjectOrganization(ctx, join.OrgID); err != nil {
		return uuid.Nil, err
	}
	return s.provisionFederated(ctx, req, &join, after)
}
