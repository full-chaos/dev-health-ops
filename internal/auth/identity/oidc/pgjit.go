package oidc

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// PGJIT is the MembershipJIT over the identity and session stores. The
// membership, the account (for a new one) and the session rows are one
// audit.Commit: a failure writes none of them. It is available only after the
// users cutover; before it every call is ErrSignupNotAvailable and writes
// nothing.
type PGJIT struct {
	Identities *identity.Store
	Sessions   *session.Store
	// Mint turns the stored session into the caller's tokens. It runs after the
	// commit and writes no state of the identity store.
	Mint func(ctx context.Context, issued session.Issued, req SessionRequest) (Tokens, error)
}

// sessionHook writes the session rows inside the join's transaction.
func (j PGJIT) sessionHook(req SessionRequest, issued *session.Issued, ran *bool) identity.AfterJoin {
	return func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
		out, err := j.Sessions.InsertIn(ctx, tx, principal, req.OrgID, req.AMR)
		if err != nil {
			return err
		}
		*issued, *ran = out, true
		return nil
	}
}

// JoinAndIssue adds the principal to orgID as req.Role and starts the session
// in one transaction. A principal that already is a member just gets a session.
func (j PGJIT) JoinAndIssue(ctx context.Context, principalID, orgID uuid.UUID, req SessionRequest) (Tokens, error) {
	req.PrincipalID = principalID
	var issued session.Issued
	var ran bool
	_, err := j.Identities.JoinOrg(ctx, principalID, identity.OrgJoin{OrgID: orgID, Role: req.Role},
		identity.SelfSignUp(nil), j.sessionHook(req, &issued, &ran))
	if err != nil {
		return Tokens{}, mapJITError(err)
	}
	if !ran {
		principal := principalID.String()
		event := audit.AuditEvent{
			EventType: "auth.login.succeeded", Outcome: audit.OutcomeAllowed,
			ActorPrincipalID: &principal, SubjectPrincipalID: &principal,
			Attributes: map[string]any{"reason_code": "accepted", "target_kind": "session", "provider": "oidc"},
		}
		if orgID != uuid.Nil {
			org := orgID.String()
			event.OrganizationID = &org
		}
		if issued, err = j.Sessions.CreateAudited(ctx, principalID, req.OrgID, req.AMR, event); err != nil {
			return Tokens{}, err
		}
	}
	return j.Mint(ctx, issued, req)
}

// ProvisionJoinAndIssue creates the account, adds it to orgID as req.Role and
// starts the session in one transaction.
func (j PGJIT) ProvisionJoinAndIssue(ctx context.Context, account FederatedAccount, orgID uuid.UUID, req SessionRequest) (Principal, Tokens, error) {
	var issued session.Issued
	var ran bool
	id, err := j.Identities.ProvisionAndJoin(ctx, identity.ProvisionRequest{
		Key:   identity.ExternalKey{Issuer: account.Key.Issuer, Provider: identity.Provider(account.Key.Provider), Subject: account.Key.Subject},
		Email: account.Email, FullName: account.FullName, Actor: identity.SelfSignUp(account.RequestID),
	}, identity.OrgJoin{OrgID: orgID, Role: req.Role}, func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
		req.PrincipalID = principal
		return j.sessionHook(req, &issued, &ran)(ctx, tx, principal)
	})
	switch {
	case errors.Is(err, identity.ErrIdentityTaken), errors.Is(err, identity.ErrEmailTaken):
		return Principal{}, Tokens{}, ErrIdentityConflict
	case err != nil:
		return Principal{}, Tokens{}, mapJITError(err)
	}
	req.PrincipalID = id
	tokens, err := j.Mint(ctx, issued, req)
	if err != nil {
		return Principal{}, Tokens{}, err
	}
	return Principal{ID: id, Email: account.Email, Active: true}, tokens, nil
}

// mapJITError turns the identity store's refusals into the flow's sentinels.
func mapJITError(err error) error {
	switch {
	case errors.Is(err, identity.ErrMembershipNotActive):
		return ErrNotOrgMember
	case errors.Is(err, identity.ErrPrincipalUnavailable):
		return ErrPrincipalDisabled
	}
	return err
}
