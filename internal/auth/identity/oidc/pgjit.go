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
// membership, the account (for a new one) and the session are one
// audit.Commit: a failure writes none of them. It is available only after the
// users cutover; before it every call is ErrSignupNotAvailable and writes
// nothing.
type PGJIT struct {
	Identities *identity.Store
	Sessions   *session.Store
	// Mint turns the stored session into the caller's tokens. It runs inside
	// the transaction, so a mint failure rolls the commit back.
	Mint func(issued session.Issued, req SessionRequest) (Tokens, error)
}

func (j PGJIT) sessionHook(req SessionRequest, tokens *Tokens, ran *bool) identity.AfterJoin {
	return func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
		issued, err := j.Sessions.InsertIn(ctx, tx, principal, req.OrgID, req.AMR)
		if err != nil {
			return err
		}
		req.PrincipalID = principal
		out, err := j.Mint(issued, req)
		if err != nil {
			return err
		}
		*tokens, *ran = out, true
		return nil
	}
}

// JoinAndIssue adds the principal to orgID as req.Role and issues the session
// in one transaction. A principal that already is a member just gets a session.
func (j PGJIT) JoinAndIssue(ctx context.Context, principalID, orgID uuid.UUID, req SessionRequest) (Tokens, error) {
	req.PrincipalID = principalID
	var tokens Tokens
	var ran bool
	err := j.Identities.JoinOrg(ctx, principalID, identity.OrgJoin{OrgID: orgID, Role: req.Role},
		identity.SelfSignUp(nil), j.sessionHook(req, &tokens, &ran))
	if err != nil {
		return Tokens{}, err
	}
	if ran {
		return tokens, nil
	}
	issued, err := j.Sessions.Create(ctx, principalID, req.OrgID, req.AMR)
	if err != nil {
		return Tokens{}, err
	}
	return j.Mint(issued, req)
}

// ProvisionJoinAndIssue creates the account, adds it to orgID as req.Role and
// issues the session in one transaction.
func (j PGJIT) ProvisionJoinAndIssue(ctx context.Context, account FederatedAccount, orgID uuid.UUID, req SessionRequest) (Principal, Tokens, error) {
	var tokens Tokens
	var ran bool
	id, err := j.Identities.ProvisionAndJoin(ctx, identity.ProvisionRequest{
		Key:   identity.ExternalKey{Issuer: account.Key.Issuer, Provider: identity.Provider(account.Key.Provider), Subject: account.Key.Subject},
		Email: account.Email, FullName: account.FullName, Actor: identity.SelfSignUp(account.RequestID),
	}, identity.OrgJoin{OrgID: orgID, Role: req.Role}, j.sessionHook(req, &tokens, &ran))
	switch {
	case errors.Is(err, identity.ErrIdentityTaken), errors.Is(err, identity.ErrEmailTaken):
		return Principal{}, Tokens{}, ErrIdentityConflict
	case err != nil:
		return Principal{}, Tokens{}, err
	}
	return Principal{ID: id, Email: account.Email, Active: true}, tokens, nil
}
