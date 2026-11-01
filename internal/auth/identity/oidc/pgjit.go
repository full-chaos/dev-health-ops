package oidc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// PGJIT is the MembershipJIT over the identity and session stores: the
// membership, a new account and the session rows are one audit.Commit. The
// tokens are minted after the commit, so a mint failure leaves the rows and the
// next sign-in finds a member. It needs the users cutover: before it every call
// is ErrSignupNotAvailable and writes nothing.
type PGJIT struct {
	Identities *identity.Store
	Sessions   *session.Store
	// LoginEvent is the sign-in's audit row, written in the commit.
	LoginEvent func(ctx context.Context, principal uuid.UUID, org *uuid.UUID) audit.AuditEvent
	// Mint turns the stored session into the caller's tokens.
	Mint   func(ctx context.Context, issued session.Issued, req SessionRequest) (Tokens, error)
	Logger *slog.Logger
}

func (j PGJIT) refused(ctx context.Context, orgID uuid.UUID, err error) error {
	mapped := mapJITError(err)
	if branch := identity.JoinRefusalBranch(err); branch != "" {
		logger := j.Logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.WarnContext(ctx, "auth oidc: sign-in join refused", "branch", branch, "org_id", orgID)
	}
	return mapped
}

func (j PGJIT) sessionHook(req SessionRequest, issued *session.Issued) identity.AfterJoin {
	return func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
		out, err := j.Sessions.InsertIn(ctx, tx, principal, req.OrgID, req.AMR, j.LoginEvent(ctx, principal, req.OrgID))
		*issued = out
		return err
	}
}

// JoinAndIssue adds the principal to orgID as req.Role and starts the session in
// one commit. An account that became a member since the caller read is
// ErrAlreadyMember.
func (j PGJIT) JoinAndIssue(ctx context.Context, principalID, orgID uuid.UUID, req SessionRequest) (Tokens, error) {
	req.PrincipalID = principalID
	var issued session.Issued
	err := j.Identities.JoinOnSignIn(ctx, principalID, identity.SSOJoin{OrgID: orgID, Role: req.Role}, identity.SelfSignUp(nil), j.sessionHook(req, &issued))
	if err != nil {
		return Tokens{}, j.refused(ctx, orgID, err)
	}
	return j.Mint(ctx, issued, req)
}

// ProvisionJoinAndIssue creates the account, adds it to orgID as req.Role and
// starts the session in one commit.
func (j PGJIT) ProvisionJoinAndIssue(ctx context.Context, account FederatedAccount, orgID uuid.UUID, req SessionRequest) (Principal, Tokens, error) {
	var issued session.Issued
	id, err := j.Identities.ProvisionAndJoin(ctx, identity.ProvisionRequest{
		Key:   identity.ExternalKey{Issuer: account.Key.Issuer, Provider: identity.Provider(account.Key.Provider), Subject: account.Key.Subject},
		Email: account.Email, FullName: account.FullName, Actor: identity.SelfSignUp(account.RequestID),
		LegacyProvider: ProviderKind, LegacySubject: account.Key.Subject, LegacyFullName: account.LegacyFullName,
	}, identity.SSOJoin{OrgID: orgID, Role: req.Role}, j.sessionHook(req, &issued))
	if err != nil {
		return Principal{}, Tokens{}, j.refused(ctx, orgID, err)
	}
	req.PrincipalID = id
	tokens, err := j.Mint(ctx, issued, req)
	if err != nil {
		return Principal{}, Tokens{}, err
	}
	return Principal{ID: id, Email: account.Email, Active: true}, tokens, nil
}

// mapJITError turns the identity and session stores' refusals into the flow's
// sentinels; the cause stays in the chain for the log branch.
func mapJITError(err error) error {
	switch {
	case errors.Is(err, identity.ErrMemberElsewhere), errors.Is(err, identity.ErrOrganizationSuspended),
		errors.Is(err, identity.ErrMembershipDiverged):
		return fmt.Errorf("%w: %w", ErrNotOrgMember, err)
	case errors.Is(err, identity.ErrAccountInactive), errors.Is(err, session.ErrPrincipalUnavailable):
		return fmt.Errorf("%w: %w", ErrPrincipalDisabled, err)
	case errors.Is(err, identity.ErrIdentityTaken), errors.Is(err, identity.ErrEmailTaken):
		return fmt.Errorf("%w: %w", ErrIdentityConflict, err)
	}
	return err
}
