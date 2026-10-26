package local

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// IdentityStore is the part of the B2 *identity.Store the flows use. ByID
// returns nil, nil for an unknown principal.
type IdentityStore interface {
	ByEmail(ctx context.Context, normalizedEmail string) (*identity.Principal, error)
	ByID(ctx context.Context, id uuid.UUID) (*identity.Principal, error)
	ProvisionLocal(ctx context.Context, req identity.LocalProvisionRequest) (uuid.UUID, error)
	SetEmailVerified(ctx context.Context, principal uuid.UUID, at time.Time, actor identity.Actor) error
	SetPasswordHashFor(ctx context.Context, principal uuid.UUID, hash string, actor identity.Actor, cause identity.PasswordCause, end func(context.Context, audit.TxOps) error) (int64, error)
}

// SessionEnder ends every credential class of a principal and creates a
// session inside a caller's transaction (*session.Store).
type SessionEnder interface {
	EndAllInTx(ctx context.Context, tx audit.TxOps, principalID uuid.UUID) (session.Ended, error)
	CreateInTx(ctx context.Context, tx audit.TxOps, principalID uuid.UUID, organizationID *uuid.UUID, amr []string) (session.Issued, error)
	RecordEnded(ctx context.Context, ended session.Ended)
}

// ResetLinks is the reset-link store as the password transaction uses it
// (*PGLinks).
type ResetLinks interface {
	PeekResetLink(ctx context.Context, tokenHash [32]byte) (uuid.UUID, bool, error)
	ConsumeResetLinkInTx(ctx context.Context, tx audit.TxOps, principalID uuid.UUID, tokenHash [32]byte) error
	EndResetLinksInTx(ctx context.Context, tx audit.TxOps, principalID uuid.UUID) (int64, error)
}

// IdentityAccounts adapts the B2 store to Accounts. Every write is a
// self-service action: the audit row names the subject as its own actor.
type IdentityAccounts struct {
	Store IdentityStore
	// RequestID returns the audit request id of the call (C1's
	// requestid.Audit once it merges); nil means none.
	RequestID func(context.Context) *string
	// Sessions ends the principal's other credentials in the password
	// transaction (*session.Store).
	Sessions SessionEnder
	// Links consumes the presented reset link and ends the principal's other
	// unconsumed reset links in the same transaction.
	Links ResetLinks
	// Now is the verification time; nil means time.Now.
	Now func() time.Time
}

func (a IdentityAccounts) actor(ctx context.Context) identity.Actor {
	var id *string
	if a.RequestID != nil {
		id = a.RequestID(ctx)
	}
	return identity.SelfSignUp(id)
}

func userOf(p *identity.Principal) *User {
	if p == nil {
		return nil
	}
	return &User{ID: p.ID, IsActive: p.IsActive, IsVerified: p.IsVerified, PasswordHash: p.PasswordHash, AuthProvider: p.AuthProvider}
}

func (a IdentityAccounts) ByEmail(ctx context.Context, email string) (*User, error) {
	p, err := a.Store.ByEmail(ctx, email)
	return userOf(p), err
}

func (a IdentityAccounts) ByID(ctx context.Context, id uuid.UUID) (*User, error) {
	p, err := a.Store.ByID(ctx, id)
	return userOf(p), err
}

func (a IdentityAccounts) ProvisionLocal(ctx context.Context, req ProvisionLocalRequest) (uuid.UUID, error) {
	hash := req.PasswordHash
	var org *identity.NewOrganization
	if req.OrganizationName != nil {
		org = &identity.NewOrganization{Name: *req.OrganizationName}
	}
	return a.Store.ProvisionLocal(ctx, identity.LocalProvisionRequest{Email: req.Email, FullName: req.FullName, PasswordHash: &hash, Actor: a.actor(ctx), Organization: org})
}

func (a IdentityAccounts) SetEmailVerified(ctx context.Context, id uuid.UUID) error {
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	return a.Store.SetEmailVerified(ctx, id, now(), a.actor(ctx))
}

func (a IdentityAccounts) PeekResetLink(ctx context.Context, tokenHash [32]byte) (uuid.UUID, bool, error) {
	return a.Links.PeekResetLink(ctx, tokenHash)
}

func (a IdentityAccounts) SetPasswordEndingSessions(ctx context.Context, req PasswordEnd) (PasswordEndResult, error) {
	var (
		ended   session.Ended
		links   int64
		result  PasswordEndResult
		cause   = identity.PasswordChanged
		endFunc = func(ctx context.Context, tx audit.TxOps) error {
			if req.ResetLink != nil {
				if err := a.Links.ConsumeResetLinkInTx(ctx, tx, req.ID, *req.ResetLink); err != nil {
					return err
				}
			}
			var err error
			if ended, err = a.Sessions.EndAllInTx(ctx, tx, req.ID); err != nil {
				return err
			}
			if links, err = a.Links.EndResetLinksInTx(ctx, tx, req.ID); err != nil {
				return err
			}
			if req.NewSession != nil {
				issued, err := a.Sessions.CreateInTx(ctx, tx, req.ID, req.NewSession.OrganizationID, req.NewSession.AMR)
				if err != nil {
					return err
				}
				result.Issued = &issued
			}
			return nil
		}
	)
	if req.ResetLink != nil {
		cause = identity.PasswordReset
	}
	version, err := a.Store.SetPasswordHashFor(ctx, req.ID, req.Hash, a.actor(ctx), cause, endFunc)
	switch {
	case errors.Is(err, identity.ErrLinkUnusable):
		return PasswordEndResult{}, ErrResetLinkUnusable
	case err != nil:
		return PasswordEndResult{}, err
	}
	a.Sessions.RecordEnded(ctx, ended)
	slog.InfoContext(ctx, "password changed; credentials ended", "principal_id", req.ID.String(),
		"sessions", ended.Sessions, "refresh_tokens", ended.RefreshTokens, "reset_links", links, "token_version", version)
	return result, nil
}

var _ Accounts = IdentityAccounts{}
var _ IdentityStore = (*identity.Store)(nil)
