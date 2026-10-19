package identity

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrIdentityConflict     = errors.New("identity: the external identity is linked to another principal")
	ErrAlreadyLinked        = errors.New("identity: the external identity is already linked to this principal")
	ErrNotFound             = errors.New("identity: external identity not linked")
	ErrNotOwner             = errors.New("identity: the external identity belongs to another principal")
	ErrLastSignInMethod     = errors.New("identity: the external identity is the last sign-in method")
	ErrPrincipalUnavailable = errors.New("identity: principal missing or disabled")
	ErrSignupNotAvailable   = errors.New("identity: sign-up through auth-service is not available")
	ErrIdentityTaken        = errors.New("identity: the external identity is already linked")
	ErrEmailTaken           = errors.New("identity: the email belongs to an existing principal")
	ErrInvalidLegacyMatch   = errors.New("identity: legacy match does not fit the provider")
)

// LegacyMatch finds a user go-api provisioned before the users cutover.
type LegacyMatch struct {
	AuthProvider  string
	MembershipOrg *uuid.UUID
	Subject       string
}

// Identity is one linked external identity.
type Identity struct {
	ID        uuid.UUID
	Issuer    string
	Provider  Provider
	Subject   string
	CreatedAt time.Time
}

// IdentityBySubject is a stub.
func (s *Store) IdentityBySubject(ctx context.Context, issuer string, provider Provider, subject string, legacy *LegacyMatch) (Principal, bool, error) {
	return Principal{}, false, errNotImplemented
}

// ProvisionRequest is a new federated user.
type ProvisionRequest struct {
	Key      ExternalKey
	Email    string
	FullName string
	Actor    Actor
}

// ProvisionFederated is a stub.
func (s *Store) ProvisionFederated(ctx context.Context, req ProvisionRequest) (uuid.UUID, error) {
	return uuid.Nil, errNotImplemented
}

// Actor names who asked for a change, for the audit row.
type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
}

// SelfSignUp is a stub.
func SelfSignUp(requestID *string) Actor { return Actor{RequestID: requestID} }

// LinkIdentity is a stub.
func (s *Store) LinkIdentity(ctx context.Context, key ExternalKey, principal uuid.UUID, actor Actor) (uuid.UUID, error) {
	return uuid.Nil, errNotImplemented
}

// UnlinkIdentity is a stub.
func (s *Store) UnlinkIdentity(ctx context.Context, key ExternalKey, principal uuid.UUID, actor Actor) error {
	return errNotImplemented
}

// ListIdentities is a stub.
func (s *Store) ListIdentities(ctx context.Context, principal uuid.UUID) ([]Identity, error) {
	return nil, errNotImplemented
}

// HasPassword is a stub.
func (s *Store) HasPassword(ctx context.Context, principal uuid.UUID) (bool, error) {
	return false, errNotImplemented
}
