// Package local is password login for local accounts (CHAOS-10218): bcrypt
// verify against a real or dummy hash, lockout on auth.login_attempts and one
// call sequence on every refusal path.
package local

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// Kind is the typed result of a login. The wire mapping belongs to the route
// layer; this package only decides.
type Kind string

const (
	KindSuccess             Kind = "success"
	KindInvalidCredentials  Kind = "invalid_credentials"
	KindLocked              Kind = "locked"
	KindDisabled            Kind = "disabled"
	KindPasswordUnavailable Kind = "password_unavailable"
	KindEmailUnverified     Kind = "email_unverified"
)

// Reason is the auth-outcome.v1 reason_code of an outcome.
type Reason string

const (
	ReasonAccepted           Reason = "accepted"
	ReasonInvalidCredentials Reason = "invalid_credentials"
	ReasonAccountLocked      Reason = "account_locked"
	ReasonPrincipalDisabled  Reason = "principal_deactivated"
	ReasonEmailUnverified    Reason = "email_unverified"
)

// Outcome is the result of Login. Issued is set only on KindSuccess;
// RetryAfter only on KindLocked. PrincipalID is set whenever the user is
// known, refusals included, so the route can audit per organization.
type Outcome struct {
	Kind        Kind
	Reason      Reason
	RetryAfter  time.Duration
	PrincipalID uuid.UUID
	Issued      *session.Issued
}

// User is the part of an identity record that login reads. A nil
// PasswordHash is an account without a password.
type User struct {
	ID           uuid.UUID
	IsActive     bool
	IsVerified   bool
	PasswordHash *string
	AuthProvider *string
}

// Users is the B2 identity lookup. ByEmail takes the normalized email and
// returns nil, nil for an unknown user.
type Users interface {
	ByEmail(ctx context.Context, normalizedEmail string) (*User, error)
	// Project makes sure the auth.principals row exists; B1 needs it.
	Project(ctx context.Context, userID uuid.UUID) error
}

// Sessions is the B1 session issuer.
type Sessions interface {
	Create(ctx context.Context, principalID uuid.UUID, organizationID *uuid.UUID, amr []string) (session.Issued, error)
}

// Attempts is the lockout store over auth.login_attempts. identifier is the
// HMAC of the normalized email, never the email.
type Attempts interface {
	// Check reports a live lock. An expired lock is cleared on the way.
	Check(ctx context.Context, identifier []byte) (locked bool, remaining time.Duration, err error)
	// RecordFailure counts a failure. While a lock holds only updated_at moves.
	RecordFailure(ctx context.Context, identifier []byte) error
	// Clear forgets a successful identifier.
	Clear(ctx context.Context, identifier []byte) error
}

// Credentials is one login request.
type Credentials struct {
	Email          string
	Password       string
	OrganizationID *uuid.UUID
}

var errNotImplemented = errors.New("local: not implemented")
