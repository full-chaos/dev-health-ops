package local

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// FlowKind is the typed result of a link or password flow. The wire mapping
// belongs to the route layer.
type FlowKind string

const (
	FlowOK              FlowKind = "ok"
	FlowInvalidToken    FlowKind = "invalid_token"
	FlowEmailTaken      FlowKind = "email_taken"
	FlowPolicy          FlowKind = "password_policy"
	FlowCurrentRejected FlowKind = "current_password_rejected"
	FlowManagedAccount  FlowKind = "password_managed_elsewhere"
	FlowNotAllowed      FlowKind = "not_allowed"
)

// MaxPasswordBytes is bcrypt's limit. go-api answers a longer password with a
// 500 (bcrypt ValueError); here it is a policy refusal.
const MaxPasswordBytes = 72

// ViolationTooLong is the violation of a password over MaxPasswordBytes.
const ViolationTooLong = "Password must be no more than 72 bytes long"

// FlowOutcome is the result of a flow. Violations is set on FlowPolicy,
// Issued on a successful Change.
type FlowOutcome struct {
	Kind        FlowKind
	PrincipalID uuid.UUID
	Violations  []string
	Issued      *session.Issued
}

// RegisterRequest is one registration. Email is not yet normalized.
type RegisterRequest struct {
	Email    string
	Password string
	FullName *string
}

// ChangeRequest is one signed-in password change. Impersonated is true for
// an impersonated session.
type ChangeRequest struct {
	PrincipalID    uuid.UUID
	OrganizationID *uuid.UUID
	Current, New   string
	Impersonated   bool
	Actor          session.Actor
}

// FlowsConfig builds Flows.
type FlowsConfig struct {
	Accounts Accounts
	Links    Links
	Sessions SessionAdmin
	Mail     Mailer
	// UsersOwnedByAuth mirrors the B2 switch (D3). Off, every flow returns
	// identity.ErrSignupNotAvailable before it reads or writes anything, so no
	// credential is consumed for a write that cannot happen.
	UsersOwnedByAuth bool
	// Compare checks a password against a hash; nil means bcrypt.
	Compare func(hash, password string) bool
	// Hash makes a password hash; nil means passwordhash.Hash.
	Hash func(password string) (string, error)
}

// Flows is register, verify, resend, forgot, reset and change password.
type Flows struct {
	accounts Accounts
	links    Links
	sessions SessionAdmin
	mail     Mailer
	owned    bool
	compare  func(hash, password string) bool
	hash     func(password string) (string, error)
}

// NewFlows validates the configuration.
func NewFlows(cfg FlowsConfig) (*Flows, error) {
	if cfg.Accounts == nil || cfg.Links == nil || cfg.Sessions == nil || cfg.Mail == nil {
		return nil, errors.New("local: accounts, links, sessions and mail are required")
	}
	f := &Flows{accounts: cfg.Accounts, links: cfg.Links, sessions: cfg.Sessions, mail: cfg.Mail,
		owned: cfg.UsersOwnedByAuth, compare: cfg.Compare, hash: cfg.Hash}
	if f.compare == nil {
		f.compare = bcryptMatches
	}
	if f.hash == nil {
		f.hash = passwordhash.Hash
	}
	return f, nil
}

var (
	_ = rand.Read
	_ = sha256.Sum256
	_ = base64.RawURLEncoding
	_ = fmt.Sprint
	_ = contracts.NewSecretString
	_ = identity.ErrEmailTaken
	_ = passwordpolicy.Validate
)

// Register creates a local account and sends its verification link.
func (f *Flows) Register(ctx context.Context, req RegisterRequest) (FlowOutcome, error) {
	return FlowOutcome{}, errNotImplemented
}

// Verify consumes a verification link and marks the e-mail verified.
func (f *Flows) Verify(ctx context.Context, secret string) (FlowOutcome, error) {
	return FlowOutcome{}, errNotImplemented
}

// Resend issues a new verification link for an unverified account. Its
// result and call sequence are the same for every address.
func (f *Flows) Resend(ctx context.Context, email string) error { return errNotImplemented }

// Forgot issues a password reset link. Its result and call sequence are the
// same for every address.
func (f *Flows) Forgot(ctx context.Context, email string) error { return errNotImplemented }

// Reset consumes a reset link, sets the password and revokes every session.
func (f *Flows) Reset(ctx context.Context, secret, newPassword string) (FlowOutcome, error) {
	return FlowOutcome{}, errNotImplemented
}

// Change sets a signed-in user's password, revokes every session and issues
// the caller a new one.
func (f *Flows) Change(ctx context.Context, req ChangeRequest) (FlowOutcome, error) {
	return FlowOutcome{}, errNotImplemented
}
