package local

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
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

// MaxPasswordBytes is bcrypt's limit. A longer password is a policy refusal.
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

func (f *Flows) gate() error {
	if !f.owned {
		return identity.ErrSignupNotAvailable
	}
	return nil
}

// newSecret is a 32-byte random URL-safe secret and the SHA-256 of its text,
// the only form that is stored.
func newSecret() (contracts.SecretString, [32]byte, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return contracts.SecretString{}, [32]byte{}, fmt.Errorf("local: no randomness for a link secret: %w", err)
	}
	text := base64.RawURLEncoding.EncodeToString(raw)
	return contracts.NewSecretString(text), sha256.Sum256([]byte(text)), nil
}

// issueAndSend stores a new credential and sends its link. A zero principal
// is a decoy: the same two calls, nothing stored and nothing sent.
func (f *Flows) issueAndSend(ctx context.Context, principal uuid.UUID, purpose Purpose, ttl time.Duration, to string, name *string) error {
	secret, hash, err := newSecret()
	if err != nil {
		return err
	}
	if err := f.links.Issue(ctx, IssueRequest{PrincipalID: principal, Purpose: purpose, TokenHash: hash, TTL: ttl}); err != nil {
		return fmt.Errorf("local: issuing a link credential: %w", err)
	}
	msg := Message{Purpose: purpose, To: to, FullName: name, Secret: secret, Deliver: principal != uuid.Nil}
	if err := f.mail.Send(ctx, msg); err != nil {
		slog.WarnContext(ctx, "local: link mail not sent", "purpose", string(purpose), logging.ErrorAttr(err))
	}
	return nil
}

func passwordRefusal(password string, applyPolicy bool) []string {
	if applyPolicy {
		if v := passwordpolicy.Validate(password); len(v) > 0 {
			return v
		}
	}
	if len(password) > MaxPasswordBytes {
		return []string{ViolationTooLong}
	}
	return nil
}

// Register creates a local account and sends its verification link.
func (f *Flows) Register(ctx context.Context, req RegisterRequest) (FlowOutcome, error) {
	if err := f.gate(); err != nil {
		return FlowOutcome{}, err
	}
	email, err := identity.NormalizeEmail(req.Email)
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: %w", err)
	}
	if v := passwordRefusal(req.Password, true); v != nil {
		return FlowOutcome{Kind: FlowPolicy, Violations: v}, nil
	}
	existing, err := f.accounts.ByEmail(ctx, email)
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: loading the user: %w", err)
	}
	if existing != nil {
		return FlowOutcome{Kind: FlowEmailTaken}, nil
	}
	hash, err := f.hash(req.Password)
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: hashing the password: %w", err)
	}
	id, err := f.accounts.ProvisionLocal(ctx, ProvisionLocalRequest{Email: email, FullName: req.FullName, PasswordHash: hash})
	if errors.Is(err, identity.ErrEmailTaken) {
		return FlowOutcome{Kind: FlowEmailTaken}, nil
	}
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: creating the account: %w", err)
	}
	if err := f.issueAndSend(ctx, id, PurposeEmailVerification, VerificationTTL, email, req.FullName); err != nil {
		return FlowOutcome{}, err
	}
	return FlowOutcome{Kind: FlowOK, PrincipalID: id}, nil
}

// Verify consumes a verification link and marks the e-mail verified.
func (f *Flows) Verify(ctx context.Context, secret string) (FlowOutcome, error) {
	if err := f.gate(); err != nil {
		return FlowOutcome{}, err
	}
	id, ok, err := f.links.Redeem(ctx, RedeemRequest{Purpose: PurposeEmailVerification, TokenHash: sha256.Sum256([]byte(secret))})
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: redeeming a verification link: %w", err)
	}
	if !ok {
		return FlowOutcome{Kind: FlowInvalidToken}, nil
	}
	if err := f.accounts.SetEmailVerified(ctx, id); err != nil {
		return FlowOutcome{}, fmt.Errorf("local: marking the e-mail verified: %w", err)
	}
	return FlowOutcome{Kind: FlowOK, PrincipalID: id}, nil
}

// Resend issues a new verification link for an unverified account. Its
// result and call sequence are the same for every address.
func (f *Flows) Resend(ctx context.Context, email string) error {
	return f.reissue(ctx, email, PurposeEmailVerification, VerificationTTL, func(u *User) bool { return !u.IsVerified })
}

// Forgot issues a password reset link. Its result and call sequence are the
// same for every address.
func (f *Flows) Forgot(ctx context.Context, email string) error {
	return f.reissue(ctx, email, PurposePasswordReset, ResetTTL, func(*User) bool { return true })
}

func (f *Flows) reissue(ctx context.Context, email string, purpose Purpose, ttl time.Duration, eligible func(*User) bool) error {
	if err := f.gate(); err != nil {
		return err
	}
	normalized, err := identity.NormalizeEmail(email)
	if err != nil {
		return fmt.Errorf("local: %w", err)
	}
	user, err := f.accounts.ByEmail(ctx, normalized)
	if err != nil {
		return fmt.Errorf("local: loading the user: %w", err)
	}
	principal := uuid.Nil
	if user != nil && eligible(user) {
		principal = user.ID
	}
	return f.issueAndSend(ctx, principal, purpose, ttl, normalized, nil)
}

// Reset consumes a reset link, sets the password and revokes every session.
func (f *Flows) Reset(ctx context.Context, secret, newPassword string) (FlowOutcome, error) {
	if err := f.gate(); err != nil {
		return FlowOutcome{}, err
	}
	// go-api applies no strength policy on reset beyond the model's length.
	// The refusal comes before the consume, so the link stays usable.
	if v := passwordRefusal(newPassword, false); v != nil {
		return FlowOutcome{Kind: FlowPolicy, Violations: v}, nil
	}
	id, ok, err := f.links.Redeem(ctx, RedeemRequest{Purpose: PurposePasswordReset, TokenHash: sha256.Sum256([]byte(secret))})
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: redeeming a reset link: %w", err)
	}
	if !ok {
		return FlowOutcome{Kind: FlowInvalidToken}, nil
	}
	hash, err := f.hash(newPassword)
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: hashing the password: %w", err)
	}
	if err := f.accounts.SetPasswordHash(ctx, id, hash); err != nil {
		return FlowOutcome{}, fmt.Errorf("local: setting the password: %w", err)
	}
	if _, err := f.sessions.RevokeAll(ctx, id, session.Actor{PrincipalID: &id}); err != nil {
		return FlowOutcome{}, fmt.Errorf("local: revoking sessions: %w", err)
	}
	return FlowOutcome{Kind: FlowOK, PrincipalID: id}, nil
}

// Change sets a signed-in user's password, revokes every session and issues
// the caller a new one.
func (f *Flows) Change(ctx context.Context, req ChangeRequest) (FlowOutcome, error) {
	if err := f.gate(); err != nil {
		return FlowOutcome{}, err
	}
	if req.Impersonated {
		return FlowOutcome{Kind: FlowNotAllowed}, nil
	}
	if v := passwordRefusal(req.New, true); v != nil {
		return FlowOutcome{Kind: FlowPolicy, Violations: v}, nil
	}
	user, err := f.accounts.ByID(ctx, req.PrincipalID)
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: loading the user: %w", err)
	}
	if user == nil || !user.IsActive {
		return FlowOutcome{Kind: FlowNotAllowed}, nil
	}
	provider := "local"
	if user.AuthProvider != nil {
		provider = *user.AuthProvider
	}
	if provider != "local" || user.PasswordHash == nil {
		return FlowOutcome{Kind: FlowManagedAccount}, nil
	}
	if !f.compare(*user.PasswordHash, req.Current) {
		return FlowOutcome{Kind: FlowCurrentRejected}, nil
	}
	hash, err := f.hash(req.New)
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: hashing the password: %w", err)
	}
	if err := f.accounts.SetPasswordHash(ctx, user.ID, hash); err != nil {
		return FlowOutcome{}, fmt.Errorf("local: setting the password: %w", err)
	}
	actor := req.Actor
	if actor.PrincipalID == nil {
		id := user.ID
		actor.PrincipalID = &id
	}
	if _, err := f.sessions.RevokeAll(ctx, user.ID, actor); err != nil {
		return FlowOutcome{}, fmt.Errorf("local: revoking sessions: %w", err)
	}
	issued, err := f.sessions.Create(ctx, user.ID, req.OrganizationID, []string{"pwd"})
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: creating the session: %w", err)
	}
	return FlowOutcome{Kind: FlowOK, PrincipalID: user.ID, Issued: &issued}, nil
}
