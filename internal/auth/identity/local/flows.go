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
	FlowPolicy          FlowKind = "password_policy"
	FlowCurrentRejected FlowKind = "current_password_rejected"
	FlowManagedAccount  FlowKind = "password_managed_elsewhere"
	FlowNotAllowed      FlowKind = "not_allowed"
)

// MaxPasswordBytes is bcrypt's limit. A longer password is a policy refusal.
// It is checked after the shared policy, which counts characters, not bytes.
const MaxPasswordBytes = passwordpolicy.MaxBytes

// ViolationTooLong is the violation of a password over MaxPasswordBytes.
const ViolationTooLong = passwordpolicy.ViolationTooLong

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
	// Locker serializes the flows that set a password, per principal.
	Locker Locker
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
	locker   Locker
	owned    bool
	compare  func(hash, password string) bool
	hash     func(password string) (string, error)
}

// NewFlows validates the configuration.
func NewFlows(cfg FlowsConfig) (*Flows, error) {
	if cfg.Accounts == nil || cfg.Links == nil || cfg.Sessions == nil || cfg.Mail == nil || cfg.Locker == nil {
		return nil, errors.New("local: accounts, links, sessions, mail and locker are required")
	}
	f := &Flows{accounts: cfg.Accounts, links: cfg.Links, sessions: cfg.Sessions, mail: cfg.Mail, locker: cfg.Locker,
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

// passwordRefusal is the one password policy of the flows that set a
// password: passwordpolicy.Validate (length, letter, digit, denylist), then
// bcrypt's 72-byte limit. It runs before anything is consumed or written.
// Every path that sets a password applies the same rule, so a password that
// registration refuses is refused on reset and change too:
//   - Register, Reset and Change call this function;
//   - the go-api session reset route calls passwordpolicy.ValidateForHash,
//     the same function, so the byte limit is the same in both services;
//   - the admin create-user and set-password routes call the same policy.
//
// The bounds are passwordpolicy's own (12 to 128 characters). The request
// model's looser 8 to 128 check runs first in the handlers and only decides
// the shape of a length error; it never accepts what the policy refuses.
// A violation list is returned in the policy's order, then the byte limit.
// Sign-in never calls this: it compares a stored hash, so an account whose
// password predates the policy still signs in.
// Violation strings are the ones go-api returned on register and change,
// so a client that parses them sees no new text for the same mistake.
// A password over the byte limit gets ViolationTooLong, in the same
// violation list shape as a policy violation.
// The byte limit exists because bcrypt reads at most 72 bytes of input;
// a longer password would otherwise fail later, at hash time, with an
// error instead of a refusal the caller can show.
// The text of every violation lives in passwordpolicy, not in this package.
// This function adds nothing of its own to that list.
func passwordRefusal(password string) []string {
	return passwordpolicy.ValidateForHash(password)
}

// Register creates a local account and sends its verification link. The
// outcome carries no id: a taken address gets the same one.
func (f *Flows) Register(ctx context.Context, req RegisterRequest) (FlowOutcome, error) {
	if err := f.gate(); err != nil {
		return FlowOutcome{}, err
	}
	email, err := identity.NormalizeEmail(req.Email)
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: %w", err)
	}
	if v := passwordRefusal(req.Password); v != nil {
		return FlowOutcome{Kind: FlowPolicy, Violations: v}, nil
	}
	existing, err := f.accounts.ByEmail(ctx, email)
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: loading the user: %w", err)
	}
	hash, err := f.hash(req.Password)
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: hashing the password: %w", err)
	}
	if existing != nil {
		return f.noticeExisting(ctx, email), nil
	}
	id, err := f.accounts.ProvisionLocal(ctx, ProvisionLocalRequest{Email: email, FullName: req.FullName, PasswordHash: hash})
	if errors.Is(err, identity.ErrEmailTaken) {
		return f.noticeExisting(ctx, email), nil
	}
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: creating the account: %w", err)
	}
	if err := f.issueAndSend(ctx, id, PurposeEmailVerification, VerificationTTL, email, req.FullName); err != nil {
		return FlowOutcome{}, err
	}
	return FlowOutcome{Kind: FlowOK}, nil
}

// noticeExisting answers a sign-up for a taken address like an accepted one and
// tells the owner by mail; the hash of the submitted password is discarded.
func (f *Flows) noticeExisting(ctx context.Context, email string) FlowOutcome {
	if err := f.mail.Send(ctx, Message{Purpose: PurposeAccountExists, To: email, Deliver: true}); err != nil {
		slog.WarnContext(ctx, "local: account notice not sent", logging.ErrorAttr(err))
	}
	return FlowOutcome{Kind: FlowOK}
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

// Reset consumes a reset link, sets the password and ends every credential.
// The link is consumed in the password transaction, after the user lock: link,
// password and ends commit together, a failed write leaves the link usable,
// and a reset whose link another write used first changes nothing.
func (f *Flows) Reset(ctx context.Context, secret, newPassword string) (FlowOutcome, error) {
	if err := f.gate(); err != nil {
		return FlowOutcome{}, err
	}
	// The shared policy applies on reset as on register and change; the
	// refusal comes before the consume, so the link stays usable.
	if v := passwordRefusal(newPassword); v != nil {
		return FlowOutcome{Kind: FlowPolicy, Violations: v}, nil
	}
	digest := sha256.Sum256([]byte(secret))
	id, ok, err := f.accounts.PeekResetLink(ctx, digest)
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: reading a reset link: %w", err)
	}
	if !ok {
		return FlowOutcome{Kind: FlowInvalidToken}, nil
	}
	out := FlowOutcome{Kind: FlowOK, PrincipalID: id}
	err = f.locker.WithPrincipalLock(ctx, id, func(ctx context.Context) error {
		hash, err := f.hash(newPassword)
		if err != nil {
			return fmt.Errorf("local: hashing the password: %w", err)
		}
		if _, err := f.accounts.SetPasswordEndingSessions(ctx, PasswordEnd{ID: id, Hash: hash, ResetLink: &digest}); err != nil {
			if errors.Is(err, ErrResetLinkUnusable) {
				out = FlowOutcome{Kind: FlowInvalidToken}
				return nil
			}
			return fmt.Errorf("local: setting the password: %w", err)
		}
		return nil
	})
	if err != nil {
		return FlowOutcome{}, err
	}
	return out, nil
}

// Change sets a signed-in user's password, ends every credential and issues
// the caller a new session, all in one transaction.
func (f *Flows) Change(ctx context.Context, req ChangeRequest) (FlowOutcome, error) {
	if err := f.gate(); err != nil {
		return FlowOutcome{}, err
	}
	if req.Impersonated {
		return FlowOutcome{Kind: FlowNotAllowed}, nil
	}
	if v := passwordRefusal(req.New); v != nil {
		return FlowOutcome{Kind: FlowPolicy, Violations: v}, nil
	}
	var out FlowOutcome
	err := f.locker.WithPrincipalLock(ctx, req.PrincipalID, func(ctx context.Context) error {
		var err error
		out, err = f.changeLocked(ctx, req)
		return err
	})
	if err != nil {
		return FlowOutcome{}, err
	}
	return out, nil
}

func (f *Flows) changeLocked(ctx context.Context, req ChangeRequest) (FlowOutcome, error) {
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
	res, err := f.accounts.SetPasswordEndingSessions(ctx, PasswordEnd{ID: user.ID, Hash: hash,
		NewSession: &NewSession{OrganizationID: req.OrganizationID, AMR: []string{"pwd"}}})
	if err != nil {
		return FlowOutcome{}, fmt.Errorf("local: setting the password: %w", err)
	}
	return FlowOutcome{Kind: FlowOK, PrincipalID: user.ID, Issued: res.Issued}, nil
}
