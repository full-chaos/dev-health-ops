package local

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	lockoutKeyBytes = 32
	// bcryptMaxPasswordBytes is bcrypt 5's limit: a longer password is no
	// match (login.py), never an error.
	bcryptMaxPasswordBytes = 72
)

var keyEncoding = base64.RawURLEncoding

// Config builds a Service.
type Config struct {
	Users    Users
	Sessions Sessions
	Attempts Attempts
	// LockoutKey is a dedicated 32-byte server key, base64url without padding.
	LockoutKey contracts.SecretString
	// Compare is the password check; nil means bcrypt. Tests count its calls.
	Compare func(hash, password string) bool
}

// Service is the local login.
type Service struct {
	users    Users
	sessions Sessions
	attempts Attempts
	key      [lockoutKeyBytes]byte
	compare  func(hash, password string) bool
}

// New validates the configuration.
func New(cfg Config) (*Service, error) {
	if cfg.Users == nil || cfg.Sessions == nil || cfg.Attempts == nil {
		return nil, errors.New("local: users, sessions and attempts are required")
	}
	raw, err := keyEncoding.DecodeString(cfg.LockoutKey.Reveal())
	if err != nil || len(raw) != lockoutKeyBytes {
		return nil, fmt.Errorf("local: the lockout key must be %d bytes, base64url without padding", lockoutKeyBytes)
	}
	s := &Service{users: cfg.Users, sessions: cfg.Sessions, attempts: cfg.Attempts, compare: cfg.Compare}
	copy(s.key[:], raw)
	if s.compare == nil {
		s.compare = bcryptMatches
	}
	return s, nil
}

// Format, GoString and LogValue are value receivers: a copy of a Service
// prints and logs without its key as well.
func (Service) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte("local.Service{[redacted]}")) }

func (Service) GoString() string { return "local.Service{[redacted]}" }

func (Service) LogValue() slog.Value { return slog.StringValue("local.Service{[redacted]}") }

// LockoutIdentifier is HMAC-SHA-256(lockout key, normalized email).
func (s *Service) LockoutIdentifier(normalizedEmail string) []byte {
	mac := hmac.New(sha256.New, s.key[:])
	mac.Write([]byte(normalizedEmail))
	return mac.Sum(nil)
}

var dummyHash = sync.OnceValue(func() string {
	secret := make([]byte, 24)
	if _, err := rand.Read(secret); err != nil {
		panic("local: no randomness for the dummy hash: " + err.Error())
	}
	hash, err := passwordhash.Hash(base64.RawURLEncoding.EncodeToString(secret))
	if err != nil {
		panic("local: dummy hash: " + err.Error())
	}
	return hash
})

// DummyHash is the bcrypt hash (cost 12) compared when no real hash applies.
// Its password is random and never kept.
func DummyHash() string { return dummyHash() }

func bcryptMatches(hash, password string) bool {
	if len(password) > bcryptMaxPasswordBytes {
		// Same bcrypt work as every other path, result discarded.
		_ = bcrypt.CompareHashAndPassword([]byte(hash), []byte(password[:bcryptMaxPasswordBytes]))
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// Login decides one credential check. Every refusal makes the same calls in
// the same order: ByEmail, Check, one password compare (against the dummy
// hash when no real hash applies), RecordFailure. Which refusal it was shows
// only in the Outcome.
func (s *Service) Login(ctx context.Context, c Credentials) (Outcome, error) {
	email, err := identity.NormalizeEmail(c.Email)
	if err != nil {
		return Outcome{}, fmt.Errorf("local: %w", err)
	}
	id := s.LockoutIdentifier(email)
	user, err := s.users.ByEmail(ctx, email)
	if err != nil {
		return Outcome{}, fmt.Errorf("local: loading the user: %w", err)
	}
	locked, remaining, err := s.attempts.Check(ctx, id)
	if err != nil {
		return Outcome{}, fmt.Errorf("local: checking the lockout: %w", err)
	}

	hash, usable := DummyHash(), false
	if !locked && user != nil && user.IsActive && user.PasswordHash != nil {
		hash, usable = *user.PasswordHash, true
	}
	matches := s.compare(hash, c.Password) && usable

	var refused Outcome
	switch {
	case locked:
		if remaining <= 0 {
			remaining = time.Second
		}
		refused = Outcome{Kind: KindLocked, Reason: ReasonAccountLocked, RetryAfter: remaining}
	case user == nil:
		refused = Outcome{Kind: KindInvalidCredentials, Reason: ReasonInvalidCredentials}
	case !user.IsActive:
		refused = Outcome{Kind: KindDisabled, Reason: ReasonPrincipalDisabled}
	case user.PasswordHash == nil:
		refused = Outcome{Kind: KindPasswordUnavailable, Reason: ReasonInvalidCredentials}
	case !matches:
		refused = Outcome{Kind: KindInvalidCredentials, Reason: ReasonInvalidCredentials}
	}
	if refused.Kind != "" {
		if user != nil {
			refused.PrincipalID = user.ID
		}
		if err := s.attempts.RecordFailure(ctx, id); err != nil {
			return Outcome{}, fmt.Errorf("local: recording the failed attempt: %w", err)
		}
		return refused, nil
	}

	if err := s.attempts.Clear(ctx, id); err != nil {
		return Outcome{}, fmt.Errorf("local: clearing attempts: %w", err)
	}
	// str(getattr(user, "auth_provider", "local")).lower(): a NULL provider is
	// the text "none", which is not "local".
	provider := "none"
	if user.AuthProvider != nil {
		provider = pythonparity.Lower(*user.AuthProvider)
	}
	if provider == "local" && !user.IsVerified {
		return Outcome{Kind: KindEmailUnverified, Reason: ReasonEmailUnverified, PrincipalID: user.ID}, nil
	}
	if err := s.users.Project(ctx, user.ID); err != nil {
		return Outcome{}, fmt.Errorf("local: projecting the principal: %w", err)
	}
	issued, err := s.sessions.Create(ctx, user.ID, c.OrganizationID, []string{"pwd"})
	if err != nil {
		return Outcome{}, fmt.Errorf("local: creating the session: %w", err)
	}
	return Outcome{Kind: KindSuccess, Reason: ReasonAccepted, PrincipalID: user.ID, Issued: &issued}, nil
}
