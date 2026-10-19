package local

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

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
type Service struct{}

// New validates the configuration.
func New(Config) (*Service, error) { return nil, errNotImplemented }

// Login decides one credential check.
func (*Service) Login(context.Context, Credentials) (Outcome, error) {
	return Outcome{}, errNotImplemented
}

// DummyHash is the bcrypt hash compared when no real hash applies.
func DummyHash() string { return "" }

// LockoutIdentifier is HMAC-SHA-256(lockout key, normalized email).
func (*Service) LockoutIdentifier(string) []byte { return nil }

func bcryptMatches(hash, password string) bool { return false }
