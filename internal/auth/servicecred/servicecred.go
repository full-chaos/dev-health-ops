// Package servicecred checks the go-api service credential the session issue
// route accepts: an auth-session-issuer row of go-api's
// internal_service_credentials table (minted, rotated and revoked with the
// service-credentials verbs), found by the SHA-256 of the presented token.
package servicecred

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// The service and scope a session issue credential holds.
const (
	ServiceSessionIssuer = "auth-session-issuer"
	ScopeSessionsIssue   = "sessions:issue"
)

// tokenPattern is the credential's shape: its service prefix and 32 random
// bytes as unpadded base64url.
var tokenPattern = regexp.MustCompile(`^svc_authsess_[A-Za-z0-9_-]{43}$`)

// Refusal names why a credential was refused. It goes to a log, never to the
// caller, and never carries the token.
type Refusal string

const (
	RefusedMalformed Refusal = "malformed"
	RefusedUnknown   Refusal = "unknown"
	RefusedRevoked   Refusal = "revoked"
	RefusedExpired   Refusal = "expired"
	RefusedService   Refusal = "wrong_service"
	RefusedScope     Refusal = "scope_missing"
)

// ErrStore wraps a failed lookup.
var ErrStore = errors.New("servicecred: credential store unavailable")

// Credential is an accepted credential: its row id.
type Credential struct{ ID string }

// PG checks credentials against public.internal_service_credentials. The
// runtime role reads id, service_name, token_hash, scopes, expires_at and
// revoked_at there (authschema.PublicReadColumns), nothing else.
type PG struct{ Pool *pgxpool.Pool }

// Check accepts token when it is the session issuer's: unrevoked, unexpired
// (a credential is still valid at its expires_at instant, as the
// service-credentials verbs read it), and holding sessions:issue. A refusal is
// a Refusal with a nil error; err is set only when the lookup failed.
func (p PG) Check(ctx context.Context, token contracts.SecretString) (Credential, Refusal, error) {
	if !tokenPattern.MatchString(token.Reveal()) {
		return Credential{}, RefusedMalformed, nil
	}
	sum := sha256.Sum256([]byte(token.Reveal()))
	var (
		id, service, scopes string
		revoked, expired    bool
	)
	err := p.Pool.QueryRow(ctx, `
		SELECT id::text, service_name, scopes::text, revoked_at IS NOT NULL,
		       expires_at IS NOT NULL AND expires_at < clock_timestamp()
		FROM public.internal_service_credentials WHERE token_hash = $1`, hex.EncodeToString(sum[:])).
		Scan(&id, &service, &scopes, &revoked, &expired)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Credential{}, RefusedUnknown, nil
	case err != nil:
		return Credential{}, "", fmt.Errorf("%w: %w", ErrStore, err)
	case service != ServiceSessionIssuer:
		return Credential{}, RefusedService, nil
	case revoked:
		return Credential{}, RefusedRevoked, nil
	case expired:
		return Credential{}, RefusedExpired, nil
	}
	var held []string
	if err := json.Unmarshal([]byte(scopes), &held); err != nil {
		return Credential{}, RefusedScope, nil
	}
	for _, scope := range held {
		if scope == ScopeSessionsIssue {
			return Credential{ID: id}, "", nil
		}
	}
	return Credential{}, RefusedScope, nil
}
