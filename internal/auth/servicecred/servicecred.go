// Package servicecred checks the go-api session issuer service credential on
// the routes that still take it: an auth-session-issuer row of go-api's
// internal_service_credentials table (minted, rotated and revoked with the
// service-credentials verbs), found by the SHA-256 of the presented token.
// The session issue routes no longer take it: they serve the go-api workload
// token only.
package servicecred

import (
	"errors"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"
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
