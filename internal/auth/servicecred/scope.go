package servicecred

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// ScopeMembershipsWrite is the scope of the internal membership, invite and
// organization deprovision routes. Only the session issuer credential may hold
// it (go-api's serviceScopes).
const ScopeMembershipsWrite = "memberships:write"

// CheckScope is Check for another scope of the session issuer credential: the
// same row rules (session issuer service, unrevoked, unexpired), holding
// scope. A refusal is a Refusal with a nil error; err is set only when the
// lookup failed.
func (p PG) CheckScope(ctx context.Context, token contracts.SecretString, scope string) (Credential, Refusal, error) {
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
	}
	return decide(id, service, scopes, revoked, expired, scope)
}

// decide is the row rule of CheckScope.
func decide(id, service, scopes string, revoked, expired bool, scope string) (Credential, Refusal, error) {
	switch {
	case service != ServiceSessionIssuer:
		return Credential{}, RefusedService, nil
	case revoked:
		return Credential{}, RefusedRevoked, nil
	case expired:
		return Credential{}, RefusedExpired, nil
	}
	var held []string
	if err := json.Unmarshal([]byte(scopes), &held); err != nil || !slices.Contains(held, scope) {
		return Credential{}, RefusedScope, nil
	}
	return Credential{ID: id}, "", nil
}
