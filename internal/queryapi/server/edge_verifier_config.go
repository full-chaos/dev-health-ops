package server

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// buildEdgeVerifierFromEnv builds the access-token verifier every REST route
// in this binary shares, or (nil, nil) when AUTH_ACCESS_TOKEN_ISSUER is unset:
// the route then accepts no bearer (the envelope is retired). With the issuer set it routes
// every token by the request's endpoint-profile row (routeEdgeVerifier); the
// validator verifies EdDSA only, so an HS256 token is refused alg_not_allowed.
func buildEdgeVerifierFromEnv(getenv getenvFunc, users edgeUserStore) (*principal.EdgeVerifier, error) {
	access, routed, err := accessFromEnv(getenv)
	if err != nil {
		return nil, fmt.Errorf("build edge access-token verifier: %w", err)
	}
	if !routed {
		return nil, nil
	}
	// CHAOS-6290: an edge verifier that cannot read the live users row would
	// accept the token of a deactivated user, so a verifier without a store is
	// a build error (newEdgeUserStore refuses first when there is no registry pool).
	edgeVerifier, err := principal.NewEdgeVerifier(users)
	if err != nil {
		return nil, fmt.Errorf("build edge access-token verifier: %w", err)
	}
	if err := routeEdgeVerifier(edgeVerifier, access); err != nil {
		return nil, fmt.Errorf("build edge access-token verifier: %w", err)
	}
	return edgeVerifier, nil
}

// edgeUserStore is the live users-row source every edge-verified REST route
// shares: policy.Store, the same interface (and PGStore implementation) the Go
// api authenticates with.
type edgeUserStore = policy.Store

// newEdgeUserStore is the live users-row source behind every edge-verified
// REST route's check (CHAOS-6290): policy.PGStore, the Go api's own reader, over
// registryPool -- the process's ONE registry Postgres pool that /query already
// opens (queryRouteHandlers.RegistryPool). It opens no pool of its own. It
// returns (nil, nil) when AUTH_ACCESS_TOKEN_ISSUER is unset (no edge verifier is
// built, so nothing reads users). With the issuer set and no registryPool -- the
// /query route is not configured, so there is no pool to read users through --
// it returns an error: fail closed, never a verifier that skips the check.
// PGStore.UserState selects is_active, is_superuser and token_version only,
// never a credential column.
func newEdgeUserStore(getenv getenvFunc, registryPool *pgxpool.Pool) (edgeUserStore, error) {
	if accessTokenIssuer(getenv) == "" {
		return nil, nil
	}
	if registryPool == nil {
		return nil, fmt.Errorf("build edge users store: %s is set but the /query route (which owns the registry Postgres pool, GO_API_REGISTRY_POSTGRES_URI) is not configured; an access token cannot be accepted without a live users check", accessTokenIssuerEnvVar)
	}
	authSchema, err := policy.AuthSchemaFromEnv(getenv)
	if err != nil {
		return nil, fmt.Errorf("build edge users store: %w", err)
	}
	platformRolesOwned, err := queryAPIPlatformRolesOwned(getenv)
	if err != nil {
		return nil, fmt.Errorf("build edge users store: %w", err)
	}
	return newQueryAPIUserStore(policy.PGStore{Pool: registryPool, AuthSchema: authSchema, PlatformRolesOwned: platformRolesOwned}), nil
}

// buildQueryEdgeAuthenticatorFromEnv builds /query's edge-carrier
// authenticator (CHAOS-6263 PR (a)): internal/api/policy.Authenticator, go-api's
// OWN component reused by import, that resolves is_active/is_superuser/
// token_version LIVE from Postgres per request rather than trusting the token's
// own claims for them (see query_api_authorization.go's queryAPIPosture doc
// comment for why this needs new grants). pool is query-api's existing registry
// Postgres pool (RegistryPostgresURI) -- the same connection every other /query
// Postgres read already uses, not a new dependency.
//
// It routes every token by the request's endpoint-profile row
// (routeAuthenticator). It returns (nil, nil, nil) when AUTH_ACCESS_TOKEN_ISSUER
// is unset and no pool is configured, and an error when the pool is configured
// without the issuer: the issuer is the only user access-token verifier.
//
// Returns the store alongside the Authenticator (not just the
// Authenticator) because policy.Authenticator.Authenticate does not itself
// check membership or impersonation -- go-api's own Scope.Impersonation/
// mayUseOrg call those on the store directly (scope.go), so /query's
// caller needs the same store value to do the same two checks after
// Authenticate succeeds, without a second Postgres pool or a new exported
// accessor on Authenticator.
func buildQueryEdgeAuthenticatorFromEnv(getenv getenvFunc, pool *pgxpool.Pool) (*policy.Authenticator, policy.Store, error) {
	access, routed, err := accessFromEnv(getenv)
	if err != nil {
		return nil, nil, fmt.Errorf("build query edge authenticator: %w", err)
	}
	if !routed {
		if pool != nil {
			return nil, nil, fmt.Errorf("build query edge authenticator: %s is required when the /query route is configured: it is the only user access-token verifier", accessTokenIssuerEnvVar)
		}
		return nil, nil, nil
	}
	authSchema, err := policy.AuthSchemaFromEnv(getenv)
	if err != nil {
		return nil, nil, fmt.Errorf("build query edge authenticator: %w", err)
	}
	platformRolesOwned, err := queryAPIPlatformRolesOwned(getenv)
	if err != nil {
		return nil, nil, fmt.Errorf("build query edge authenticator: %w", err)
	}
	store := newQueryAPIUserStore(policy.PGStore{Pool: pool, AuthSchema: authSchema, PlatformRolesOwned: platformRolesOwned})
	auth, err := policy.NewAuthenticator(store, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build query edge authenticator: %w", err)
	}
	if err := routeAuthenticator(auth, access); err != nil {
		return nil, nil, fmt.Errorf("build query edge authenticator: %w", err)
	}
	return auth, store, nil
}
