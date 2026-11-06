package server

import (
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// edgeJWTSecretEnvVar named the shared secret of the retired HS256 edge access
// token. Nothing verifies with it now; a pod that still sets it gets one WARN
// (warnUnusedEdgeSecret).
const edgeJWTSecretEnvVar = "GO_API_EDGE_JWT_SECRET"

// defaultEdgeJWTIssuer and defaultEdgeJWTAudience key the HS256 half of an
// edge verifier, which no token reaches: every row refuses HS256 first.
const (
	defaultEdgeJWTIssuer   = "dev-health-ops"
	defaultEdgeJWTAudience = "dev-health-api"
)

// buildEdgeVerifierFromEnv builds the access-token verifier every REST route
// in this binary shares, or (nil, nil) when AUTH_ACCESS_TOKEN_ISSUER is unset:
// the route then accepts the envelope alone. With the issuer set it routes
// every token by the request's endpoint-profile row (routeEdgeVerifier); its
// validator has no HS256 verifier, so an HS256 token is refused
// alg_not_allowed.
func buildEdgeVerifierFromEnv(getenv getenvFunc, users edgeUserStore) (*principal.EdgeVerifier, error) {
	access, routed, err := accessFromEnv(getenv)
	if err != nil {
		return nil, fmt.Errorf("build edge access-token verifier: %w", err)
	}
	if !routed {
		return nil, nil
	}
	secret, issuer, audience := unusedEdgeSecret(), defaultEdgeJWTIssuer, defaultEdgeJWTAudience
	// CHAOS-6290: an edge verifier that cannot read the live users row would
	// accept the token of a deactivated user, so the secret without a store is
	// a build error (newEdgeUserStore refuses first when there is no registry pool), never a silent JWT-only verifier.
	edgeVerifier, err := principal.NewEdgeVerifier(secret, issuer, audience, users)
	if err != nil {
		return nil, fmt.Errorf("build edge access-token verifier: %w", err)
	}
	if routed {
		if err := routeEdgeVerifier(edgeVerifier, access); err != nil {
			return nil, fmt.Errorf("build edge access-token verifier: %w", err)
		}
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
// returns (nil, nil) when GO_API_EDGE_JWT_SECRET is absent (no edge verifier is
// built, so nothing reads users). With the secret set and no registryPool -- the
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

// warnUnusedEdgeSecret logs one WARN when GO_API_EDGE_JWT_SECRET is still set:
// it verified the retired HS256 edge token and nothing reads it now. A
// leftover value is config hygiene, never a refused start (D6465).
func warnUnusedEdgeSecret(getenv getenvFunc, logger *slog.Logger) {
	if getenv(edgeJWTSecretEnvVar) != "" {
		logger.Warn(edgeJWTSecretEnvVar + " is set but unused (HS256 retired)")
	}
}

// buildQueryEdgeAuthenticatorFromEnv builds /query's edge-carrier
// authenticator (CHAOS-6263 PR (a)) -- the SAME GO_API_EDGE_JWT_* trio
// buildEdgeVerifierFromEnv reads (so a pod that already accepts the edge
// token on its REST routes needs no new config to also accept it on
// /query), but wired to internal/api/policy.Authenticator instead of
// principal.EdgeVerifier: go-api's OWN component, reused by import, that
// resolves is_active/is_superuser/token_version LIVE from Postgres per
// request rather than trusting the token's own claims for them (see
// query_api_authorization.go's queryAPIPosture doc comment for why this
// needs new grants). pool is query-api's existing registry Postgres pool
// (RegistryPostgresURI) -- the same connection every other /query Postgres
// read already uses, not a new dependency.
//
// Returns (nil, nil, nil), the same "not configured" contract as
// buildEdgeVerifierFromEnv, when GO_API_EDGE_JWT_SECRET is absent.
//
// Returns the store alongside the Authenticator (not just the
// Authenticator) because policy.Authenticator.Authenticate does not itself
// check membership or impersonation -- go-api's own Scope.Impersonation/
// mayUseOrg call those on the store directly (scope.go), so /query's
// caller needs the same store value to do the same two checks after
// Authenticate succeeds, without a second Postgres pool or a new exported
// accessor on Authenticator.
//
// With AUTH_ACCESS_TOKEN_ISSUER set, the Authenticator routes every token by
// the request's endpoint-profile row (routeAuthenticator), and the issuer
// alone, with no edge secret, builds it.
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
	if routed {
		if err := routeAuthenticator(auth, access); err != nil {
			return nil, nil, fmt.Errorf("build query edge authenticator: %w", err)
		}
	}
	return auth, store, nil
}
