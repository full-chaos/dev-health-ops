package server

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// The pod env contract for the edge access token credential -- the
// variable trio every REST route reads so it can ALSO accept the
// credential a real user's browser actually carries, not only the
// effective-principal envelope. Named GO_API_EDGE_JWT_* to sit beside the
// existing GO_API_ENVELOPE_JWKS_PATH/ISSUER/AUDIENCE trio each route
// already reads.
//
//   - GO_API_EDGE_JWT_SECRET (REQUIRED to enable edge-token acceptance;
//     absent means every REST route keeps accepting the envelope alone --
//     see buildEdgeVerifierFromEnv's own doc comment): the SAME shared
//     secret Python's JWT_SECRET_KEY names (services/auth.py's
//     _get_jwt_secret) -- by secretKeyRef, never inline, at the SAME
//     Secret key ("JWT_SECRET_KEY") the chart already defines for
//     web-deployment.yaml. Wiring this into query-api-deployment.yaml is
//     a deploy-side change, not a Go source change.
//   - GO_API_EDGE_JWT_ISSUER (optional, defaults to "dev-health-ops"):
//     the same string as Python's JWT_ISSUER env var default
//     (services/auth.py). No chart in this repo sets JWT_ISSUER today, so
//     Python always signs with its own default -- this default keeps the
//     two in sync without requiring the chart to also set this variable.
//   - GO_API_EDGE_JWT_AUDIENCE (optional, defaults to "dev-health-api"):
//     same reasoning, matching Python's JWT_AUDIENCE default.
const (
	edgeJWTSecretEnvVar   = "GO_API_EDGE_JWT_SECRET"
	edgeJWTIssuerEnvVar   = "GO_API_EDGE_JWT_ISSUER"
	edgeJWTAudienceEnvVar = "GO_API_EDGE_JWT_AUDIENCE"

	// defaultEdgeJWTIssuer and defaultEdgeJWTAudience mirror
	// services/auth.py's own JWT_ISSUER/JWT_AUDIENCE literal defaults
	// exactly (`os.getenv("JWT_ISSUER", "dev-health-ops")` /
	// `os.getenv("JWT_AUDIENCE", "dev-health-api")`) -- not a
	// Go-specific choice, a copy of the Python constant.
	defaultEdgeJWTIssuer   = "dev-health-ops"
	defaultEdgeJWTAudience = "dev-health-api"
)

// buildEdgeVerifierFromEnv builds the edge-access-token verifier every
// REST route in this binary shares, or (nil, nil) when the pod has not
// been given GO_API_EDGE_JWT_SECRET yet.
//
// Unlike the envelope verifier's jwksPath/issuer/audience trio (already
// required for a route to mount at all, per each route's own
// loadXRouteConfig), the edge trio is OPTIONAL: a route whose pod lacks
// GO_API_EDGE_JWT_SECRET keeps mounting and keeps accepting the envelope
// alone, rather than refusing to start. This is deliberate -- an existing
// deployment predates the chart change that will set this variable (a
// deploy-side follow-up), and these routes must not regress THAT
// deployment's envelope-only traffic while the chart change is still
// pending.
//
// A non-nil, misconfigured secret (present but under 32 characters) IS a
// hard error: unlike "not configured at all", that is an operator typo
// this binary can catch at start, the same fail-fast NewVerifier already
// applies to the envelope's own issuer/audience.
func buildEdgeVerifierFromEnv(getenv getenvFunc, users edgeUserStore) (*principal.EdgeVerifier, error) {
	secret, issuer, audience := edgeJWTConfigFromEnv(getenv)
	if secret == "" {
		return nil, nil
	}
	// CHAOS-6290: an edge verifier that cannot read the live users row would
	// accept the token of a deactivated user, so the secret without a store is
	// a build error (newEdgeUserStore builds the store, and refuses first when
	// GO_API_REGISTRY_POSTGRES_URI is absent), never a silent JWT-only verifier.
	edgeVerifier, err := principal.NewEdgeVerifier(secret, issuer, audience, users)
	if err != nil {
		return nil, fmt.Errorf("build edge access-token verifier: %w", err)
	}
	return edgeVerifier, nil
}

// edgeUserStore is the live users-row source every edge-verified REST route
// shares: policy.Store, the same interface (and PGStore implementation) the Go
// api authenticates with.
type edgeUserStore = policy.Store

// newEdgeUserStore builds the ONE Postgres pool behind every edge-verified
// REST route's live users-row check (CHAOS-6290), once per Build, and hands the
// resulting store to each route builder. It returns (nil, no-op, nil) when
// GO_API_EDGE_JWT_SECRET is absent (no edge verifier is built, so nothing reads
// users). With the secret set and GO_API_REGISTRY_POSTGRES_URI empty it returns
// an error: fail closed, never a verifier that skips the check. The pool is
// lazy (pgxpool.New does not dial) and reads through policy.PGStore, whose
// UserState selects is_active, is_superuser and token_version only -- never a
// credential column.
func newEdgeUserStore(getenv getenvFunc) (edgeUserStore, func(), error) {
	noop := func() {}
	if secret, _, _ := edgeJWTConfigFromEnv(getenv); secret == "" {
		return nil, noop, nil
	}
	uri := getenv("GO_API_REGISTRY_POSTGRES_URI")
	if uri == "" {
		return nil, noop, fmt.Errorf("build edge users store: %s is set but GO_API_REGISTRY_POSTGRES_URI is empty; the edge token cannot be accepted without a live users check", edgeJWTSecretEnvVar)
	}
	pool, err := pgxpool.New(context.Background(), uri)
	if err != nil {
		return nil, noop, fmt.Errorf("build edge users store: %w", err)
	}
	return policy.PGStore{Pool: pool}, pool.Close, nil
}

// edgeJWTConfigFromEnv resolves the shared GO_API_EDGE_JWT_* trio, applying
// the same issuer/audience defaults every caller needs. secret == "" means
// "not configured" -- callers use that to opt out exactly as
// buildEdgeVerifierFromEnv always has.
func edgeJWTConfigFromEnv(getenv getenvFunc) (secret, issuer, audience string) {
	secret = getenv(edgeJWTSecretEnvVar)
	if secret == "" {
		return "", "", ""
	}
	issuer = getenv(edgeJWTIssuerEnvVar)
	if issuer == "" {
		issuer = defaultEdgeJWTIssuer
	}
	audience = getenv(edgeJWTAudienceEnvVar)
	if audience == "" {
		audience = defaultEdgeJWTAudience
	}
	return secret, issuer, audience
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
func buildQueryEdgeAuthenticatorFromEnv(getenv getenvFunc, pool *pgxpool.Pool) (*policy.Authenticator, policy.Store, error) {
	secret, issuer, audience := edgeJWTConfigFromEnv(getenv)
	if secret == "" {
		return nil, nil, nil
	}
	verifier, err := edgetoken.New(secret, issuer, audience)
	if err != nil {
		return nil, nil, fmt.Errorf("build query edge-access-token verifier: %w", err)
	}
	store := policy.PGStore{Pool: pool}
	auth, err := policy.NewAuthenticator(verifier, store, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build query edge authenticator: %w", err)
	}
	return auth, store, nil
}
