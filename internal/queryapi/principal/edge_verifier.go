package principal

import (
	"context"
	"fmt"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// EdgeAlgorithm is the one signing method the edge access token verifier
// accepts (edgetoken.Algorithm).
const EdgeAlgorithm = edgetoken.Algorithm

// EdgeClaims is the narrow set of claims this verifier's caller needs --
// today, only the org id an authenticated REST request is scoped to,
// mirroring AuthenticatedUser.org_id (services/auth.py): payload.get(
// "org_id", ""), never required, defaulting to empty exactly as the
// Python dataclass does.
type EdgeClaims struct {
	OrgID string
}

// EdgeVerifier verifies the edge access token -- the HS256-signed JWT
// AuthService.create_access_token mints and validate_token verifies
// (services/auth.py) -- the credential a real user's browser actually
// carries. query-api never minted this token and never will: the secret
// is supplied to the pod as key material (see this repo's edge verifier
// env-var contract), read once at process start, never logged and never
// read from any file this binary opens itself.
//
// This verifier reproduces validate_token's JWT-level decision ONLY: same
// algorithm, same conditional issuer/audience checks, the same required
// exp/sub/type claims, and the same `type == "access"` gate. It does NOT
// reproduce authenticate_access_token's Postgres-backed is_active/
// token_version check -- that is a database round trip query-api's REST
// routes have never made for the envelope credential either, and adding
// one is out of this verifier's own scope (see this repo's PR history
// for the reasoning).
type EdgeVerifier struct {
	verifier *edgetoken.Verifier
}

// NewEdgeVerifier builds an EdgeVerifier bound to secret (the pod's own
// copy of Python's JWT_SECRET_KEY), issuer and audience (Python's
// JWT_ISSUER/JWT_AUDIENCE, "dev-health-ops"/"dev-health-api" by default).
// The checks on them are edgetoken.New's: a secret under 32 characters and
// an empty issuer or audience are refused at construction, not on the
// first live 401 an operator cannot explain.
func NewEdgeVerifier(secret, issuer, audience string) (*EdgeVerifier, error) {
	verifier, err := edgetoken.New(secret, issuer, audience)
	if err != nil {
		return nil, fmt.Errorf("principal: edge access-token verifier: %w", err)
	}
	return &EdgeVerifier{verifier: verifier}, nil
}

// Verify verifies tokenString with edgetoken.Verifier.Verify (the one Go
// copy of validate_token's decision) and adds this binary's telemetry: one
// log line and one counted reason per rejection, one outcome count per
// call.
//
// On success, returns the validated claims' org_id (empty string when
// the claim is absent, matching AuthenticatedUser.org_id's own default).
// Never reproduces authenticate_access_token's Postgres lookup -- see
// EdgeVerifier's own doc comment.
func (v *EdgeVerifier) Verify(ctx context.Context, tokenString string) (*EdgeClaims, error) {
	meta := requestMetaFrom(ctx)
	claims, err := v.verifier.Verify(tokenString)
	if err != nil {
		v.reject(ctx, edgetoken.ReasonOf(err), claims, meta)
		return nil, fmt.Errorf("principal: edge: %w", err)
	}
	recordEdgeVerifyOutcome("verified")
	orgID, _ := claims["org_id"].(string)
	return &EdgeClaims{OrgID: orgID}, nil
}

// reject is Verify's one call site for every rejection branch: it logs
// and counts the SAME reason exactly once, at the point the decision was
// made, so no branch above can log without counting (or the reverse) by
// a future editing mistake -- the same single-site discipline
// principal.Verify's own rejection handling follows.
func (v *EdgeVerifier) reject(ctx context.Context, reason string, claims jwt.MapClaims, meta requestMeta) {
	logEdgeRejection(ctx, reason, claims, meta)
	recordEdgeRejected(reason)
	recordEdgeVerifyOutcome("rejected")
}
