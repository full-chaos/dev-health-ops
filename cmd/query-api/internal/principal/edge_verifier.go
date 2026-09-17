package principal

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// EdgeAlgorithm is the one signing method the edge access token verifier
// accepts -- HS256, the algorithm services/auth.py's JWT_ALGORITHM
// constant fixes for every token AuthService.create_access_token mints.
// Passed to jwt.WithValidMethods so an alg-confusion attempt (e.g. "none"
// or an RS256 token whose payload happens to carry the right claims) is
// rejected before any key material is even considered, the same guard
// principal.Verify applies for the envelope's own EdDSA-only contract.
const EdgeAlgorithm = "HS256"

// edgeAccessTokenType is the one `type` claim value this verifier
// accepts -- AuthService.validate_token's own `payload.get("type") !=
// token_type` check, called with token_type="access" by
// get_authenticated_user (services/auth.py), the only path REST auth
// (auth/routers/dependencies.py's get_current_user) ever calls. A
// refresh token -- type "refresh" -- is a different credential meant for
// POST /auth/refresh, never a REST route's own Authorization header, and
// is rejected here exactly as validate_token rejects it.
const edgeAccessTokenType = "access"

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
	secret   []byte
	issuer   string
	audience string
}

// NewEdgeVerifier builds an EdgeVerifier bound to secret (the pod's own
// copy of Python's JWT_SECRET_KEY), issuer and audience (Python's
// JWT_ISSUER/JWT_AUDIENCE, "dev-health-ops"/"dev-health-api" by default --
// see this repo's edge verifier env-var contract for how a caller
// resolves these before calling here).
//
// secret must be at least 32 characters -- the same floor
// _get_jwt_secret (services/auth.py) enforces on the Python side before
// AuthService will even sign a token with it. A shorter value is refused
// here for the same reason Python refuses it: fail loudly at
// construction, not on the first live 401 an operator cannot explain.
// issuer and audience must both be non-empty for the same reason
// principal.NewVerifier's own doc comment gives for the envelope
// verifier: jwt.WithIssuer("")/WithAudience("") do not fail closed, they
// disable the check entirely.
func NewEdgeVerifier(secret, issuer, audience string) (*EdgeVerifier, error) {
	if len(secret) < 32 {
		return nil, errors.New("principal: edge access-token secret must be at least 32 characters")
	}
	if issuer == "" {
		return nil, errors.New("principal: edge access-token issuer must not be empty")
	}
	if audience == "" {
		return nil, errors.New("principal: edge access-token audience must not be empty")
	}
	return &EdgeVerifier{secret: []byte(secret), issuer: issuer, audience: audience}, nil
}

// Verify parses and verifies tokenString as an edge access token,
// reproducing validate_token's (services/auth.py) exact decisions:
//
//   - signature: HMAC-SHA256 against v.secret, the same algorithm
//     jwt.encode(payload, self.secret_key, algorithm=JWT_ALGORITHM) signs
//     with -- WithValidMethods closes the alg-confusion path the same way
//     principal.Verify's own EdDSA-only contract does.
//   - audience/issuer: verified ONLY when the token's own claims carry an
//     "aud"/"iss" key at all (has_audience/has_issuer in validate_token,
//     from an UNVERIFIED peek at the payload) -- present-but-wrong is
//     always a rejection; ABSENT is not checked at all, matching
//     validate_token's decode_kwargs assembly exactly.
//   - exp/sub/type: all three are REQUIRED claims (jwt.decode's
//     options={"require": ["exp", "sub", "type"]}) -- a token missing any
//     one is rejected before signature-adjacent claim checks would even
//     matter.
//   - type: must equal "access" (validate_token's own post-decode
//     `payload.get("type") != token_type` check, called with
//     token_type="access" by every REST auth path) -- a valid, unexpired
//     REFRESH token is rejected here exactly as it is on the Python side.
//
// On success, returns the validated claims' org_id (empty string when
// the claim is absent, matching AuthenticatedUser.org_id's own default).
// Never reproduces authenticate_access_token's Postgres lookup -- see
// EdgeVerifier's own doc comment.
func (v *EdgeVerifier) Verify(ctx context.Context, tokenString string) (*EdgeClaims, error) {
	meta := requestMetaFrom(ctx)

	var peeked jwt.MapClaims
	if _, _, err := jwt.NewParser().ParseUnverified(tokenString, &peeked); err != nil {
		v.reject(ctx, "malformed", nil, meta)
		return nil, fmt.Errorf("principal: edge: unverified peek: %w", err)
	}
	_, hasAudience := peeked["aud"]
	_, hasIssuer := peeked["iss"]

	opts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{EdgeAlgorithm}),
		jwt.WithExpirationRequired(),
	}
	if hasAudience {
		opts = append(opts, jwt.WithAudience(v.audience))
	}
	if hasIssuer {
		opts = append(opts, jwt.WithIssuer(v.issuer))
	}

	var claims jwt.MapClaims
	token, err := jwt.ParseWithClaims(tokenString, &claims, func(*jwt.Token) (interface{}, error) {
		return v.secret, nil
	}, opts...)
	if err != nil {
		v.reject(ctx, classifyEdgeRejectionReason(err), claims, meta)
		return nil, fmt.Errorf("principal: edge: verify: %w", err)
	}
	if !token.Valid {
		v.reject(ctx, "malformed", claims, meta)
		return nil, errors.New("principal: edge: token invalid")
	}

	// require=["exp", "sub", "type"]: exp is already enforced by
	// WithExpirationRequired above; sub and type have no equivalent
	// ParserOption (golang-jwt has no generic "require this claim key
	// exists" hook), so presence is checked directly against the decoded
	// map, matching pyjwt's own key-existence semantics -- a claim
	// present with a zero value (e.g. sub: "") still counts as present.
	if _, ok := claims["sub"]; !ok {
		v.reject(ctx, "missing_sub", claims, meta)
		return nil, errors.New("principal: edge: missing sub claim")
	}
	typeClaim, ok := claims["type"]
	if !ok {
		v.reject(ctx, "missing_type", claims, meta)
		return nil, errors.New("principal: edge: missing type claim")
	}
	typeValue, _ := typeClaim.(string)
	if typeValue != edgeAccessTokenType {
		v.reject(ctx, "type_mismatch", claims, meta)
		return nil, fmt.Errorf("principal: edge: type claim is %q, want %q", typeValue, edgeAccessTokenType)
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

// classifyEdgeRejectionReason maps a jwt.ParseWithClaims error to a
// caller-observable rejection reason, the same errors.Is-over-the-full-
// chain approach classifyRejectionReason (verifier.go) uses for the
// envelope's own five claim-level sentinels -- golang-jwt/jwt/v5 wraps
// each of these identically regardless of which SigningMethod or
// Validator produced them, so the same switch applies unchanged here.
// Anything this switch does not name (an unsupported/missing alg, a
// malformed token body, a required claim missing) collapses to
// "malformed" for the same reason classifyRejectionReason's own doc
// comment gives: the caller-observable fact is identical in every one of
// those cases -- the presented token could not be processed as valid at
// all, as opposed to being processed and found to disagree on one
// specific claim.
func classifyEdgeRejectionReason(err error) string {
	switch {
	case errors.Is(err, jwt.ErrTokenSignatureInvalid):
		return "bad_signature"
	case errors.Is(err, jwt.ErrTokenInvalidAudience):
		return "audience"
	case errors.Is(err, jwt.ErrTokenInvalidIssuer):
		return "issuer"
	case errors.Is(err, jwt.ErrTokenExpired):
		return "expired"
	case errors.Is(err, jwt.ErrTokenNotValidYet):
		return "not_yet_valid"
	default:
		return "malformed"
	}
}
