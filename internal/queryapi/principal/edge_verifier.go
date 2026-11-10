package principal

import (
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// EdgeAlgorithm is the alg of the retired HS256 edge access token. AccessClassOf
// reads it from the header only, so such a token reaches the routed validator,
// which refuses it alg_not_allowed.
const EdgeAlgorithm = "HS256"

// OrgRequiredError is VerifyRouted's refusal of a valid token that names no
// organization when the live users row is not a superuser's.
type OrgRequiredError struct {
	// Caller and Delegated are the refused token's shadow caller kind and delegation state, for the PDP shadow only.
	Caller    string
	Delegated bool
}

func (*OrgRequiredError) Error() string { return "principal: edge: organization context required" }

// EdgeClaims is the narrow set of claims this verifier's caller needs --
// today, only the org id an authenticated REST request is scoped to,
// mirroring AuthenticatedUser.org_id (services/auth.py): payload.get(
// "org_id", ""), never required, defaulting to empty exactly as the
// Python dataclass does.
type EdgeClaims struct {
	OrgID string
	// Caller is the shadow's closed caller kind and Delegated is true under a delegated session; VerifyRouted sets
	// both for the PDP shadow, which only observes them.
	Caller    string
	Delegated bool
}

// EdgeVerifier authenticates the bearer of query-api's REST routes against the
// request's endpoint-profile row (RouteAccess, then VerifyRouted) and the live
// users row, through policy.Authenticator: the same implementation the Go api
// authenticates with. The lookup reads no credential column (policy.PGStore
// selects is_active, is_superuser and token_version only).
type EdgeVerifier struct {
	auth *policy.Authenticator
}

// NewEdgeVerifier builds an EdgeVerifier over users, the live users-row source.
// users is required: a verifier without one would accept the token of a
// deactivated user (CHAOS-6290), so nil is refused here, not tolerated. It
// accepts no token until RouteAccess.
func NewEdgeVerifier(users policy.Store) (*EdgeVerifier, error) {
	if users == nil {
		return nil, errors.New("principal: edge access-token verifier needs a users store")
	}
	auth, err := policy.NewAuthenticator(users, edgeLogger())
	if err != nil {
		return nil, fmt.Errorf("principal: edge access-token verifier: %w", err)
	}
	return &EdgeVerifier{auth: auth}, nil
}
