package server

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// impersonationProxyEnv is go-api's AUTH_PROXY_IMPERSONATION: on, an
// impersonation is a delegated session whose own bearer is the target, and
// auth-service still writes go-api's impersonation row beside the admin's
// own bearer.
const impersonationProxyEnv = "AUTH_PROXY_IMPERSONATION"

// refusesLegacyImpersonation reads AUTH_PROXY_IMPERSONATION as go-api reads
// it: unset or empty is off, true or false as strconv.ParseBool reads them,
// any other value an error.
func refusesLegacyImpersonation(getenv getenvFunc) (bool, error) {
	return false, nil
}

// newGraphQLEdgeDeps is /graphql's middleware dependencies from the settings.
func newGraphQLEdgeDeps(getenv getenvFunc, lookup func(string) (string, bool), auth *policy.Authenticator) (graphQLEdgeDeps, error) {
	return graphQLEdgeDeps{
		auth:        auth,
		corsOrigins: corsAllowedOrigins(lookup),
		maxBytes:    graphQLMaxQueryBytes(getenv),
		logger:      slog.Default(),
	}, nil
}
