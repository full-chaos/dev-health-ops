package server

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

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
	raw := strings.TrimSpace(getenv(impersonationProxyEnv))
	if raw == "" {
		return false, nil
	}
	on, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", impersonationProxyEnv)
	}
	return on, nil
}

// newGraphQLEdgeDeps is /graphql's middleware dependencies from the settings.
func newGraphQLEdgeDeps(getenv getenvFunc, lookup func(string) (string, bool), auth *policy.Authenticator, logger *slog.Logger) (graphQLEdgeDeps, error) {
	refuse, err := refusesLegacyImpersonation(getenv)
	if err != nil {
		return graphQLEdgeDeps{}, err
	}
	return graphQLEdgeDeps{
		auth:                      auth,
		corsOrigins:               corsAllowedOrigins(lookup),
		maxBytes:                  graphQLMaxQueryBytes(getenv),
		logger:                    logger,
		refuseLegacyImpersonation: refuse,
	}, nil
}
