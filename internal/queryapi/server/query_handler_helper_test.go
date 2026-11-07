package server

import (
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/featureflags"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// newQueryHandler is newQueryHandlerWithEdge over the edge carrier getenv configures, as buildQueryRoute builds it.
func newQueryHandler(chClient featureflags.QueryClient, pgPool *pgxpool.Pool, verifier *principal.Verifier, schemaDigest string, getenv getenvFunc, classSwitches ...routeswitch.Switch) (http.HandlerFunc, http.HandlerFunc, http.HandlerFunc, http.HandlerFunc, error) {
	edgeAuth, edgeStore, err := buildQueryEdgeAuthenticatorFromEnv(getenv, pgPool)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("query-api: edge authenticator: %w", err)
	}
	return newQueryHandlerWithEdge(chClient, pgPool, verifier, schemaDigest, getenv, edgeAuth, edgeStore, classSwitches...)
}
