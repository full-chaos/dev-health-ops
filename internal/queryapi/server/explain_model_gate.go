package server

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// modelGate is the D6107 own-model / platform-model gate both explain routes run before any provider
// is resolved or built.
type modelGate interface {
	Decide(ctx context.Context, orgID string) llmorgsettings.Decision
}

// explainOrgSettings is the org settings both explain routes read: provider credentials and the saved config.
type explainOrgSettings interface {
	llmorgsettings.Resolver
	llmorgsettings.ConfigSource
}

// explainModelGate builds the gate for a route; a test swaps it to see what a route does with a refusing gate.
var explainModelGate = newModelGate

// newModelGate wires the production gate over the registry pool and the explain-purpose settings.
func newModelGate(pool *pgxpool.Pool, config llmorgsettings.ConfigSource) modelGate {
	return llmorgsettings.Gate{Source: llmorgsettings.PgSource{
		Licensing: licensing.PostgresStore{Pool: pool},
		Queryer:   pool,
		Config:    config,
	}}
}

// admitModel runs the gate. It returns the request to serve, or false after answering a refusal. A nil
// gate refuses: the platform model is never the default.
func admitModel(gate modelGate, w http.ResponseWriter, r *http.Request, operation, orgID string) (*http.Request, bool) {
	decision := llmorgsettings.Decision{Kind: llmorgsettings.Refuse, Code: llmorgsettings.CodeUnavailable, Status: http.StatusServiceUnavailable}
	if gate != nil {
		decision = gate.Decide(r.Context(), orgID)
	}
	switch decision.Kind {
	case llmorgsettings.Own:
		return r.WithContext(investmentexplain.WithOwnModelOnly(r.Context())), true
	case llmorgsettings.Platform:
		return r, true
	}
	writeRESTError(w, r, operation, orgID, decision.Status, decision.Code)
	return r, false
}
