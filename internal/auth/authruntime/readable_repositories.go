package authruntime

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisionstate"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// readableProfileRows is the endpoint-profile document the readable repositories route is looked up in; nil is the
// embedded document. Tests plant a document.
var readableProfileRows []byte

// readableRepositoriesRoutes builds go-api's read listing route over the decision state, on the session issue route's
// go-api workload path (nil mounts nothing). The state reads run with no decision cache: the route never asks one.
func readableRepositoriesRoutes(cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	workload *authapi.SessionIssueWorkload) ([]httpapi.Route, error) {
	state, err := decisionstate.New(decisionstate.Config{Pool: pool, Schema: schema, Users: pgUsers(cfg, pool, schema), RefreshTTL: sessionRefreshTTL()})
	if err != nil {
		return nil, err
	}
	return authapi.ReadableRepositoriesRoutes(authapi.ReadableRepositoriesDeps{
		State: state, Evaluator: authorization.NewEvaluator(), Workload: workload, ProfileRows: readableProfileRows, Logger: logger,
	})
}
