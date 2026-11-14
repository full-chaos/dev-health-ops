package authruntime

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// AuthorizeRoutesWithoutFeed is authorizeRoutes with the revision feed off (no decision cache): the production builder
// of the decision point routes, for a test decision point over a test database. auth-service never calls it.
func AuthorizeRoutesWithoutFeed(cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	signer authapi.AccessSigner) ([]httpapi.Route, error) {
	return authorizeRoutes(cfg, logger, pool, schema, signer, nil)
}
