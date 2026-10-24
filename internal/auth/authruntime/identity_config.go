package authruntime

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func identityConfig(cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) identity.Config {
	return identity.Config{Pool: pool, Schema: schema, Logger: logger, UsersOwnedByAuth: cfg.UsersOwned, OrgsOwnedByAuth: cfg.OrgsOwned}
}
