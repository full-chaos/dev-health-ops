package authruntime

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Stub: the next commit builds the feed.
func revisionFeed(authconfig.Config, *slog.Logger, *pgxpool.Pool, authschema.ValidatedIdentifier) (*audit.Publisher, []lifecycle.Component, error) {
	return nil, nil, nil
}
