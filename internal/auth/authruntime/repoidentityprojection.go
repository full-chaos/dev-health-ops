package authruntime

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/repoidentityprojection"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var repoIdentityFloorInputOnce sync.Once

// repoIdentityProjection builds the repository identity projection reader when AUTH_REPO_IDENTITY_PROJECTION=on
// (default off): one component, and the reader's restore-fence floor input, registered once per process. Off builds
// nothing. projectOrg copies an organization the auth copy lacks; nil parks such an event until the copy appears.
func repoIdentityProjection(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	registry *health.Registry, projectOrg func(context.Context, uuid.UUID) (bool, error),
) ([]lifecycle.Component, error) {
	if !cfg.RepoIdentityProjection {
		return nil, nil
	}
	readerCfg := repoidentityprojection.DefaultConfig(schema, registry)
	readerCfg.Pool, readerCfg.Logger, readerCfg.ProjectOrganization = pool, logger, projectOrg
	reader, err := repoidentityprojection.NewReader(readerCfg)
	if err != nil {
		return nil, fmt.Errorf("repository identity projection reader: %w", err)
	}
	repoIdentityFloorInputOnce.Do(func() {
		restorefence.RegisterFloorInput(repoidentityprojection.NewFloorInput(pool, schema))
	})
	return []lifecycle.Component{reader}, nil
}
