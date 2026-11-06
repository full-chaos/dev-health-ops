package authruntime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/outboxprojection"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushprojection"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var pushFloorInputOnce sync.Once

// pushProjection builds the External Push projection reader when AUTH_PUSH_PROJECTION=on (default off): one
// component, and the reader's restore-fence floor input, registered once per process. Off builds nothing. projectOrg
// is the organization projection the engine runs before its store step; nil skips it.
func pushProjection(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	registry *health.Registry, projectOrg func(context.Context, uuid.UUID) (bool, error),
) ([]lifecycle.Component, error) {
	if !cfg.PushProjection {
		return nil, nil
	}
	engine, err := outboxprojection.New(outboxprojection.Config{Pool: pool, Schema: schema, ProjectOrganization: projectOrg, Logger: logger})
	if err != nil {
		return nil, fmt.Errorf("push projection engine: %w", err)
	}
	readerCfg := pushprojection.DefaultConfig(schema, registry)
	readerCfg.Pool, readerCfg.Engine, readerCfg.Logger = pool, engine, logger
	reader, err := pushprojection.NewReader(readerCfg)
	if err != nil {
		return nil, fmt.Errorf("push projection reader: %w", err)
	}
	pushFloorInputOnce.Do(func() { restorefence.RegisterFloorInput(pushprojection.NewFloorInput(pool, schema)) })
	return []lifecycle.Component{reader}, nil
}

// organizationProjector adapts the identity store's sign-in organization projection to the engine's hook.
func organizationProjector(identities *identity.Store) func(context.Context, uuid.UUID) (bool, error) {
	return func(ctx context.Context, org uuid.UUID) (bool, error) {
		err := identities.ProjectOrganization(ctx, org)
		if err == nil {
			return true, nil
		}
		if isOrganizationNotFound(err) {
			return false, nil
		}
		return false, err
	}
}

func isOrganizationNotFound(err error) bool { return errors.Is(err, identity.ErrOrganizationNotFound) }

// organizationProjectorFor builds the identity store the projections copy organizations through, only when a
// projection (External Push or repository identity) is on.
func organizationProjectorFor(cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) (func(context.Context, uuid.UUID) (bool, error), error) {
	if !cfg.PushProjection && !cfg.RepoIdentityProjection {
		return nil, nil
	}
	identities, err := identity.New(identityConfig(cfg, logger, pool, schema))
	if err != nil {
		return nil, fmt.Errorf("push projection identity store: %w", err)
	}
	return organizationProjector(identities), nil
}
