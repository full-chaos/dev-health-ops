package authruntime

import (
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/outboxprojection"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// acrEventRoutes is the acr events route: built only when
// AUTH_ACR_EVENTS_WORKLOAD_SUBJECT is set (none otherwise), and mounted only
// with its endpoint-profile row.
func acrEventRoutes(cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	signer authapi.KeySetSource) ([]httpapi.Route, error) {
	if cfg.AcrEventsWorkloadSubject == "" {
		return nil, nil
	}
	caller, err := authapi.NewSessionIssueWorkload(authapi.SessionIssueWorkloadConfig{
		IssuerURL: cfg.TokenIssuerURL, Keys: signer, Subject: cfg.AcrEventsWorkloadSubject,
	})
	if err != nil {
		return nil, fmt.Errorf("authruntime: %s: %w", authconfig.EnvAcrEventsWorkloadSubject, err)
	}
	workloads, err := workload.New(workload.Config{Pool: pool, Schema: schema, Logger: logger})
	if err != nil {
		return nil, err
	}
	identities, err := identity.New(identityConfig(cfg, logger, pool, schema))
	if err != nil {
		return nil, err
	}
	engine, err := outboxprojection.New(outboxprojection.Config{Pool: pool, Schema: schema, Logger: logger,
		ProjectOrganization: identities.EnsureOrganizationFound})
	if err != nil {
		return nil, err
	}
	return authapi.AcrEventRoutes(authapi.AcrEventDeps{Workload: caller, Workloads: workloads, Projector: engine, Logger: logger})
}
