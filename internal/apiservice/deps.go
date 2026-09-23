package apiservice

import (
	"github.com/full-chaos/dev-health-ops/internal/api/policy"

	"context"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

// apiDatabaseCheck and apiValkeyCheck are declared in service.go, alongside
// listenerCheck -- one place names every readiness check this Service
// registers.

// Deps are the shared handles every `api` route area is built from. The api
// Service owns exactly one Postgres pool and one Valkey client; an area
// package (internal/api/<area>, internal/apiservice/<area>) never opens its
// own.
type Deps struct {
	// Pool is nil when API_DATABASE_URI is not configured (the CHAOS-6269
	// bootstrap has not run yet in this environment). A route needing it
	// answers CodeInternal rather than dereferencing a nil pool; the
	// api_database readiness check is what surfaces the real cause.
	Pool *pgxpool.Pool
	// Valkey is nil under the same "not configured yet" rule as Pool.
	Valkey valkeygo.Client
	// ClickHouse is nil when CLICKHOUSE_URI is not configured. A route area
	// that needs it (internal/api/teamsidentity, CHAOS-6251) answers
	// CodeInternal rather than dereferencing a nil connection; the
	// api_clickhouse readiness check surfaces the real cause.
	ClickHouse driver.Conn
	// Auth and Guard are the protected-route runtime (internal/api/policy),
	// nil without a pool: Auth for routes that check membership themselves,
	// Guard to wrap a handler with its authorization level.
	Auth  *policy.Authenticator
	Guard *policy.Guard
}

// pgxpoolComponent closes the pool on shutdown. Start performs no I/O, for
// the same reason internal/auth/authstore.Postgres.Open does not: a
// configured-but-unreachable database must not block process startup --
// readiness is re-answered live by the registered check, not by Start.
type pgxpoolComponent struct{ pool *pgxpool.Pool }

func (p *pgxpoolComponent) Name() string { return "api-postgres" }

func (p *pgxpoolComponent) Start(context.Context) error { return nil }

func (p *pgxpoolComponent) Shutdown(context.Context) error {
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}

// valkeyComponent closes the client on shutdown.
type valkeyComponent struct{ client valkeygo.Client }

func (v *valkeyComponent) Name() string { return "api-valkey" }

func (v *valkeyComponent) Start(context.Context) error { return nil }

func (v *valkeyComponent) Shutdown(context.Context) error {
	if v.client != nil {
		v.client.Close()
	}
	return nil
}

// clickHouseComponent closes the connection on shutdown.
type clickHouseComponent struct{ conn driver.Conn }

func (c *clickHouseComponent) Name() string { return "api-clickhouse" }

func (c *clickHouseComponent) Start(context.Context) error { return nil }

func (c *clickHouseComponent) Shutdown(context.Context) error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// buildDeps opens the pool and the Valkey client this process needs for the
// route areas mounted under `dho api`, and registers a readiness check for
// each ONLY when it is configured -- matching CHAOS-6244's original acr
// wiring exactly (RegisterRequired lived inside the same `if …Configured()`
// block, never outside it). Neither is required to be configured: a
// deployment that has not yet run the CHAOS-6269 bootstrap, or a route area
// that needs neither, still starts and reports /readyz 200 -- the container
// smoke test runs `dho api` with no database or Valkey configured at all and
// asserts exactly that. A route area that DOES need a dependency but finds
// it unconfigured answers CodeInternal on the route itself (Deps.Pool/.Valkey
// is nil), not by making the whole process permanently unready.
//
// The Postgres check, when registered, is postgres.CheckAPIAuthorization
// (api_authorization.go), not a bare Ping: it proves the login is the
// declared api role AND holds EXACTLY apiPosture()'s privilege manifest, no
// more and no less -- the same posture-proof shape domainPosture/
// coordinatorPosture already use for the River runtime roles. Every route PR
// that adds a table this Service reads or writes adds it to apiPosture() in
// the same PR (CHAOS-6244's own rule, restated in api_authorization.go's doc
// comment).
func buildDeps(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
) (Deps, []lifecycle.Component, error) {
	var deps Deps
	var components []lifecycle.Component

	if cfg.APIDatabaseURI.Configured() {
		pool, err := postgres.New(ctx, postgres.DefaultConfig(cfg.APIDatabaseURI.Reveal()))
		if err != nil {
			return Deps{}, nil, err
		}
		deps.Pool = pool
		components = append(components, &pgxpoolComponent{pool: pool})
		if err := registry.RegisterRequired(apiDatabaseCheck, func(checkCtx context.Context) error {
			return postgres.CheckAPIAuthorization(checkCtx, pool, cfg.APIDatabaseRole, cfg.RiverDatabaseSchema)
		}); err != nil {
			return Deps{}, nil, err
		}
	}

	if cfg.ValkeyURI.Configured() {
		client, err := valkey.Open(ctx, valkey.DefaultConfig(cfg.ValkeyURI.Reveal()))
		if err != nil {
			return Deps{}, nil, err
		}
		deps.Valkey = client
		components = append(components, &valkeyComponent{client: client})
		if err := registry.RegisterRequired(apiValkeyCheck, func(ctx context.Context) error {
			return client.Do(ctx, client.B().Ping().Build()).Error()
		}); err != nil {
			return Deps{}, nil, err
		}
	}

	if cfg.ClickHouseURI.Configured() {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(cfg.ClickHouseURI.Reveal()))
		if err != nil {
			return Deps{}, nil, err
		}
		deps.ClickHouse = conn
		components = append(components, &clickHouseComponent{conn: conn})
		if err := registry.RegisterRequired(apiClickHouseCheck, func(checkCtx context.Context) error {
			return conn.Ping(checkCtx)
		}); err != nil {
			return Deps{}, nil, err
		}
	}

	logger.LogAttrs(ctx, slog.LevelInfo, "api dependencies configured",
		slog.Bool("postgres_configured", deps.Pool != nil),
		slog.Bool("valkey_configured", deps.Valkey != nil),
		slog.Bool("clickhouse_configured", deps.ClickHouse != nil),
	)
	return deps, components, nil
}
