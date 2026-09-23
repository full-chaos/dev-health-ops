package apiservice

import (
	"github.com/full-chaos/dev-health-ops/internal/api/policy"

	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

// contractRoot is webhookintake.LoadJobRegistry's manifest directory,
// relative to the process's own working directory (the dho api image's
// WORKDIR /app, docker/go-worker.Dockerfile's dho target). A var, not a
// const, so a test two directories below the repo root can override it to
// "../../contracts/jobs/v1" -- internal/schedulerservice's own
// testContractRoot convention, restored after the test.
var contractRoot = "contracts/jobs/v1"

// apiDependencyError is a start-up failure at one named construction site. The
// shell logs only a DependencyReason, never an arbitrary error, so without this
// a failing site reached the log as a bare "configure runtime dependencies"
// line with no dependency named. Reason is a bounded constant per site.
type apiDependencyError struct {
	dependency string
	reason     string
	err        error
}

func (e apiDependencyError) Error() string            { return e.dependency + ": " + e.err.Error() }
func (e apiDependencyError) Unwrap() error            { return e.err }
func (e apiDependencyError) DependencyReason() string { return e.reason }

// dependencyFailure logs the failing dependency, its reason and the underlying
// error, and returns the reason-coded error the shell logs again. Every error
// reaching it is free of credentials: the storage opens return a fixed sentinel
// (ErrInvalidConfig), or ErrUnavailable followed by the driver's text with every
// credential component of the DSN redacted (secrets.WithRedactedCause), and the
// other sites return configuration or contract messages that never format a
// secret.
func dependencyFailure(ctx context.Context, logger *slog.Logger, dependency, reason string, err error) error {
	if logger != nil {
		logger.LogAttrs(ctx, slog.LevelError, "api dependency configuration failed",
			slog.String("dependency", dependency),
			slog.String("reason", reason),
			slog.String("error", err.Error()),
		)
	}
	return apiDependencyError{dependency: dependency, reason: reason, err: err}
}

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
	// Probes is the configuration the health probes report on.
	Probes ProbeConfig
	// Telemetry is the telemetry areas' configuration.
	Telemetry TelemetryConfig
	// Producer publishes to the job outbox (webhookintake's GitHub/GitLab/
	// Jira durable deliveries -- jobcontract.KindWebhookDelivery). Nil under
	// the same "not configured yet" rule as Pool.
	Producer *joboutbox.Producer
	// Decryptor decrypts pagerduty_webhook_bindings.signing_secret_encrypted
	// (wire-compatible with core/encryption.py's encrypt_value/decrypt_value)
	// and is reused by the admin org-deletion route (CHAOS-6306) to read a
	// stored PagerDuty OAuth token before revoking it. Its zero value is a
	// legal, always-failing decryptor -- see buildDeps's own comment on
	// SettingsEncryptionKey below.
	Decryptor providerfoundation.FernetDecryptor
	// PagerDuty is the admin org-deletion route's own dependency
	// (CHAOS-6306): the OAuth client config used to revoke a stored token.
	PagerDuty providerfoundation.PagerDutyRevokeConfig
	// HTTPDoer is the client the org-deletion route's PagerDuty revoke call
	// uses; nil means admin.Routes defaults it to http.DefaultClient. A
	// venue test overrides it to reach a fake revoke endpoint from the Go
	// plane too.
	HTTPDoer providerfoundation.HTTPDoer
	// ClickHouseDSN is the org-deletion route's analytics-table purge
	// connection (CHAOS-6306), sourced from CLICKHOUSE_URI -- the same
	// broadly-privileged, unrestricted-posture credential
	// internal/workerservice's own daily/sync/reports jobs already use, NOT
	// deps.ClickHouse/API_CLICKHOUSE_URI (that connection is locked to
	// chclickhouse.APIPosture's closed, exact manifest -- teams/identities
	// only -- so it structurally cannot hold ALTER DELETE on org-deletion's
	// live-discovered purge targets, which grow with every future
	// org_id-bearing table and can never be a fixed enumerated grant list).
	// "" = not configured; every ClickHouse count/delete is skipped with a
	// warning, matching org_deletion.py's own behavior when its ClickHouse
	// client cannot connect.
	ClickHouseDSN string
	// Now is injectable so a test can drive an area's own clock (e.g.
	// admin's keyed rate limiter, CHAOS-6357). Nil means time.Now, the
	// same "nil is the production default" contract every other Now field
	// in this codebase uses.
	Now func() time.Time
}

// TelemetryConfig is TELEMETRY_ENDPOINT (where /telemetry/report sends) and
// the Valkey DSN product-telemetry batches are appended to ("" = no stream:
// batches are accepted with stream "disabled").
type TelemetryConfig struct {
	Endpoint  string
	ValkeyURI string
}

// ProbeConfig is what /health, /ready and /health/workers check beyond
// deps.Pool/deps.ClickHouse (both already carried on Deps itself): the
// Valkey DSN ("" = not configured) and EXPECTED_WORKER_GROUPS (nil =
// unset). ClickHouse has no DSN field here on purpose -- CHAOS-6310 r1:
// the health check reuses deps.ClickHouse (the api's own dedicated login),
// never a second connection from a separate generic DSN.
type ProbeConfig struct {
	ValkeyURI            string
	ExpectedWorkerGroups *[]string
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
			return Deps{}, nil, dependencyFailure(ctx, logger, "api_postgres", "api_postgres_open_failed", err)
		}
		deps.Pool = pool
		components = append(components, &pgxpoolComponent{pool: pool})
		if err := registry.RegisterRequired(apiDatabaseCheck, func(checkCtx context.Context) error {
			return postgres.CheckAPIAuthorization(checkCtx, pool, cfg.APIDatabaseRole, cfg.RiverDatabaseSchema)
		}); err != nil {
			return Deps{}, nil, dependencyFailure(ctx, logger, "api_postgres", "api_postgres_check_register_failed", err)
		}

		jobRegistry, err := webhookintake.LoadJobRegistry(contractRoot)
		if err != nil {
			return Deps{}, nil, dependencyFailure(ctx, logger, "webhook_job_registry", "api_job_registry_load_failed", fmt.Errorf("load job contracts for webhook intake: %w", err))
		}
		producer, err := joboutbox.NewProducer(pool, jobRegistry)
		if err != nil {
			return Deps{}, nil, dependencyFailure(ctx, logger, "webhook_outbox_producer", "api_outbox_producer_failed", fmt.Errorf("build webhook intake job outbox producer: %w", err))
		}
		deps.Producer = producer
		// SettingsEncryptionKey unconfigured leaves Decryptor at its zero
		// value: PagerDuty's route needs it at request time and answers 500
		// there (the same "not yet bootstrapped" shape as an unconfigured
		// Pool/Valkey) -- it must not stop the whole process from starting.
		if cfg.SettingsEncryptionKey.Configured() {
			decryptor, err := providerfoundation.NewFernetDecryptor(cfg.SettingsEncryptionKey, cfg.SettingsEncryptionSalt.Reveal())
			if err != nil {
				return Deps{}, nil, dependencyFailure(ctx, logger, "webhook_secret_decryptor", "api_secret_decryptor_failed", fmt.Errorf("build webhook intake secret decryptor: %w", err))
			}
			deps.Decryptor = decryptor
		}
	}

	if cfg.ValkeyURI.Configured() {
		client, err := valkey.Open(ctx, valkey.DefaultConfig(cfg.ValkeyURI.Reveal()))
		if err != nil {
			return Deps{}, nil, dependencyFailure(ctx, logger, "api_valkey", "api_valkey_open_failed", err)
		}
		deps.Valkey = client
		components = append(components, &valkeyComponent{client: client})
		if err := registry.RegisterRequired(apiValkeyCheck, func(ctx context.Context) error {
			return client.Do(ctx, client.B().Ping().Build()).Error()
		}); err != nil {
			return Deps{}, nil, dependencyFailure(ctx, logger, "api_valkey", "api_valkey_check_register_failed", err)
		}
	}

	if cfg.APIClickHouseURI.Configured() {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(cfg.APIClickHouseURI.Reveal()))
		if err != nil {
			return Deps{}, nil, dependencyFailure(ctx, logger, "api_clickhouse", "api_clickhouse_open_failed", err)
		}
		deps.ClickHouse = conn
		components = append(components, &clickHouseComponent{conn: conn})
		if err := registry.RegisterRequired(apiClickHouseCheck, func(checkCtx context.Context) error {
			return chclickhouse.CheckAPIClickHouseAuthorization(checkCtx, conn)
		}); err != nil {
			return Deps{}, nil, dependencyFailure(ctx, logger, "api_clickhouse", "api_clickhouse_check_register_failed", err)
		}
	}

	logger.LogAttrs(ctx, slog.LevelInfo, "api dependencies configured",
		slog.Bool("postgres_configured", deps.Pool != nil),
		slog.Bool("valkey_configured", deps.Valkey != nil),
		slog.Bool("clickhouse_configured", deps.ClickHouse != nil),
		slog.Bool("webhook_outbox_producer_configured", deps.Producer != nil),
		slog.Bool("webhook_secret_decryptor_configured", deps.Pool != nil && cfg.SettingsEncryptionKey.Configured()),
	)
	return deps, components, nil
}
