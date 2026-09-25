// Package apiservice is `dho api`: the Go HTTP api that takes over the
// Python api's REST routes one area at a time.
//
// This package holds the service shell: transport, middleware, and the top-
// level Routes() that composes each area package's own route set. CHAOS-6244
// mounted the first business routes (the acr area, internal/apiservice/acr);
// CHAOS-6246 added the external-ingest area (internal/api/externalingest);
// CHAOS-6247 added the webhook-intake area (internal/api/webhookintake);
// every other path still gets the Python api's own 404 body until its own
// area package adds routes. What runs for every request, business route or
// not, is the transport every route inherits:
//
//   - the shared process runtime (internal/platform/shell): flags > env
//     configuration, the redacting JSON logger, OTEL, and the operator
//     listener with /healthz, /readyz and /metrics on --http-addr;
//   - a SEPARATE api listener on --api-addr, so probes never meet request
//     middleware;
//   - the transport middleware of internal/auth/httpapi (request id, panic
//     recovery, body bound, deadline), rendering every error in the Python
//     api's wire shape (errors.go);
//   - the Python api's security headers and CORS behaviour (headers.go,
//     cors.go), in the Python request order.
//
// Per-route policy (principal, org scope, impersonation, client rate limits,
// origin validation, licensing, audit) arrives with the first route that needs
// it; see the design spec's section 4.
package apiservice

import (
	"context"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/api/apimetrics"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/api/buildinfo"
	"github.com/full-chaos/dev-health-ops/internal/api/credentials"
	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/api/githubapp"
	healthroutes "github.com/full-chaos/dev-health-ops/internal/api/health"
	"github.com/full-chaos/dev-health-ops/internal/api/integrationsadmin"
	"github.com/full-chaos/dev-health-ops/internal/api/legacyingest"
	"github.com/full-chaos/dev-health-ops/internal/api/orgs"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/producttelemetry"
	"github.com/full-chaos/dev-health-ops/internal/api/session"
	"github.com/full-chaos/dev-health-ops/internal/api/syncadmin"
	"github.com/full-chaos/dev-health-ops/internal/api/teamsidentity"
	"github.com/full-chaos/dev-health-ops/internal/api/telemetry"
	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/customerpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// requestTimeout bounds every request context. The Python api has no
	// per-request bound; a route that needs a different one declares it when it
	// is ported.
	requestTimeout = 60 * time.Second
	// maxBodyBytes bounds every request body at the ingress in front of both
	// planes (proxy-body-size 50m on the prod ops ingress). The Python api has
	// no bound of its own, so any body the ingress forwards reaches it; this
	// default keeps that true for the Go api.
	maxBodyBytes = 50 << 20
	// maxHeaderBytes is the smallest bound under which the api accepts every
	// request head the Python api accepts. uvicorn on h11 (uv.lock), with the
	// head arriving in one piece, accepts a request line of up to 127,917
	// bytes of target and a single header value of up to 127,887 bytes;
	// net/http reads MaxHeaderBytes plus its own slop (measured +4,040 bytes
	// for a long target, +4,019 for a long header). The raw-HTTP golden pins
	// the edges.
	maxHeaderBytes = 123877
	// maxHeaderValueCount is high enough that the byte bound, not a count,
	// ends a request head, as in uvicorn, which has no count limit: the
	// shortest header line is 4 bytes, so 32,768 lines cannot fit the bound.
	maxHeaderValueCount = 32 << 10
	// idleTimeout is uvicorn's timeout_keep_alive default (5 s), which the
	// Python api runs with: a client reusing a connection idle longer than
	// that finds it closed on either plane.
	idleTimeout = 5 * time.Second
	// listenerCheck is the readiness check that fails until the api listener
	// is bound.
	listenerCheck = "api_listener"
	// apiDatabaseCheck is the readiness check that fails until the api role's
	// (CHAOS-6269, devhealth_api) Postgres pool is reachable AND holds exactly
	// its declared apiPosture() privilege manifest -- see
	// postgres.CheckAPIAuthorization's doc comment. It is registered only
	// when APIDatabaseURI is configured (buildDeps, deps.go).
	apiDatabaseCheck = "api_database"
	// apiValkeyCheck is the readiness check that fails until the Valkey
	// client (the external-ingest area's stream producer, and
	// webhookintake's PagerDuty replay-claim/stream-write path) can PING.
	// It is registered only when ValkeyURI is configured (buildDeps, deps.go).
	apiValkeyCheck = "api_valkey"
	// apiClickHouseCheck is the readiness check that fails until the
	// ClickHouse connection (internal/api/teamsidentity, CHAOS-6251, the
	// first Go api ClickHouse writer) can PING. It is registered only when
	// CLICKHOUSE_URI is configured (buildDeps, deps.go).
	apiClickHouseCheck = "api_clickhouse"
)

// Spec is the shell specification of `dho api`. Exported so a test can run
// the real service through shell.Execute.
var Spec = shell.Spec{
	Service:                         config.APIServiceName,
	Invocation:                      "dho api",
	TraceServiceName:                config.APIServiceName,
	ConfigureDependenciesWithLogger: configure,
}

// Command is the `api` vertical of the dho binary.
func Command() cli.Command {
	return cli.Command{
		Name:    "api",
		Summary: "serve the Go HTTP api",
		Kind:    cli.Service,
		Run: func(ctx context.Context, env cli.Env) int {
			return shell.Execute(ctx, Spec, env.Args, env.Lookup, shell.IO{
				Stdout: env.Stdout,
				Stderr: env.Stderr,
			})
		},
	}
}

// credentialProbeClient and credentialHostLookup are the credential
// connection test's outbound seams: nil in production (a client that follows
// no redirects, the system resolver); the venue oracle replaces them to reach
// a stub provider through the same SSRF guard.
var (
	credentialProbeClient *http.Client
	credentialHostLookup  func(context.Context, string) ([]netip.Addr, error)
)

// Routes is the route set the api mounts, built from deps (the shared
// Postgres pool and Valkey client every area package is handed rather than
// opening its own). deps.Pool is nil when APIDatabaseURI is not configured;
// acr.Routes still mounts both of its paths in that case (see acr.Deps's
// doc comment) -- the ingress path table switch (spec.md §4.6), not process
// configuration, decides whether any traffic ever reaches them, and the
// entitlement route answers 503 rather than being silently absent from the
// mux. webhookintake's routes register unconditionally too: a handler that
// needs a live Pool/Valkey/Producer/Decryptor answers 500 at request time
// (its own Deps doc comment). Each area package contributes its own
// []httpapi.Route; this function only concatenates them.
func Routes(deps Deps, logger *slog.Logger) []httpapi.Route {
	limits, err := limitStore(deps)
	if err != nil {
		// Only a nil Valkey client can fail it and that path is not taken; keep
		// the process up on the in-process store rather than mount no limits.
		logger.Error("api: shared rate limiter unavailable; using the in-process limiter", "error", err)
		limits = httpapi.NewMemoryCounters(deps.Now)
	}
	var store acr.EntitlementStore
	if deps.Pool != nil {
		store = acr.PostgresEntitlementStore{Pool: deps.Pool}
	}
	var routes []httpapi.Route
	routes = append(routes, acr.Routes(acr.Deps{Store: store, Logger: logger})...)
	routes = append(routes, externalingest.Routes(externalingest.Deps{
		Pool:     deps.Pool,
		Valkey:   deps.Valkey,
		Logger:   logger,
		Counters: limits,
	})...)
	var legacyStore legacyingest.Store
	if deps.Valkey != nil {
		legacyStore = legacyingest.ValkeyStore{Client: deps.Valkey}
	}
	// The telemetry route writes ClickHouse with the api's own login; with
	// none configured it accepts and skips, as the Python route does.
	var legacyClickHouse legacyingest.ClickHouse
	if deps.ClickHouse != nil {
		legacyClickHouse = deps.ClickHouse
	}
	routes = append(routes, legacyingest.Routes(legacyingest.Deps{Store: legacyStore, Metrics: deps.LegacyIngestMetrics, ClickHouse: legacyClickHouse, Logger: logger})...)
	routes = append(routes, healthroutes.Routes(healthroutes.Deps{
		// deps.ClickHouse is the api's own dedicated ClickHouse login
		// (CHAOS-6310), the SAME connection internal/api/teamsidentity's
		// routes use -- one process, one ClickHouse credential (R395),
		// never a fresh connection from a separate, generic DSN a real
		// deployment's api Secret has no reason to also carry.
		Pool: deps.Pool, ClickHouse: deps.ClickHouse, ValkeyURI: deps.Probes.ValkeyURI,
		ExpectedWorkerGroups: deps.Probes.ExpectedWorkerGroups, Logger: logger,
		RateLimiterBackend: limits.Backend(),
	})...)
	routes = append(routes, producttelemetry.Routes(&producttelemetry.ValkeyStreams{URI: deps.Telemetry.ValkeyURI}, logger)...)
	routes = append(routes, buildinfo.Routes(deps.Guard, version.Current("api"))...)
	routes = append(routes, session.Routes(session.Deps{
		Pool: deps.Pool, Guard: deps.Guard, Auth: deps.Auth, Verifier: deps.Verifier, Signer: deps.Signer,
		// The api's own ClickHouse login: organization activity.
		ClickHouse: deps.ClickHouse, Limits: limits, Write: WriteError, OAuth: deps.SessionOAuth, Logger: logger,
		Mail: deps.Invites, RegisterLimit: deps.RegisterLimit,
	})...)
	if deps.Guard != nil {
		routes = append(routes, orgs.Routes(deps.Pool, deps.Guard, logger)...)
		routes = append(routes, telemetry.Routes(deps.Pool, deps.Guard, deps.Auth, deps.Telemetry.Endpoint, logger)...)
		routes = append(routes, customerpush.Routes(customerpush.Deps{Pool: deps.Pool, Guard: deps.Guard, Logger: logger})...)
		routes = append(routes, syncadmin.Routes(syncadmin.Deps{Pool: deps.Pool, ClickHouse: deps.ClickHouse, Guard: deps.Guard, Logger: logger, Decryptor: deps.Decryptor, Now: deps.Now, JiraHTTP: deps.SyncJiraHTTP})...)
		routes = append(routes, credentials.Routes(credentials.Deps{Pool: deps.Pool, Guard: deps.Guard, Cipher: deps.Decryptor, Logger: logger, Now: deps.Now,
			HTTPClient: credentialProbeClient, HostLookup: credentialHostLookup})...)
		routes = append(routes, githubapp.Routes(githubapp.Deps{Pool: deps.Pool, Guard: deps.Guard, Valkey: deps.Valkey, Cipher: deps.Decryptor,
			Logger: logger, Now: deps.Now, Config: deps.GitHubApp, Signer: deps.GitHubStateSigner,
			HTTPClient: deps.GitHubAppHTTPClient, GitHubURL: deps.GitHubAppURL, GitHubAPIURL: deps.GitHubAppAPIURL})...)
		routes = append(routes, integrationsadmin.Routes(integrationsadmin.Deps{Pool: deps.Pool, Guard: deps.Guard, Logger: logger, Now: deps.Now,
			Discovery: integrationDiscovery(deps, logger)})...)
		if deps.ClickHouse != nil {
			routes = append(routes, teamsidentity.Routes(deps.ClickHouse, deps.Guard, logger, deps.Pool, deps.Decryptor)...)
		}
	}
	// admin is this Service's other consumer of policy.Guard: mounted only
	// when the protected-route runtime is actually up (deps.Pool
	// configured); with no pool these paths are simply absent from the mux,
	// same as any other not-yet-ported area.
	if deps.Pool != nil && deps.Guard != nil {
		routes = append(routes, billing.Routes(billing.Deps{
			Pool: deps.Pool, Guard: deps.Guard, Stripe: deps.Stripe, Config: deps.BillingConfig, Logger: logger,
			WebhookSecret: deps.StripeWebhookSecret, LicensePrivateKey: deps.LicensePrivateKey, Producer: deps.Producer,
		})...)
		routes = append(routes, admin.Routes(admin.Deps{
			Pool:          deps.Pool,
			Valkey:        deps.Valkey,
			Guard:         deps.Guard,
			Logger:        logger,
			ClickHouseDSN: deps.ClickHouseDSN,
			Decryptor:     deps.Decryptor,
			PagerDuty:     deps.PagerDuty,
			HTTPDoer:      deps.HTTPDoer,
			Now:           deps.Now,
			Write:         WriteError,
			Limits:        limits,
			Invites:       deps.Invites,
		})...)
	}
	routes = append(routes, webhookintake.Routes(webhookintake.Deps{
		Pool:      deps.Pool,
		Valkey:    deps.Valkey,
		Producer:  deps.Producer,
		Decryptor: deps.Decryptor,
		Logger:    logger,
		Counters:  limits,
	})...)
	return markResponseModels(routes)
}

func configure(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
) ([]lifecycle.Component, error) {
	return configureWith(ctx, cfg, registry, logger, nil)
}

// RegisterOperatorMetrics puts the api's counters on the operator
// /metrics, the Go api's scrape surface (the Python api served its
// prometheus_client counters on /metrics): the OTel instruments, each
// declared under the Python counter name, as one fragment, and the
// legacy-ingest refusal counter, which it sets on deps, as another.
func RegisterOperatorMetrics(registry *health.Registry, deps *Deps) error {
	source, err := apimetrics.Install()
	if err != nil {
		return err
	}
	if err := registry.RegisterMetrics("api_instruments", source); err != nil {
		return err
	}
	deps.LegacyIngestMetrics = legacyingest.NewMetrics()
	return registry.RegisterMetrics("legacy_ingest", deps.LegacyIngestMetrics)
}

// configureWith is configure with adjust applied to the built Deps before
// the routes are composed. Only a test passes adjust (to point a client at
// a fake external service); production always runs configure.
func configureWith(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
	adjust func(*Deps),
) ([]lifecycle.Component, error) {
	deps, depComponents, err := buildDeps(ctx, cfg, registry, logger)
	if err != nil {
		return nil, err
	}
	var scope []func(http.Handler) http.Handler
	if deps.Pool != nil {
		protected, err := newProtection(cfg, deps.Pool, logger)
		if err != nil {
			closeComponents(depComponents)
			return nil, dependencyFailure(ctx, logger, "api_access_token_verifier", "api_protection_config_failed", err)
		}
		deps.Auth, deps.Guard = protected.auth, protected.guard
		deps.Verifier, deps.Signer = protected.verifier, protected.signer
		scope = []func(http.Handler) http.Handler{protected.scope.OrgScope, protected.scope.Impersonation}
	}
	if err := RegisterOperatorMetrics(registry, &deps); err != nil {
		closeComponents(depComponents)
		return nil, err
	}
	deps.Probes = ProbeConfig{
		ValkeyURI:            cfg.ValkeyURI.Reveal(),
		ExpectedWorkerGroups: cfg.APIExpectedWorkerGroups,
	}
	deps.Telemetry = TelemetryConfig{Endpoint: cfg.TelemetryEndpoint, ValkeyURI: cfg.ValkeyURI.Reveal()}
	// deps.Decryptor is already set by buildDeps (webhookintake's own
	// SettingsEncryptionKey gating) when a pool is configured -- the only
	// case admin.Routes (and its org-deletion PagerDuty revoke call) is
	// ever mounted at all. PagerDuty is the org-deletion route's own
	// dependency (CHAOS-6306): the OAuth client config used to revoke a
	// stored token.
	deps.Stripe = stripeclient.New(stripeclient.Options{Key: cfg.StripeSecretKey.Reveal()})
	deps.BillingConfig = cfg.APIBilling
	deps.StripeWebhookSecret = cfg.StripeWebhookSecret
	deps.LicensePrivateKey = cfg.LicensePrivateKey
	deps.PagerDuty = providerfoundation.PagerDutyRevokeConfig{ClientID: cfg.PagerDutyOAuthClientID.Reveal(), ClientSecret: cfg.PagerDutyOAuthSecret.Reveal(), RedirectURI: cfg.PagerDutyOAuthRedirectURI}
	// deps.ClickHouseDSN: see Deps' own doc comment for why this is
	// CLICKHOUSE_URI, never API_CLICKHOUSE_URI.
	if cfg.ClickHouseURI.Configured() {
		deps.ClickHouseDSN = cfg.ClickHouseURI.Reveal()
	}
	deps.Invites = inviteConfig(cfg, logger, os.LookupEnv)
	deps.GitHubApp = GitHubAppConfig(os.LookupEnv)
	deps.GitHubStateSigner = githubapp.Signer{Secret: cfg.APIJWTSecret.Reveal(), Issuer: cfg.APIJWTIssuer, Audience: cfg.APIJWTAudience}
	deps.RegisterLimit, err = registerLimit(os.LookupEnv)
	if err != nil {
		closeComponents(depComponents)
		return nil, dependencyFailure(ctx, logger, "api_server", "api_register_limit_invalid", err)
	}
	if adjust != nil {
		adjust(&deps)
	}
	server, err := NewServer(cfg, logger, Routes(deps, logger), scope...)
	if err != nil {
		return nil, dependencyFailure(ctx, logger, "api_server", "api_server_config_failed", err)
	}
	// The rate-limit store error counter is scraped from the operator
	// /metrics: the api installs no OTel meter provider, so this is the one
	// place it is observable.
	if err := registry.RegisterMetrics("api_rate_limit_store", httpapi.RateLimitStoreErrors); err != nil {
		return nil, dependencyFailure(ctx, logger, "api_server", "api_metrics_register_failed", err)
	}
	if err := registry.RegisterRequired(listenerCheck, func(context.Context) error {
		if server.Address() == "" {
			return errors.New("api listener is not bound")
		}
		return nil
	}); err != nil {
		return nil, dependencyFailure(ctx, logger, "api_server", "api_listener_check_register_failed", err)
	}
	return append(depComponents, server), nil
}

// protection is the protected-route runtime built over the api pool.
type protection struct {
	auth     *policy.Authenticator
	scope    *policy.Scope
	guard    *policy.Guard
	verifier *edgetoken.Verifier
	signer   *edgetoken.Signer
}

// newProtection builds the principal service. The api verifies the access
// token the Python api mints, so with a database it needs the same key:
// JWT_SECRET_KEY is required, and a missing or short key stops startup.
func newProtection(cfg config.Config, pool *pgxpool.Pool, logger *slog.Logger) (protection, error) {
	if !cfg.APIJWTSecret.Configured() {
		return protection{}, errors.New("JWT_SECRET_KEY is required by dho api when API_DATABASE_URI is set")
	}
	verifier, err := edgetoken.New(cfg.APIJWTSecret.Reveal(), cfg.APIJWTIssuer, cfg.APIJWTAudience)
	if err != nil {
		return protection{}, fmt.Errorf("access-token verifier: %w", err)
	}
	signer, err := edgetoken.NewSigner(cfg.APIJWTSecret.Reveal(), cfg.APIJWTIssuer, cfg.APIJWTAudience)
	if err != nil {
		return protection{}, fmt.Errorf("access-token signer: %w", err)
	}
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		return protection{}, err
	}
	return protection{auth: auth, scope: policy.NewScope(auth, logger), guard: policy.NewGuard(auth, logger),
		verifier: verifier, signer: signer}, nil
}

// closeComponents releases what buildDeps opened when startup fails later.
func closeComponents(components []lifecycle.Component) {
	for index := len(components) - 1; index >= 0; index-- {
		_ = components[index].Shutdown(context.Background())
	}
}

// NewServer builds the api listener with the full transport stack. The stack
// order, request side first, is: the plane/build provenance stamp (outside
// every handler-chain layer so no response it produces, scope rejection or
// unhandled error, lacks it),
// request id, panic recovery, then scope (the
// org scope and impersonation middlewares, when given), the register
// route's origin check, security headers, CORS, then the mux (and, per route, recovery, deadline, body bound). It
// matches the Python api's request order (src/dev_health_ops/api/
// _middleware.py registers in reverse): OrgIdMiddleware and
// ImpersonationMiddleware run outside SecurityHeadersMiddleware and
// CORSMiddleware, so their own 403 carries neither.
func NewServer(
	cfg config.Config,
	logger *slog.Logger,
	routes []httpapi.Route,
	scope ...func(http.Handler) http.Handler,
) (*httpapi.Server, error) {
	middleware := append([]func(http.Handler) http.Handler{buildinfo.Stamp(version.Current("api")), UnhandledErrorShape, CloseHTTP10, DecodedPathRouting}, scope...)
	middleware = append(middleware, NewOriginValidation(cfg.CORSAllowedOrigins).Wrap, SecurityHeaders, NewCORS(cfg.CORSAllowedOrigins).Wrap)
	return httpapi.NewServer(httpapi.ServerOptions{
		Name:           "api-http",
		Address:        cfg.APIAddress,
		Logger:         logger,
		Routes:         routes,
		RequestTimeout: requestTimeout,
		MaxBodyBytes:   maxBodyBytes,
		ErrorWriter:    WriteError,
		// The Python api echoes any non-empty X-Request-ID
		// (api/middleware/correlation_id.py) and routes the raw path, never
		// redirecting one.
		AcceptRequestID:     func(id string) bool { return id != "" },
		StrictPaths:         true,
		MaxHeaderBytes:      maxHeaderBytes,
		MaxHeaderValueCount: maxHeaderValueCount,
		IdleTimeout:         idleTimeout,
		// FastAPI routes declared with @router.get do not answer HEAD.
		ExplicitHead: true,
		// Starlette's Router redirects a trailing-slash variant of a route
		// path (redirect_slashes, on by default in FastAPI) with a 307.
		RedirectSlashes: true,
		// The redirect's scheme honours X-Forwarded-Proto from the peers
		// uvicorn trusts: FORWARDED_ALLOW_IPS, default 127.0.0.1.
		ForwardedAllowIPs: forwardedAllowIPs(),
		Middleware:        middleware,
	})
}

// forwardedAllowIPs is FORWARDED_ALLOW_IPS as uvicorn reads it: nil when
// unset (uvicorn's default applies), the raw value otherwise.
func forwardedAllowIPs() *string {
	value, ok := os.LookupEnv("FORWARDED_ALLOW_IPS")
	if !ok {
		return nil
	}
	return &value
}

// integrationDiscovery builds the source discovery the integration discover
// route runs, the one implementation the sync config create path shares, on
// the api pool. nil without a pool or a decryptor.
func integrationDiscovery(deps Deps, logger *slog.Logger) schedsync.SourceDiscoveryExecutor {
	if deps.Pool == nil {
		return nil
	}
	var client providerfoundation.HTTPDoer
	if deps.SyncJiraHTTP != nil {
		client = deps.SyncJiraHTTP
	}
	discovery, err := schedsync.NewAPISourceDiscovery(deps.Pool, deps.Decryptor, client, logger, deps.Now)
	if err != nil {
		logger.Error("integration discover: source discovery is unavailable", "error", err)
		return nil
	}
	return discovery
}
