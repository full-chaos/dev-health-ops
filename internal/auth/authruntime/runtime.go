// Package authruntime composes the auth-service process.
//
// It exists so that cmd/auth-service/main.go can be a thin entrypoint and
// nothing else: Execute takes its arguments, its environment and its streams
// as parameters, touches no package-level state, and returns an exit code
// instead of calling os.Exit, so it is callable from a test as easily as from
// main. See main.go's package doc for the one-line design note on why thin
// mains are worth the small indirection -- and for why that note is not a
// licence to build a plugin host here.
//
// It wires interfaces: token.Signer over signing-key custody,
// authstore.Prober for storage, httpapi.Route for transport. The domain
// packages plug in at this one site: the session routes (internal/auth/authapi
// over internal/auth/session) mount here when their secrets are configured.
package authruntime

import (
	"context"
	"errors"
	"flag"
	"fmt"
	envsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/apimetrics"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/authstore"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Readiness check names. They are metric label values (see
// health.Registry.RegisterRequired's name pattern) and the strings an operator
// reads in a /readyz body, so they are declared once here.
const (
	CheckPostgres   = "postgres"
	CheckSigningKey = "signing_key"
)

// IO is the process's stream set, injectable for tests.
type IO struct {
	Stdout io.Writer
	Stderr io.Writer
}

// maxInFlightRequests caps concurrent requests on the auth API listener; at the
// 1 MiB default body limit that bounds buffering near 64 MiB.
const maxInFlightRequests = 64

// sessionRoutes is the service's mounted route set when cfg.Session is
// enabled, and nothing otherwise: the session routes (CHAOS-10228) and
// go-api's compatibility refresh, logout and social-login, and login when the
// lockout key is set too. Without the session
// secrets the service stays as CHAOS-4881 built it, dormant: every path
// receives the 404 envelope and no token is issued. No deployed stack sets
// those secrets yet, and go-api routes to these only behind its AUTH_PROXY_*
// flags, all off; prod enablement is a separate decision (CHAOS-3275 D1).
func sessionRoutes(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
) ([]httpapi.Route, error) {
	if !cfg.Session.Enabled {
		return nil, nil
	}
	sessions, err := session.New(session.Config{
		Pool: pool, Schema: schema, Logger: logger,
		SuccessorKey: contracts.NewSecretString(cfg.Session.SuccessorKey.Reveal()),
	})
	if err != nil {
		return nil, err
	}
	issuer, err := authapi.NewIssuer(cfg.Session.AccessSecret.Reveal(), cfg.Session.AccessIssuer, cfg.Session.AccessAudience)
	if err != nil {
		return nil, err
	}
	routes, err := authapi.Routes(authapi.Deps{
		Sessions: sessions, Users: authapi.PGUsers{Pool: pool}, Issuer: issuer, Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	compat, err := compatRoutes(cfg, logger, pool, schema, sessions, issuer)
	if err != nil {
		return nil, err
	}
	return append(routes, compat...), nil
}

// compatRoutes builds go-api's compatibility routes over the identity store,
// the social provider client and the append-only audit writer.
func compatRoutes(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	sessions *session.Store, issuer *authapi.Issuer,
) ([]httpapi.Route, error) {
	identities, err := identity.New(identity.Config{Pool: pool, Schema: schema, Logger: logger})
	if err != nil {
		return nil, err
	}
	clients := make(map[social.Provider]social.ClientCredentials, len(authconfig.SocialProviders))
	for _, name := range authconfig.SocialProviders {
		provider, ok := social.ParseProvider(name)
		if !ok {
			return nil, fmt.Errorf("authruntime: unknown social provider %q", name)
		}
		client := cfg.Social[name]
		clients[provider] = social.NewClientCredentials(client.ID, client.Secret.Reveal())
	}
	socialService, err := social.New(social.Config{
		Store:     identities,
		Providers: &social.ProviderClient{HTTP: &http.Client{Transport: httpguard.Transport()}, Endpoints: social.DefaultEndpoints, Clients: clients},
		Logger:    logger,
	})
	if err != nil {
		return nil, err
	}
	deps := authapi.CompatDeps{
		Sessions: sessions, Users: authapi.PGUsers{Pool: pool}, Issuer: issuer, Logger: logger,
		Projector: identities, Social: socialService, ServiceCredentials: servicecred.PG{Pool: pool},
		AppendAudit: func(ctx context.Context, event audit.AuditEvent) error {
			return audit.Append(ctx, pool, schema, event)
		},
	}
	if cfg.Session.LoginEnabled {
		deps.Login = &authapi.LoginConfig{
			Users:      local.IdentityUsers{Store: identities},
			Attempts:   local.NewPGAttempts(pool, schema),
			LockoutKey: contracts.NewSecretString(cfg.Session.LockoutKey.Reveal()),
		}
	}
	routes, err := authapi.CompatRoutes(deps)
	if err != nil {
		return nil, err
	}
	sso, err := ssoRoutes(cfg, logger, pool, schema, identities, deps)
	if err != nil {
		return nil, err
	}
	return append(routes, sso...), nil
}

// Main runs the command and exits with its status.
func Main() {
	os.Exit(Execute(context.Background(), os.Args[1:], envsecrets.ProcessLookup, IO{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}))
}

// Execute is the testable process entry point. It never calls os.Exit.
func Execute(parent context.Context, args []string, lookup secrets.LookupEnv, streams IO) int {
	if streams.Stdout == nil {
		streams.Stdout = io.Discard
	}
	if streams.Stderr == nil {
		streams.Stderr = io.Discard
	}

	overrides, showVersion, healthcheck, code, done := parseArguments(args, streams)
	if done {
		return code
	}

	build := version.Current(authconfig.Service)
	if showVersion {
		if err := build.WriteJSON(streams.Stdout); err != nil {
			fmt.Fprintln(streams.Stderr, "could not write version metadata")
			return 1
		}
		return 0
	}

	cfg, err := authconfig.Load(authconfig.Spec{Overrides: overrides, LookupEnv: lookup})
	if err == nil && healthcheck {
		return runHealthcheck(parent, cfg, streams)
	}
	if err != nil {
		// A configuration error can quote an operator-supplied value, so it
		// goes through the same redactor the logger uses before reaching a
		// stream that may be captured into a transcript.
		writeConfigurationError(streams.Stderr, err)
		return 1
	}

	logger := logging.NewJSON(streams.Stdout, cfg.LogLevel)
	defer logging.InstallDefault(logger)()
	if len(cfg.EnvOnlySettings) > 0 {
		logger.Warn(
			"configuration supplied through environment variables",
			"settings", strings.Join(cfg.EnvOnlySettings, ","),
			"guidance", "pass these as flags in the deployed command so the configuration is visible where it is deployed",
		)
	}

	ctx, stop := lifecycle.SignalContext(parent)
	defer stop()

	components, err := configure(ctx, cfg, logger)
	if err != nil {
		logger.ErrorContext(
			ctx, "configure runtime dependencies",
			"error_category", "dependency_configuration_failed",
			"reason", reasonOf(err),
		)
		return 1
	}

	runtime, err := lifecycle.New(lifecycle.Options{
		Logger:          logger,
		ShutdownTimeout: cfg.ShutdownTimeout,
		Components:      components,
	})
	if err != nil {
		logger.ErrorContext(ctx, "construct runtime", "error", err)
		return 1
	}

	logger.LogAttrs(
		ctx, slog.LevelInfo, "service starting",
		append(cfg.SafeAttrs(), build.Attrs()...)...,
	)
	if err := runtime.Run(ctx); err != nil {
		logger.ErrorContext(
			context.Background(), "service stopped with error",
			"error_category", "runtime_failure", "error", err,
		)
		return 1
	}
	logger.InfoContext(context.Background(), "service stopped")
	return 0
}

// configure builds every runtime component and registers the readiness checks.
//
// Declaration order is startup order, and lifecycle.Runtime stops components
// in REVERSE, which is what makes this list the whole shutdown contract too:
//
//  1. auth-postgres  -- constructed without I/O so an unreachable database
//     leaves the process running and /readyz failing, rather than
//     crash-looping. Closed last, after nothing can still be using it.
//     auth-outbox-loop -- publishes and reaps the outbox through the store's
//     pool; stopped before the pool closes.
//  2. operator-http  -- /healthz, /readyz, /metrics. Second so the operator
//     surface answers while later components are still starting, and stops
//     only after the API has drained.
//  3. auth-api-http  -- the API listener.
//  4. readiness-gate -- opens admission only once every component above has
//     started, and closes it FIRST on the way down so a load balancer sees
//     not-ready before the API stops accepting.
func configure(
	ctx context.Context, cfg authconfig.Config, logger *slog.Logger,
) ([]lifecycle.Component, error) {
	return configureWithRoutes(ctx, cfg, logger, nil)
}

// configureWithRoutes is configure with extra API routes mounted beside the
// service's own, so the production-stack test drives the handler configure
// really builds.
func configureWithRoutes(
	ctx context.Context, cfg authconfig.Config, logger *slog.Logger, extra []httpapi.Route,
) ([]lifecycle.Component, error) {
	store, err := authstore.Open(ctx, authstore.Config{
		URI:            cfg.DatabaseURI.Reveal(),
		Schema:         cfg.DatabaseSchema,
		MaxConns:       cfg.DatabaseMaxConns,
		ConnectTimeout: cfg.DatabaseConnectTimeout,
	})
	if err != nil {
		return nil, err
	}

	// Every failure path below must close the pool it already opened.
	// configure's caller exits the process, so a leaked pool would not
	// out-live this call for long -- but a resource opened and abandoned on an
	// error path is the shape that survives into the first caller that DOESN'T
	// exit (a test, or a future host that runs this as a subcommand), and it is
	// cheaper to get right now than to find later.
	// TestConfigureClosesThePoolWhenALaterStepFails is what keeps it true.
	//
	// The context carries no deadline, deliberately, and this comment says so
	// rather than implying a bound that does not exist. Postgres.Shutdown ends
	// in pgxpool.Close, which takes no context: it returns once every acquired
	// connection is released, and cannot be cut short by one.
	//
	// It cannot block here, and the REASON is worth naming, because it is a
	// property of this function rather than of the pool:
	// health.Registry.RegisterRequired only STORES its callback -- it never
	// invokes it -- so store.Probe, and therefore pool.Ping, has not run by the
	// time any of the four closeStore calls below can fire. The pool has
	// acquired no connection and has no dial in flight, so Close returns
	// immediately. A future change that eagerly probes a dependency inside
	// configure would break that property, and this paragraph is the thing it
	// should trip over.
	//
	// An earlier revision wrapped this in a five-second timeout, which read as
	// a guarantee while enforcing nothing (team-lead ruling, 2026-09-02, after
	// lane-auth-cp's executed check: correct the comment, do not build a bound
	// for a call that cannot hang). WithoutCancel is kept so an
	// already-cancelled parent cannot skip the close if a future adapter does
	// start honouring the context.
	closeStore := func() {
		if closeErr := store.Shutdown(context.WithoutCancel(ctx)); closeErr != nil {
			logger.ErrorContext(ctx, "close auth store after a failed startup", "error", closeErr)
		}
	}

	// The token signer is built here and proved by readiness; nothing issues
	// with it yet.
	signer := token.NewFileSigner(token.FileConfig{
		KeySetPath: cfg.SigningKeySetPath,
		KeyPath:    cfg.SigningKeyPath,
		KeyID:      cfg.SigningKeyID,
	})

	registry := health.NewRegistry(cfg.HealthCheckTimeout)
	// Both checks are REQUIRED, so readiness fails closed on either. The
	// registry bounds each one with cfg.HealthCheckTimeout, recovers a panic
	// inside a check as a failure, and returns the failing check NAMES to the
	// HTTP surface -- never the underlying error text, which for these two
	// would carry a DSN and a filesystem path respectively.
	//
	// The error is logged here, unredacted-by-category, so an operator can
	// diagnose without shell access while the response stays detail-free:
	// the same split cmd/query-api landed for CHAOS-4724.
	if err := registry.RegisterRequired(CheckPostgres, func(ctx context.Context) error {
		if probeErr := store.Probe(ctx); probeErr != nil {
			logger.ErrorContext(
				ctx, "readiness check failed",
				"check", CheckPostgres, "reason", authstore.ReasonOf(probeErr),
			)
			return probeErr
		}
		return nil
	}); err != nil {
		closeStore()
		return nil, err
	}
	if err := registry.RegisterRequired(CheckSigningKey, func(ctx context.Context) error {
		if checkErr := signer.Check(ctx); checkErr != nil {
			logger.ErrorContext(
				ctx, "readiness check failed",
				"check", CheckSigningKey, "reason", token.ReasonOf(checkErr),
			)
			return checkErr
		}
		return nil
	}); err != nil {
		closeStore()
		return nil, err
	}

	// The shared httpapi server records its request counter and duration
	// histogram through OTel; without this source they never reach /metrics.
	// Fails soft: a broken exporter leaves them unexported, never stops boot.
	if err := apimetrics.Register(registry); err != nil {
		logger.Warn("register OTel instruments on the operator /metrics", "error", err)
	}

	operatorHTTP, err := health.NewServer(health.ServerOptions{
		Address:  cfg.OperatorAddress,
		Registry: registry,
		Service:  cfg.Service,
		Version:  version.Current(cfg.Service).Version,
	})
	if err != nil {
		closeStore()
		return nil, err
	}

	schemaID, err := authschema.NewValidatedIdentifier(cfg.DatabaseSchema)
	if err != nil {
		closeStore()
		return nil, err
	}
	routes, err := sessionRoutes(cfg, logger, store.Pool(), schemaID)
	if err != nil {
		closeStore()
		return nil, err
	}
	routes = append(routes, authapi.WellKnownRoutes(authapi.WellKnownDeps{
		Keys: signer, IssuerURL: cfg.TokenIssuerURL, Logger: logger,
	})...)
	logger.InfoContext(ctx, "auth API routes", "api_routes_mounted", len(routes) > 0, "routes", len(routes)+len(extra),
		"discovery_mounted", cfg.TokenIssuerURL != "")
	routes = append(routes, extra...)

	apiHTTP, err := httpapi.NewServer(apiServerOptions(cfg, logger, routes))
	if err != nil {
		closeStore()
		return nil, err
	}

	// No consumer of auth outbox events exists yet, so the loop is built with no
	// sink: publishing stays off (a sink-less publisher would mark events
	// delivered that nobody received) and the reaper runs. A sink is injected
	// here when the first consumer lands.
	loopConfig := audit.DefaultLoopConfig(schemaID, registry)
	loopConfig.Logger = logger
	outboxLoop, err := audit.NewOutboxLoop(store.Pool(), nil, loopConfig)
	if err != nil {
		closeStore()
		return nil, err
	}

	return []lifecycle.Component{
		store,
		outboxLoop,
		announcedListener{listener: operatorHTTP, logger: logger},
		announcedListener{listener: apiHTTP, logger: logger},
		health.Gate{Registry: registry},
	}, nil
}

// listener is the surface a socket-binding component exposes: its lifecycle,
// the errors it can raise after a successful bind, and the address it bound.
type listener interface {
	lifecycle.Component
	lifecycle.ErrorSource
	Address() string
}

// announcedListener logs the address a listener actually bound once Start
// returns. With ":0" configured the operating system picks the port, and this
// log line is the only place the choice is visible -- a harness that wants to
// reach the service without pre-reserving a port reads it from here rather
// than racing another process for an ephemeral port it released.
type announcedListener struct {
	listener listener
	logger   *slog.Logger
}

func (a announcedListener) Name() string { return a.listener.Name() }

func (a announcedListener) Start(ctx context.Context) error {
	if err := a.listener.Start(ctx); err != nil {
		return err
	}
	a.logger.InfoContext(ctx, "listener bound", "component", a.listener.Name(), "address", a.listener.Address())
	return nil
}

func (a announcedListener) Shutdown(ctx context.Context) error { return a.listener.Shutdown(ctx) }

func (a announcedListener) Errors() <-chan error { return a.listener.Errors() }

// reasonOf renders a bounded reason code for a dependency-construction
// failure. It never falls back to err.Error(): those errors can carry a DSN.
func reasonOf(err error) string {
	var coded interface{ DependencyReason() string }
	if errors.As(err, &coded) {
		return coded.DependencyReason()
	}
	return "dependency_configuration_failed"
}

// optionValue backs every declared flag. It stores the RAW string rather than
// parsing here, because the very same value can arrive through the option's
// environment fallback: one parser at one site (authconfig.Load) is what keeps
// a flag and its variable from disagreeing about what "16m" or "20" means.
type optionValue struct {
	raw string
	set bool
}

func (v *optionValue) String() string {
	if v == nil {
		return ""
	}
	return v.raw
}

func (v *optionValue) Set(raw string) error {
	v.raw = raw
	v.set = true
	return nil
}

// parseArguments registers the option registry as flags and returns the
// overrides keyed by the environment variable each flag shadows.
//
// done is true when the process should stop with the returned code: --help
// (0), a parse error (2), or a positional argument (2). An unrecognized flag
// STOPS the process -- the half of the contract an environment variable can
// never offer, since a misspelled variable name is indistinguishable from an
// unset one and stays silently inert.
func parseArguments(args []string, streams IO) (
	overrides map[string]string, showVersion, healthcheck bool, code int, done bool,
) {
	flags := flag.NewFlagSet(authconfig.Service, flag.ContinueOnError)
	flags.SetOutput(streams.Stdout)
	flags.Usage = func() { fmt.Fprint(streams.Stdout, authconfig.HelpText()) }
	showVersionFlag := flags.Bool("version", false, "print build metadata as JSON and exit")
	healthcheckFlag := flags.Bool("healthcheck", false,
		"probe this deployment's own operator /readyz over loopback and exit 0 only when it answers 200")

	bound := make(map[string]*optionValue)
	for _, option := range authconfig.Options() {
		if option.Secret || option.Flag == "" {
			continue
		}
		value := &optionValue{}
		flags.Var(value, option.Flag, option.Usage)
		bound[option.Env] = value
	}

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, false, false, 0, true
		}
		writeArgumentError(streams.Stderr, err)
		fmt.Fprintf(streams.Stderr, "run %s --help for the full option list\n", authconfig.Service)
		return nil, false, false, 2, true
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(streams.Stderr, "argument error: positional arguments are not accepted")
		return nil, false, false, 2, true
	}

	overrides = make(map[string]string)
	for env, value := range bound {
		if value.set {
			overrides[env] = value.raw
		}
	}
	return overrides, *showVersionFlag, *healthcheckFlag, 0, false
}

// writeConfigurationError and writeArgumentError print an error that can quote an operator-supplied value back: through the log
// redactor first, before the stream a deployment captures into logs (CHAOS-7937).
func writeConfigurationError(w io.Writer, err error) {
	fmt.Fprintf(w, "configuration error: %s\n", logging.RedactText(err.Error()))
}

func writeArgumentError(w io.Writer, err error) {
	fmt.Fprintf(w, "argument error: %s\n", logging.RedactText(err.Error()))
}

// apiServerOptions is the ONE place the API listener's middleware stack is
// configured from the resolved configuration. configure builds the production
// server from it and the production-stack integration test (CHAOS-4887, G-69)
// builds its server from it with a probe route, so the test exercises the
// configuration the service really runs, not a hand-assembled router.
func apiServerOptions(cfg authconfig.Config, logger *slog.Logger, routes []httpapi.Route) httpapi.ServerOptions {
	return httpapi.ServerOptions{
		Address:        cfg.APIAddress,
		Logger:         logger,
		Routes:         routes,
		RequestTimeout: cfg.RequestTimeout,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		RateLimit:      cfg.RateLimit.PerSecond,
		RateLimitBurst: cfg.RateLimit.Burst,
		MaxInFlight:    maxInFlightRequests,
		// The server-minted request id (internal/auth/requestid) wraps the
		// whole mux, so the transport's own refusals (404, 405, 413, 429)
		// carry X-Auth-Request-ID as route answers do. The client address
		// (every audit row and session) and the user agent (every session)
		// are bound by internal/auth/clientaddr.
		Middleware: []func(http.Handler) http.Handler{requestid.Middleware, clientaddr.Middleware(cfg.TrustedProxies)},
		// The transport id is always minted here, never a client's
		// X-Request-ID: it reaches every log line and envelope.
		AcceptRequestID: func(string) bool { return false },
		// A GET route does not also answer HEAD: every method the API serves
		// is one its endpoint profile names.
		ExplicitHead: true,
	}
}

// runHealthcheck is the exec-form container probe: the runtime image is
// distroless (no shell, no curl), so Docker's healthcheck must invoke this
// binary. It asks the SAME deployment's operator listener for /readyz over
// loopback and mirrors the answer as its exit code. /readyz already
// aggregates the required checks (postgres, signing_key), so the probe adds no
// failure logic of its own -- a container is healthy exactly when the service
// would be ready.
func runHealthcheck(parent context.Context, cfg authconfig.Config, streams IO) int {
	_, port, err := net.SplitHostPort(cfg.OperatorAddress)
	if err != nil || port == "" {
		fmt.Fprintln(streams.Stderr, "healthcheck: operator address has no port")
		return 1
	}
	ctx, cancel := context.WithTimeout(parent, cfg.HealthCheckTimeout+healthcheckSlack)
	defer cancel()
	request, err := http.NewRequestWithContext(
		ctx, http.MethodGet, "http://"+net.JoinHostPort("127.0.0.1", port)+"/readyz", nil,
	)
	if err != nil {
		fmt.Fprintln(streams.Stderr, "healthcheck: could not build the request")
		return 1
	}
	// No keep-alive: a probe that leaves a pooled or pre-dialled connection behind
	// holds the operator listener's graceful shutdown open for the idle timeout
	// when this runs in-process (the tests), and buys nothing for a one-shot probe.
	client := &http.Client{
		Timeout:   cfg.HealthCheckTimeout + healthcheckSlack,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	response, err := client.Do(request)
	if err != nil {
		fmt.Fprintln(streams.Stderr, "healthcheck: operator listener unreachable")
		return 1
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintf(streams.Stderr, "healthcheck: /readyz answered %d\n", response.StatusCode)
		return 1
	}
	return 0
}

// healthcheckSlack is added to the per-check readiness deadline so the probe
// outlasts a readiness evaluation that runs to its own bound.
const healthcheckSlack = time.Second
