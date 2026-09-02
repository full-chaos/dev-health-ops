// Package authruntime composes the auth-service process.
//
// It exists so that cmd/auth-service/main.go can be a thin entrypoint and
// nothing else. That is the constraint chris ratified with the Wave 1 plan on
// 2026-09-02 -- "eventually all the binaries we have for servicing will likely
// need to be plugins to replace dev-hops" -- expressed structurally: Execute
// takes its arguments, its environment and its streams as parameters, touches
// no package-level state, and returns an exit code instead of calling
// os.Exit. Re-hosting this service as a subcommand of a future unified binary
// is then one call to Execute, not a rewrite.
//
// Nothing here knows what an identity, a session or a token is. It wires
// interfaces: keystore.Source for signing-key custody, authstore.Prober for
// storage, httpapi.Route for transport. The domain packages later waves add
// plug in at this one site.
package authruntime

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/authstore"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/keystore"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
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

// Routes is the service's mounted route set.
//
// It is EMPTY, and that is the whole of CHAOS-4881's dormancy requirement:
// "the service is built dormant, nothing in prod calls it, no production token
// issuance". The transport that would serve routes is fully built and fully
// tested in internal/auth/httpapi; what is withheld is the mounting, at this
// one visible site, so that adding the first real route is a one-line,
// reviewable change rather than an archaeology exercise.
//
// A request to any path therefore receives this service's 404 envelope.
func Routes() []httpapi.Route { return nil }

// Main runs the command and exits with its status.
func Main() {
	os.Exit(Execute(context.Background(), os.Args[1:], os.LookupEnv, IO{
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

	overrides, showVersion, code, done := parseArguments(args, streams)
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
	if err != nil {
		// A configuration error can quote an operator-supplied value, so it
		// goes through the same redactor the logger uses before reaching a
		// stream that may be captured into a transcript.
		fmt.Fprintf(streams.Stderr, "configuration error: %s\n", logging.RedactText(err.Error()))
		return 1
	}

	logger := logging.NewJSON(streams.Stdout, cfg.LogLevel)
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
	store, err := authstore.Open(ctx, authstore.Config{
		URI:            cfg.DatabaseURI.Reveal(),
		Schema:         cfg.DatabaseSchema,
		MaxConns:       cfg.DatabaseMaxConns,
		ConnectTimeout: cfg.DatabaseConnectTimeout,
	})
	if err != nil {
		return nil, err
	}

	var signingKey keystore.Source = keystore.FileSource{
		Path:  cfg.SigningKeyPath,
		KeyID: cfg.SigningKeyID,
	}

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
		return nil, err
	}
	if err := registry.RegisterRequired(CheckSigningKey, func(ctx context.Context) error {
		if _, describeErr := signingKey.Describe(ctx); describeErr != nil {
			logger.ErrorContext(
				ctx, "readiness check failed",
				"check", CheckSigningKey, "reason", keystore.ReasonOf(describeErr),
			)
			return describeErr
		}
		return nil
	}); err != nil {
		return nil, err
	}

	operatorHTTP, err := health.NewServer(health.ServerOptions{
		Address:  cfg.OperatorAddress,
		Registry: registry,
		Service:  cfg.Service,
		Version:  version.Current(cfg.Service).Version,
	})
	if err != nil {
		return nil, err
	}

	apiHTTP, err := httpapi.NewServer(httpapi.ServerOptions{
		Address:        cfg.APIAddress,
		Logger:         logger,
		Routes:         Routes(),
		RequestTimeout: cfg.RequestTimeout,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		RateLimit:      cfg.RateLimit.PerSecond,
		RateLimitBurst: cfg.RateLimit.Burst,
	})
	if err != nil {
		return nil, err
	}

	return []lifecycle.Component{
		store,
		operatorHTTP,
		apiHTTP,
		health.Gate{Registry: registry},
	}, nil
}

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
	overrides map[string]string, showVersion bool, code int, done bool,
) {
	flags := flag.NewFlagSet(authconfig.Service, flag.ContinueOnError)
	flags.SetOutput(streams.Stdout)
	flags.Usage = func() { fmt.Fprint(streams.Stdout, authconfig.HelpText()) }
	showVersionFlag := flags.Bool("version", false, "print build metadata as JSON and exit")

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
			return nil, false, 0, true
		}
		fmt.Fprintf(streams.Stderr, "argument error: %s\n", logging.RedactText(err.Error()))
		fmt.Fprintf(streams.Stderr, "run %s --help for the full option list\n", authconfig.Service)
		return nil, false, 2, true
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(streams.Stderr, "argument error: positional arguments are not accepted")
		return nil, false, 2, true
	}

	overrides = make(map[string]string)
	for env, value := range bound {
		if value.set {
			overrides[env] = value.raw
		}
	}
	return overrides, *showVersionFlag, 0, false
}
