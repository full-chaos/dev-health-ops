// Package shell composes the common executable runtime without owning any job
// or storage behavior.
package shell

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// ConfigureDependencies keeps command construction injectable. Storage
// adapters register their required Ping checks on Registry and return lifecycle
// components in startup order.
type ConfigureDependencies func(
	context.Context,
	config.Config,
	*health.Registry,
) ([]lifecycle.Component, error)

// ConfigureDependenciesWithLogger is the optional logger-aware variant of
// ConfigureDependencies. The logger is the shell-owned JSON logger for this
// process; commands must not replace it with a separate handler or sink.
type ConfigureDependenciesWithLogger func(
	context.Context,
	config.Config,
	*health.Registry,
	*slog.Logger,
) ([]lifecycle.Component, error)

type Spec struct {
	Service                         string
	Profiles                        []string
	DefaultProfile                  string
	RequireQueues                   bool
	ConfigureDependencies           ConfigureDependencies
	ConfigureDependenciesWithLogger ConfigureDependenciesWithLogger
}

type IO struct {
	Stdout io.Writer
	Stderr io.Writer
}

// optionValue is the flag.Value backing every declared option.
//
// It deliberately stores the raw string rather than parsing here: the very same
// value can arrive through the option's environment fallback, and one parser at
// one site (config.Load) is what keeps a flag and its variable from disagreeing
// about what "16m" or "true" means. Parse errors surface from there naming both
// surfaces.
type optionValue struct {
	kind       config.Kind
	repeatable bool
	raw        string
	set        bool
}

func (value *optionValue) String() string {
	if value == nil {
		return ""
	}
	return value.raw
}

func (value *optionValue) Set(raw string) error {
	if value.repeatable && value.set {
		value.raw += "," + raw
	} else {
		value.raw = raw
	}
	value.set = true
	return nil
}

// IsBoolFlag lets the flag package accept a bare --flag for boolean options
// while still honoring the explicit --flag=false form.
func (value *optionValue) IsBoolFlag() bool { return value.kind == config.KindBool }

// registerOptions binds every option this service offers onto flags and returns
// the bound values keyed by canonical flag name. A short alias is registered
// against the same value, so -q and --queues are one setting rather than two
// that could disagree.
func registerOptions(flags *flag.FlagSet, spec Spec) map[string]*optionValue {
	bound := make(map[string]*optionValue)
	for _, option := range config.OptionsFor(spec.Service, spec.RequireQueues) {
		if option.Secret {
			continue
		}
		// Profiles are declared by the Spec, not by the registry's service
		// list; a binary that declares none must not advertise the flag.
		if option.Flag == "profile" && len(spec.Profiles) == 0 {
			continue
		}
		value := &optionValue{
			kind: option.Kind,
			// Queue topology is the one place operators habitually repeat a
			// flag instead of writing one comma-separated value.
			repeatable: option.Flag == "queues" || option.Flag == "concurrency",
		}
		flags.Var(value, option.Flag, option.Usage)
		if option.Short != "" {
			flags.Var(value, option.Short, option.Usage)
		}
		for _, alias := range option.Aliases {
			flags.Var(value, alias, option.Usage)
		}
		bound[option.Flag] = value
	}
	return bound
}

// overridesFrom collects the options the operator actually supplied, keyed by
// the environment variable each one shadows. Only options that were set appear,
// which is what makes the environment a fallback rather than a competitor.
//
// A flag whose value is blank ("--shutdown-timeout=") counts as NOT supplied
// and is omitted, so the environment still resolves beneath it. Every
// resolution site in config treats a blank value as unset -- envOrDefault,
// durationEnv, boolEnv, and the durationArgumentOrEnv this replaced all trim
// and fall through -- so an empty flag that shadowed a real environment value
// would be the one place in the surface where blank meant "override with
// nothing". That would silently drop a deployment's shutdown budget to the
// package default and flip ShutdownTimeoutExplicit, which feeds the worker's
// drain-budget decision.
func overridesFrom(bound map[string]*optionValue) map[string]string {
	overrides := make(map[string]string)
	for _, option := range config.Options() {
		value, registered := bound[option.Flag]
		if !registered || !value.set || option.Env == "" {
			continue
		}
		if strings.TrimSpace(value.raw) == "" {
			continue
		}
		overrides[option.Env] = value.raw
	}
	return overrides
}

// resolveProfile owns runtime-profile selection end to end (CHAOS-3875). Only
// dev-health-stream-runner declares profiles, so the platform config package
// no longer carries a Profiles/DefaultProfile pair threaded through a second
// package just to be validated there: the flag, the DEV_HEALTH_PROFILE
// fallback, the default, and the membership check all live at this one site,
// and config.Spec receives an already-resolved value.
func resolveProfile(
	spec Spec,
	selected *string,
	lookup secrets.LookupEnv,
) (string, error) {
	chosen := ""
	if selected != nil {
		chosen = strings.TrimSpace(*selected)
	}
	if len(spec.Profiles) == 0 {
		if chosen != "" {
			return "", fmt.Errorf("%s does not accept a profile", spec.Service)
		}
		return "", nil
	}
	if chosen == "" {
		if value, ok := lookup("DEV_HEALTH_PROFILE"); ok {
			chosen = strings.TrimSpace(value)
		}
	}
	if chosen == "" {
		chosen = spec.DefaultProfile
	}
	if !slices.Contains(spec.Profiles, chosen) {
		return "", fmt.Errorf("profile must be one of %s", strings.Join(spec.Profiles, ", "))
	}
	return chosen, nil
}

// warnEnvOnlySettings reports configuration that arrived through an
// environment variable although a canonical flag exists for it.
//
// The environment fallback is retained for 12-factor compatibility and to make
// the migration incremental, but a deployment still configured that way is not
// visible in `docker compose config`, so it is worth one warning per start
// rather than silence.
func warnEnvOnlySettings(logger *slog.Logger, cfg config.Config) {
	if len(cfg.EnvOnlySettings) == 0 {
		return
	}
	logger.Warn(
		"configuration supplied through environment variables",
		"settings", strings.Join(cfg.EnvOnlySettings, ","),
		"guidance", "pass these as flags in the deployed command so the configuration is visible where it is deployed",
	)
}

// Main runs a production command and exits with its status.
func Main(spec Spec) {
	os.Exit(Execute(context.Background(), spec, os.Args[1:], os.LookupEnv, IO{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}))
}

// Execute is the testable command entry point.
// dependencyReason is implemented by dependency-construction errors that carry
// a bounded, non-sensitive reason code identifying which construction site
// failed. Adapters that do not implement it keep the previous opaque logging.
type dependencyReason interface {
	DependencyReason() string
}

func Execute(
	parent context.Context,
	spec Spec,
	args []string,
	lookup secrets.LookupEnv,
	streams IO,
) int {
	if streams.Stdout == nil {
		streams.Stdout = io.Discard
	}
	if streams.Stderr == nil {
		streams.Stderr = io.Discard
	}

	flags := flag.NewFlagSet(spec.Service, flag.ContinueOnError)
	flags.SetOutput(streams.Stdout)
	// --help is the single discovery surface for this binary's configuration
	// (CHAOS-4020), so it is rendered from the option registry rather than from
	// the flag package's own defaults listing: the registry is what carries the
	// environment fallback names, the documented defaults, and the route
	// vocabulary.
	flags.Usage = func() {
		fmt.Fprint(streams.Stdout, config.HelpText(spec.Service, spec.RequireQueues))
	}
	showVersion := flags.Bool("version", false, "print build metadata as JSON and exit")
	bound := registerOptions(flags, spec)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		// An unrecognized flag lands here and stops the process. This is the
		// half of the contract an environment variable could never offer: a
		// misspelled variable name is indistinguishable from an unset one and
		// stays silently inert, which is how OTEL_SERVICE_NAMEi survived in
		// production.
		fmt.Fprintf(streams.Stderr, "argument error: %s\n", logging.RedactText(err.Error()))
		fmt.Fprintf(streams.Stderr, "run %s --help for the full option list\n", spec.Service)
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(streams.Stderr, "argument error: positional arguments are not accepted")
		return 2
	}
	var selectedProfile *string
	if profile, registered := bound["profile"]; registered {
		selectedProfile = &profile.raw
	}

	build := version.Current(spec.Service)
	if *showVersion {
		if err := build.WriteJSON(streams.Stdout); err != nil {
			fmt.Fprintln(streams.Stderr, "could not write version metadata")
			return 1
		}
		return 0
	}

	profile, err := resolveProfile(spec, selectedProfile, lookup)
	if err != nil {
		fmt.Fprintf(streams.Stderr, "configuration error: %s\n", logging.RedactText(err.Error()))
		return 1
	}
	loadSpec := config.Spec{
		Service:       spec.Service,
		Profile:       profile,
		RequireQueues: spec.RequireQueues,
		Overrides:     overridesFrom(bound),
		LookupEnv:     lookup,
	}
	if queues, registered := bound["queues"]; registered && queues.set {
		loadSpec.Queues = []string{queues.raw}
	}
	cfg, err := config.Load(loadSpec)
	if err != nil {
		fmt.Fprintf(streams.Stderr, "configuration error: %s\n", logging.RedactText(err.Error()))
		return 1
	}

	logger := logging.NewJSON(streams.Stdout, cfg.LogLevel)
	warnEnvOnlySettings(logger, cfg)
	tracingComponent := tracing.Init(logger)
	registry := health.NewRegistry(cfg.HealthCheckTimeout)
	operatorHTTP, err := health.NewServer(health.ServerOptions{
		Address:  cfg.HTTPAddress,
		Registry: registry,
		Service:  cfg.Service,
		Version:  build.Version,
	})
	if err != nil {
		logger.Error("construct operator HTTP", "error", err)
		return 1
	}

	ctx, stop := lifecycle.SignalContext(parent)
	defer stop()
	// tracingComponent starts first and, by lifecycle.Runtime's reverse
	// shutdown order, stops last -- so buffered spans from every other
	// component's work flush before the exporter shuts down.
	components := []lifecycle.Component{tracingComponent, operatorHTTP}
	if spec.ConfigureDependencies != nil && spec.ConfigureDependenciesWithLogger != nil {
		logger.ErrorContext(
			ctx,
			"configure runtime dependencies",
			"error_category",
			"ambiguous_dependency_configuration",
		)
		return 1
	}
	if spec.ConfigureDependencies != nil || spec.ConfigureDependenciesWithLogger != nil {
		var configured []lifecycle.Component
		var configureErr error
		if spec.ConfigureDependenciesWithLogger != nil {
			configured, configureErr = spec.ConfigureDependenciesWithLogger(
				ctx, cfg, registry, logger,
			)
		} else {
			configured, configureErr = spec.ConfigureDependencies(ctx, cfg, registry)
		}
		if configureErr != nil {
			// Dependency adapters return operational detail to their caller, but the
			// shell never assumes an arbitrary error is free of DSNs or secrets.
			// A reason code, when the adapter supplies one, is a bounded
			// compile-time constant naming the failing construction site, so it
			// can be logged without redaction (CHAOS-3873).
			attributes := []any{"error_category", "dependency_configuration_failed"}
			var coded dependencyReason
			if errors.As(configureErr, &coded) {
				attributes = append(attributes, "reason", coded.DependencyReason())
			}
			logger.ErrorContext(ctx, "configure runtime dependencies", attributes...)
			return 1
		}
		components = append(components, configured...)
	}
	components = append(components, health.Gate{Registry: registry})

	runtime, err := lifecycle.New(lifecycle.Options{
		Logger:          logger,
		ShutdownTimeout: cfg.ShutdownTimeout,
		Components:      components,
	})
	if err != nil {
		logger.ErrorContext(ctx, "construct runtime", "error", err)
		return 1
	}

	attrs := append(cfg.SafeAttrs(), build.Attrs()...)
	logger.LogAttrs(ctx, slog.LevelInfo, "service starting", attrs...)
	if err := runtime.Run(ctx); err != nil {
		// Mirrors the configure path above: a reason code, when a component
		// supplies one, is a bounded compile-time constant safe to log
		// as-is, and the error itself is attached as a normal attribute
		// value (not pre-formatted into the message) so the logging
		// handler's redacting ReplaceAttr still processes it (CHAOS-3873).
		attributes := []any{"error_category", "runtime_failure", "error", err}
		var coded dependencyReason
		if errors.As(err, &coded) {
			attributes = append(attributes, "reason", coded.DependencyReason())
		}
		logger.ErrorContext(context.Background(), "service stopped with error", attributes...)
		return 1
	}
	logger.InfoContext(context.Background(), "service stopped")
	return 0
}
