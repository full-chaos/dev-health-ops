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

type repeatedStringFlag []string

func (values *repeatedStringFlag) String() string {
	return fmt.Sprint([]string(*values))
}

func (values *repeatedStringFlag) Set(value string) error {
	*values = append(*values, value)
	return nil
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
	showVersion := flags.Bool("version", false, "print build metadata as JSON and exit")
	var selectedProfile *string
	var selectedQueues repeatedStringFlag
	var queueConcurrency repeatedStringFlag
	var workerGroup, shutdownTimeout string
	if len(spec.Profiles) > 0 {
		selectedProfile = flags.String("profile", "", "runtime profile")
	}
	if spec.RequireQueues {
		flags.Var(&selectedQueues, "queues", "registered queues to consume (comma-separated or repeatable)")
		flags.Var(
			&queueConcurrency,
			"queue-concurrency",
			"queue worker budgets as queue=workers entries (comma-separated or repeatable)",
		)
		flags.StringVar(&workerGroup, "worker-group", "", "stable worker group label for logs and metrics")
		flags.StringVar(&shutdownTimeout, "shutdown-timeout", "", "graceful shutdown timeout")
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(streams.Stderr, "argument error: %s\n", logging.RedactText(err.Error()))
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(streams.Stderr, "argument error: positional arguments are not accepted")
		return 2
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
	cfg, err := config.Load(config.Spec{
		Service:       spec.Service,
		Profile:       profile,
		RequireQueues: spec.RequireQueues,
		Queues:        append([]string(nil), selectedQueues...),
		QueueConcurrency: append(
			[]string(nil), queueConcurrency...,
		),
		WorkerGroup:     workerGroup,
		ShutdownTimeout: shutdownTimeout,
		LookupEnv:       lookup,
	})
	if err != nil {
		fmt.Fprintf(streams.Stderr, "configuration error: %s\n", logging.RedactText(err.Error()))
		return 1
	}

	logger := logging.NewJSON(streams.Stdout, cfg.LogLevel)
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
