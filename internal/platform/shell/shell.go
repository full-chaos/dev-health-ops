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

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
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
	ConfigureDependencies           ConfigureDependencies
	ConfigureDependenciesWithLogger ConfigureDependenciesWithLogger
}

type IO struct {
	Stdout io.Writer
	Stderr io.Writer
}

// Main runs a production command and exits with its status.
func Main(spec Spec) {
	os.Exit(Execute(context.Background(), spec, os.Args[1:], os.LookupEnv, IO{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}))
}

// Execute is the testable command entry point.
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
	if len(spec.Profiles) > 0 {
		selectedProfile = flags.String("profile", "", "runtime profile")
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

	profile := ""
	if selectedProfile != nil {
		profile = *selectedProfile
	}
	cfg, err := config.Load(config.Spec{
		Service:        spec.Service,
		Profiles:       spec.Profiles,
		DefaultProfile: spec.DefaultProfile,
		Profile:        profile,
		LookupEnv:      lookup,
	})
	if err != nil {
		fmt.Fprintf(streams.Stderr, "configuration error: %s\n", logging.RedactText(err.Error()))
		return 1
	}

	logger := logging.NewJSON(streams.Stdout, cfg.LogLevel)
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
	components := []lifecycle.Component{operatorHTTP}
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
			logger.ErrorContext(
				ctx,
				"configure runtime dependencies",
				"error_category",
				"dependency_configuration_failed",
			)
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
		logger.ErrorContext(
			context.Background(),
			"service stopped with error",
			"error_category",
			"runtime_failure",
		)
		return 1
	}
	logger.InfoContext(context.Background(), "service stopped")
	return 0
}
