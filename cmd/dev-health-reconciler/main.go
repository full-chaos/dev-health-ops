package main

import (
	"context"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

var reconcilerSpec = shell.Spec{
	Service:                         "dev-health-reconciler",
	ConfigureDependenciesWithLogger: configureReconcilerDependenciesWithLogger,
}

func main() {
	shell.Main(reconcilerSpec)
}

func configureReconcilerDependenciesWithLogger(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
) ([]lifecycle.Component, error) {
	return configureReconcilerDependenciesWithSourcesAndLogger(
		ctx,
		cfg,
		registry,
		logger,
		productionReconcilerDependencySources,
	)
}
