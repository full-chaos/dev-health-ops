package main

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

var reconcilerSpec = shell.Spec{
	Service:               "dev-health-reconciler",
	ConfigureDependencies: configureReconcilerDependencies,
}

func main() {
	shell.Main(reconcilerSpec)
}

func configureReconcilerDependencies(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
) ([]lifecycle.Component, error) {
	return configureReconcilerDependenciesWithSources(
		ctx,
		cfg,
		registry,
		productionReconcilerDependencySources,
	)
}
