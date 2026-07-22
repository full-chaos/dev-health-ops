package main

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
	"github.com/full-chaos/dev-health-ops/internal/processreadiness"
)

var reconcilerSpec = shell.Spec{
	Service:               "dev-health-reconciler",
	ConfigureDependencies: configureReconcilerDependencies,
}

func main() {
	shell.Main(reconcilerSpec)
}

func configureReconcilerDependencies(
	_ context.Context,
	_ config.Config,
	registry *health.Registry,
) ([]lifecycle.Component, error) {
	// Phase 1 has no reconciler loop or composed storage clients. Keep every
	// dependency required by the control-process topology explicitly closed.
	err := processreadiness.RegisterUnavailable(
		registry,
		"domain_postgres",
		"queue_postgres",
		"reconciler_loop",
		"river_schema",
	)
	return nil, err
}
