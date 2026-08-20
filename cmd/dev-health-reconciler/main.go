package main

import (
	"context"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

// CHAOS-4005: the reconciler owns the unreclaimable-dispatching sweep. Its
// --unreclaimable-sweep flag is declared in the platform option registry scoped
// to this service name (CHAOS-4020), so the flag, its --help entry, its
// SYNC_UNRECLAIMABLE_SWEEP fallback, and flag > env precedence all follow from
// one declaration instead of a per-binary opt-in bool.
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
