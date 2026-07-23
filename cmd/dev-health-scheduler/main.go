package main

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
	"github.com/full-chaos/dev-health-ops/internal/processreadiness"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

var schedulerSpec = shell.Spec{
	Service:               "dev-health-scheduler",
	ConfigureDependencies: configureSchedulerDependencies,
}

// schedulerOwnership is intentionally fixed in the binary. Do not make it an
// environment setting: deployment_state remains coexistence_disabled and the
// Python scheduler retains all production marker-mutation ownership.
var schedulerOwnership = schedulersync.DefaultOwnershipPolicy()

var errSchedulerActivationUnavailable = errors.New("scheduler activation is unavailable")

// schedulerActivation is a source-reviewed, package-private composition seam.
// It deliberately cannot be influenced by process environment or deployment
// profile. A future owner-transfer change must prove coordinator policy parity
// before it supplies a loop factory and sets both gates true.
type schedulerActivation struct {
	goOwnsMarkers           bool
	coordinatorPolicyParity bool
}

var checkedInSchedulerActivation = schedulerActivation{}

type schedulerDependencySources struct {
	buildLoop func(context.Context, config.Config, *health.Registry) (lifecycle.Component, error)
}

var productionSchedulerDependencySources = schedulerDependencySources{}

func main() {
	shell.Main(schedulerSpec)
}

func configureSchedulerDependencies(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
) ([]lifecycle.Component, error) {
	return configureSchedulerDependenciesWithSources(
		ctx,
		cfg,
		registry,
		checkedInSchedulerActivation,
		productionSchedulerDependencySources,
	)
}

func configureSchedulerDependenciesWithSources(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	activation schedulerActivation,
	sources schedulerDependencySources,
) ([]lifecycle.Component, error) {
	if err := schedulerOwnership.Validate(); err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, errSchedulerActivationUnavailable
	}
	if !activation.goOwnsMarkers || !activation.coordinatorPolicyParity {
		// The checked-in Celery/coexistence_disabled policy must not even open a
		// PostgreSQL client. Keep all externally visible readiness names closed.
		return nil, processreadiness.RegisterUnavailable(
			registry,
			"domain_postgres",
			"queue_postgres",
			"river_schema",
			"scheduler_loop",
		)
	}
	if sources.buildLoop == nil {
		return nil, errSchedulerActivationUnavailable
	}
	loop, err := sources.buildLoop(ctx, cfg, registry)
	if err != nil || loop == nil {
		return nil, errSchedulerActivationUnavailable
	}
	return []lifecycle.Component{loop}, nil
}
