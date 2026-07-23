package main

import (
	"context"

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

func main() {
	shell.Main(schedulerSpec)
}

func configureSchedulerDependencies(
	_ context.Context,
	_ config.Config,
	registry *health.Registry,
) ([]lifecycle.Component, error) {
	if err := schedulerOwnership.Validate(); err != nil {
		return nil, err
	}
	// Phase 1 has no scheduler loop or composed storage clients. Keep every
	// dependency required by the control-process topology explicitly closed.
	err := processreadiness.RegisterUnavailable(
		registry,
		"domain_postgres",
		"queue_postgres",
		"river_schema",
		"scheduler_loop",
	)
	return nil, err
}
