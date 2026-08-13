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
// environment setting.
//
// CHAOS-3128: this is now schedulersync.TransferScheduleMarkerOwnershipToGo(),
// the reviewed, in-source ownership transfer -- see that function's doc
// comment for what calling it does and does not activate, and for why a
// concurrently running Celery Beat cannot also mutate a marker once this
// policy is in effect. productionSchedulerRuntimeSources.newRepository below
// is what actually wires this value into the repository construction Go
// uses, so this variable is the single source of truth for which ownership
// policy the binary runs with, not a decorative check.
//
// The checked-in activation below now makes this ownership effective. The
// deployment contract must still provide the separate domain, queue, and
// coordinator PostgreSQL connections, and must not run Celery Beat against the
// same marker tables.
//
// CHAOS-3114 repointed every database call site in dependencies.go onto the
// coordinator pool: first the sync-path repository and occurrence reconciler
// (schedulersync.NewMutationRepository/NewOccurrenceCoordinator/
// NewOccurrenceReconciler's SQL -- scheduled_jobs, sync_configurations
// UPDATE, scheduled_sync_occurrences), then the fixed maintenance engine,
// whose runOccurrence transaction is now covered end to end by
// coordinatorPosture (internal/storage/postgres/domain_authorization.go).
// The materializer now uses both role-specific pools: coordinator policy and
// ledger work stays on the coordinator pool while sync_runs, sync_run_units,
// and FK-dependent provider inventory repair commit on the domain pool.
var schedulerOwnership = schedulersync.TransferScheduleMarkerOwnershipToGo()

var errSchedulerActivationUnavailable = errors.New("scheduler activation is unavailable")

// schedulerActivation is a source-reviewed, package-private composition seam.
// It deliberately cannot be influenced by process environment or deployment
// profile. The production loop factory is reached only through the checked-in
// ownership decision below.
//
// coordinatorPolicyParity was removed deliberately (program-owner decision,
// CHAOS-3114) rather than left at false: it was a bare bool with a prose
// comment and no test, checklist, or document anywhere in this repository
// defining what "the coordinator behaves equivalently to the Python one"
// means or how it would be proven. A gate whose precondition is undefined
// cannot honestly gate anything -- it only blocks indefinitely while
// implying a rigour that does not exist. Do not re-add a field like this as
// a placeholder; if a coordinator-parity gate is ever needed again, it must
// ship together with the test/checklist/document that defines what it
// proves.
//
// CHAOS-3128 transferred marker-mutation ownership itself (schedulerOwnership
// above). The materializer and fixed-schedule producers now satisfy the source
// precondition for activation; the deployment owner separately supplies the
// required role-specific connections and removes the competing Beat process.
type schedulerActivation struct {
	goOwnsMarkers bool
}

var checkedInSchedulerActivation = schedulerActivation{goOwnsMarkers: true}

type schedulerDependencySources struct {
	buildLoop func(context.Context, config.Config, *health.Registry) (lifecycle.Component, error)
}

var productionSchedulerDependencySources = schedulerDependencySources{
	buildLoop: buildProductionSchedulerLoop,
}

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
	if !activation.goOwnsMarkers {
		// schedulerOwnership alone (CHAOS-3128) is not activation: until this
		// gate is true this process must not even open a PostgreSQL client.
		// Keep all externally visible readiness names closed.
		return nil, processreadiness.RegisterUnavailable(
			registry,
			"domain_postgres",
			"queue_postgres",
			"coordinator_postgres",
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
