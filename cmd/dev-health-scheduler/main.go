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
// Obtaining this policy does NOT make the binary write anything: it still
// requires checkedInSchedulerActivation's goOwnsMarkers and
// coordinatorPolicyParity to both be true before this process opens a
// database pool at all (see below), and at minimum three further
// preconditions this change does not attempt to satisfy:
//
//  1. coordinatorPolicyParity has no defined bar anywhere in this repository
//     today -- no test, doc, or checklist states what "the coordinator
//     behaves equivalently to the Python one" means well enough to prove.
//     Setting it true without that definition would defeat the purpose of a
//     source-reviewed gate.
//  2. This binary has never been given a coordinator PostgreSQL pool (see
//     .github/docs-legacy/architecture/chaos-3033-coordinator-pool-activation.md,
//     "scheduler | not repointed -- blocked on CHAOS-3114"), and
//     schedulersync.NewMutationRepository/NewOccurrenceCoordinator/
//     NewOccurrenceReconciler's SQL (scheduled_jobs, sync_configurations
//     UPDATE, scheduled_sync_occurrences) is coordinator-exclusive per
//     internal/storage/postgres/domain_authorization.go's coordinatorPosture.
//     dependencies.go still hands them database.DomainPool(); flipping
//     activation without repointing that call site would 42501 on the first
//     real handoff.
//  3. sources.newOccurrences below still composes
//     schedulersync.NewUnavailableMaterializer(): the native sync planner is
//     CUT-09/CUT-10 work, not this change. Activating today would durably
//     record occurrences Go can never materialize.
var schedulerOwnership = schedulersync.TransferScheduleMarkerOwnershipToGo()

var errSchedulerActivationUnavailable = errors.New("scheduler activation is unavailable")

// schedulerActivation is a source-reviewed, package-private composition seam.
// It deliberately cannot be influenced by process environment or deployment
// profile. The production loop factory is retained but remains unreachable
// until a future change proves coordinator policy parity and sets both gates
// true.
//
// CHAOS-3128 transferred marker-mutation ownership itself (schedulerOwnership
// above) but deliberately leaves both gates false: coordinatorPolicyParity
// has no defined bar to prove yet, this binary's sync-path repository and
// occurrence constructors still run on the domain pool rather than the
// coordinator pool their SQL requires, and the occurrence materializer is
// still the CUT-09/CUT-10 stub. See schedulerOwnership's doc comment for the
// full list. Flipping goOwnsMarkers to true without also resolving those
// would ship a database permission failure or a stranded-occurrence backlog
// on the first real due schedule, not a working scheduler.
type schedulerActivation struct {
	goOwnsMarkers           bool
	coordinatorPolicyParity bool
}

var checkedInSchedulerActivation = schedulerActivation{}

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
	if !activation.goOwnsMarkers || !activation.coordinatorPolicyParity {
		// schedulerOwnership alone (CHAOS-3128) is not activation: until both
		// gates are true this process must not even open a PostgreSQL client.
		// Keep all externally visible readiness names closed.
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
