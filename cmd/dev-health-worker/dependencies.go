package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

const (
	defaultContractRoot      = "contracts/jobs/v1"
	workerFinalizationBuffer = 60 * time.Second
)

var errWorkerDependencyUnavailable = errors.New("worker readiness dependency is unavailable")

// dependencyFailure attaches a bounded reason code to the generic dependency
// sentinel. Dozens of distinct construction failures -- rescue collisions,
// drain-budget math, a missing ClickHouse URI, handler drift -- used to return
// the bare sentinel, and the shell logged only "dependency_configuration_failed",
// so an operator could not tell which knob was wrong (CHAOS-3873). The reason is
// always a compile-time constant, never interpolated input, so logging it cannot
// leak a DSN or a secret.
type dependencyFailure struct {
	reason string
}

func (failure dependencyFailure) Error() string {
	return errWorkerDependencyUnavailable.Error() + ": " + failure.reason
}

func (dependencyFailure) Unwrap() error { return errWorkerDependencyUnavailable }

// DependencyReason satisfies the shell's reason-code interface.
func (failure dependencyFailure) DependencyReason() string { return failure.reason }

func dependencyUnavailable(reason string) error { return dependencyFailure{reason: reason} }

type workerDatabase interface {
	DomainReady(context.Context) error
	QueueReady(context.Context) error
	RiverSchemaReady(context.Context, string) error
	PoolSaturation() (domain float64, queueControl float64)
	NewQueueTelemetrySampler(riverstore.QueueTelemetryConfig) (queueTelemetrySampler, error)
	// AttachPoolAcquireObserver wires worker_database_pool_acquire_seconds
	// into the domain/queue-control pools' Acquire path. It is called once
	// dependencies.metrics exists, which is after the database itself opens
	// (see NewRuntimePools's tracer freeze-at-construction constraint).
	AttachPoolAcquireObserver(postgres.PoolAcquireObserver)
	// DomainTxOpener exposes the domain pool's Begin capability for the
	// CHAOS-4029 execution-liveness signal (idempotency_backend /
	// execution_liveness). It is the SAME pool internal/jobruntime's
	// PostgresIdempotency.Begin uses for every real job's claim -- see
	// executionLivenessPool's doc comment for why this proves a materially
	// different thing than domain_postgres's role-posture SELECT.
	DomainTxOpener() selfprobe.TxOpener
	Close()
}

type queueTelemetrySampler interface {
	Snapshot(context.Context) (riverstore.QueueTelemetrySnapshot, error)
	CheckAvailableContractVersions(context.Context) error
}

type postgresWorkerDatabase struct {
	pools       *postgres.RuntimePools
	domainRole  string
	queueRole   string
	riverSchema string
}

func (database *postgresWorkerDatabase) NewWorkerPresence(
	workerGroup string,
	queues []string,
	instanceID string,
) (*jobruntime.WorkerPresence, error) {
	if database == nil || database.pools == nil || database.pools.Domain == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return jobruntime.NewWorkerPresence(database.pools.Domain, workerGroup, queues, instanceID)
}

// githubProjectsV2DurableConfigReader is deliberately narrower than
// workerDatabase: Projects v2 configuration is a startup advisory, not a
// per-claim collector dependency. Production obtains it from the domain pool
// only after CheckDomainAuthorization has proven the 0088 snapshot-table
// posture; tests can provide the same small query seam without widening roles.
type githubProjectsV2DurableConfigReader interface {
	GitHubProjectsV2Configured(context.Context) (bool, error)
}

// databaseConfigurationRejected delegates to postgres.ConfigurationRejected,
// which owns the sentinel list it classifies. It stays as a named local so the
// call site below still reads as a policy decision rather than a type check.
func databaseConfigurationRejected(err error) bool {
	return postgres.ConfigurationRejected(err)
}

func openWorkerDatabase(ctx context.Context, cfg config.Config) (workerDatabase, error) {
	runtimeConfig := postgres.RuntimeConfigFromPlatform(cfg)
	pools, err := postgres.NewRuntimePools(ctx, runtimeConfig)
	if err != nil {
		return nil, err
	}
	return &postgresWorkerDatabase{
		pools: pools, domainRole: runtimeConfig.DomainRole, queueRole: runtimeConfig.QueueRole, riverSchema: runtimeConfig.RiverSchema,
	}, nil
}

func (database *postgresWorkerDatabase) DomainReady(ctx context.Context) error {
	if database == nil || database.pools == nil || database.pools.Domain == nil {
		return errWorkerDependencyUnavailable
	}
	return postgres.CheckDomainAuthorization(ctx, database.pools.Domain, database.domainRole, database.riverSchema)
}

// GitHubProjectsV2Configured reports whether any enabled GitHub integration
// has the durable D18 target key. Presence is intentional: an explicit empty
// list is durable configuration (and means no Projects v2 targets), while a
// malformed value is handled later as ErrInvalidConfiguration for its claim.
// This is a startup-only census, never a route/collector environment fallback.
func (database *postgresWorkerDatabase) GitHubProjectsV2Configured(ctx context.Context) (bool, error) {
	if database == nil || database.pools == nil || database.pools.Domain == nil || ctx == nil {
		return false, errWorkerDependencyUnavailable
	}
	const query = `
SELECT EXISTS (
	SELECT 1
	FROM public.integrations
	WHERE lower(provider) = 'github'
		AND is_active
		AND config::jsonb ? 'github_projects_v2'
)`
	var configured bool
	if err := database.pools.Domain.QueryRow(ctx, query).Scan(&configured); err != nil {
		return false, err
	}
	return configured, nil
}

func (database *postgresWorkerDatabase) QueueReady(ctx context.Context) error {
	if database == nil || database.pools == nil || database.pools.QueueControl == nil {
		return errWorkerDependencyUnavailable
	}
	return postgres.CheckQueueAuthorization(ctx, database.pools.QueueControl, database.queueRole, database.riverSchema)
}

func (database *postgresWorkerDatabase) RiverSchemaReady(ctx context.Context, schema string) error {
	if database == nil || database.pools == nil || database.pools.QueueControl == nil {
		return errWorkerDependencyUnavailable
	}
	_, err := riverstore.CheckSchema(ctx, database.pools.QueueControl, schema, nil)
	return err
}

func (database *postgresWorkerDatabase) PoolSaturation() (float64, float64) {
	if database == nil || database.pools == nil {
		return 0, 0
	}
	return poolSaturation(database.pools.Domain), poolSaturation(database.pools.QueueControl)
}

func (database *postgresWorkerDatabase) NewQueueTelemetrySampler(
	config riverstore.QueueTelemetryConfig,
) (queueTelemetrySampler, error) {
	if database == nil || database.pools == nil || database.pools.QueueControl == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return riverstore.NewQueueTelemetrySampler(database.pools.QueueControl, config)
}

func poolSaturation(pool *pgxpool.Pool) float64 {
	if pool == nil {
		return 0
	}
	statistics := pool.Stat()
	if statistics == nil || statistics.MaxConns() <= 0 {
		return 0
	}
	return float64(statistics.AcquiredConns()) / float64(statistics.MaxConns())
}

func (database *postgresWorkerDatabase) AttachPoolAcquireObserver(observer postgres.PoolAcquireObserver) {
	if database == nil || database.pools == nil {
		return
	}
	database.pools.AttachPoolAcquireObserver(observer)
}

// DomainTxOpener implements workerDatabase.DomainTxOpener -- see that
// interface method's doc comment.
//
// executionLivenessPool doc (CHAOS-4029): domain_postgres proves the runtime
// role HOLDS the right grants (a role-posture introspection SELECT); it does
// not prove a transaction can actually be opened right now against this
// pool. On 2026-08-20 the domain pool moved (pgbouncer-1 recreated) 17
// seconds after startup admission, and every job failed at
// PostgresIdempotency.Begin's own pool.Begin(ctx) call -- a failure mode a
// SELECT-shaped check does not reproduce. Wiring selfprobe against this same
// pool exercises the identical primitive real jobs depend on.
func (database *postgresWorkerDatabase) DomainTxOpener() selfprobe.TxOpener {
	if database == nil || database.pools == nil {
		return nil
	}
	return selfprobe.NewPool(database.pools.Domain)
}

func (database *postgresWorkerDatabase) Close() {
	if database != nil && database.pools != nil {
		database.pools.Close()
	}
}

// workerFamily is everything one constructed handler family contributes to the
// process: its lifecycle component, the adapters it concretely registered, and
// the River queue budget it actually consumes. Runtime capability is only ever
// what a builder constructed here — never what a compiled list advertises.
type workerFamily struct {
	handlers []jobruntime.HandlerSpec
	queues   []jobruntime.QueueBudget
	cleanups []func() error
	// ownedKinds names kinds this family registers real workers for WITHOUT
	// reporting them as handler specs -- today only the sync coordinator's
	// four bridge-backed kinds (syncdispatchruntime.RegisterWorkers). Rescue
	// coverage must treat them as owned, so they have to survive composition
	// rather than being consumed by a per-family rescue call.
	ownedKinds []string
	// metricsSource is an additional Prometheus fragment this family owns
	// (e.g. providerfoundation's dev_health_provider_* family), registered
	// with the health.Registry alongside "worker_runtime" once construction
	// finishes. Most families leave this nil.
	metricsSource health.MetricsSource
}

type workerFamilyBuilder func(
	config.Config,
	workerDatabase,
	*jobruntime.Registry,
	jobruntime.Observer,
	*slog.Logger,
	*river.Workers,
) (workerFamily, error)

type workerProcessBuilder func(
	config.Config,
	workerDatabase,
	*river.Workers,
	workerFamily,
	*slog.Logger,
) (lifecycle.Component, error)

type workerDependencySources struct {
	openDatabase        func(context.Context, config.Config) (workerDatabase, error)
	loadRuntimeRegistry func(string) (*jobruntime.Registry, error)
	newRiverClientID    func() string
	buildOperational    workerFamilyBuilder
	buildDaily          workerFamilyBuilder
	buildReports        func(context.Context, config.Config, workerDatabase, *jobruntime.Registry, jobruntime.Observer, *slog.Logger, *river.Workers) (workerFamily, error)
	buildProviderSync   func(context.Context, config.Config, workerDatabase, *jobruntime.Registry, jobruntime.Observer, *slog.Logger, *river.Workers) (workerFamily, error)
	// buildSyncCoordinator takes a ctx (CHAOS-4175): reference_discovery's
	// native ClickHouse readback verification made this the third ctx-taking
	// builder alongside buildReports/buildProviderSync -- the sync-dispatch
	// coordinator queue never needed ClickHouse before, so this queue's own
	// builder is now in that group instead of the ctx-less one below.
	buildSyncCoordinator func(context.Context, config.Config, workerDatabase, *jobruntime.Registry, jobruntime.Observer, *slog.Logger, *river.Workers) (workerFamily, error)
	buildRiverProcess    workerProcessBuilder
	buildWorkgraph       workerFamilyBuilder
	contractRoot         string
	// newClaimLiveness constructs the CHAOS-4029 claim-liveness tracker (see
	// claim_liveness.go), pre-seeded per selected queue. Injectable so a test
	// can capture the returned *claimLiveness and call SetStaleWindow on it
	// to prove staleness detection in real wall-clock time, the same reason
	// every other constructor in this struct is a field rather than a direct
	// call.
	newClaimLiveness func(time.Time, []string) *claimLiveness
}

var productionWorkerDependencySources = workerDependencySources{
	openDatabase:         openWorkerDatabase,
	loadRuntimeRegistry:  jobruntime.Load,
	newRiverClientID:     defaultRiverClientID,
	buildOperational:     buildOperationalWorker,
	buildSyncCoordinator: buildSyncCoordinatorWorker,
	buildDaily:           buildDailyWorker,
	buildReports:         buildReportWorker,
	buildProviderSync:    buildProviderSyncWorker,
	buildRiverProcess:    newRiverWorkerProcess,
	buildWorkgraph:       buildWorkgraphWorker,
	contractRoot:         defaultContractRoot,
	newClaimLiveness:     newClaimLiveness,
}

func defaultRiverClientID() string {
	return uuid.NewString()
}

type workerDependencies struct {
	database    workerDatabase
	databaseErr error

	runtimeRegistry        *jobruntime.Registry
	registryErr            error
	startup                jobruntime.StartupSpec
	startupErr             error
	metrics                *jobruntime.MetricsCollector
	metricsErr             error
	queueTelemetry         queueTelemetrySampler
	queueTelemetryErr      error
	queueTelemetryRequired bool
	instanceID             string
	// logger is set by configureWorkerDependenciesWithSources. It exists so a
	// readiness check that HAS bounded detail can emit it: the health registry
	// reports check names only, so a refusal with no other outlet reaches an
	// operator as "failed_checks=queued_contract_versions" and nothing more.
	logger *slog.Logger
	// unsupportedContractsMu guards the last reported offender set. Readiness
	// is re-evaluated on EVERY /readyz probe and /metrics scrape, and a
	// failing check does not stop an already-running process, so logging on
	// each evaluation would turn one persistent incompatible row into
	// probe-rate ERROR on every replica. Report a transition, not a state.
	unsupportedContractsMu       sync.Mutex
	reportedUnsupportedContracts string
	shutdownGrace                time.Duration
	workerDrainBudget            time.Duration
	workerGroup                  string
}

type preclaimReadinessComponent struct {
	registry *health.Registry
	logger   *slog.Logger
}

func (preclaimReadinessComponent) Name() string { return "preclaim-readiness" }

func (component preclaimReadinessComponent) Start(ctx context.Context) error {
	if component.registry == nil {
		return errWorkerDependencyUnavailable
	}
	readiness := component.registry.CheckRequired(ctx)
	if readiness.Ready {
		return nil
	}
	// Name the checks that refused. A preclaim failure aborts Start, so the
	// shell exits 1 and the process is restarted -- and the readiness detail
	// that would explain why is only reachable on the operator HTTP surface,
	// which this process never lives long enough to serve. Without this line
	// the whole crash loop reports nothing but the shell's
	// "runtime_failure" category (CHAOS-3902).
	//
	// Check names are bounded compile-time constants registered by this
	// package, and Registry.CheckRequired returns names only -- never a
	// dependency error string that could carry a DSN or credential -- so this
	// is the same disclosure the /readyz surface already makes, on the one
	// path that cannot reach it. Joined the way every other multi-valued
	// worker startup attribute is (see the "queues" attribute).
	if component.logger != nil {
		component.logger.ErrorContext(
			ctx,
			"preclaim readiness refused",
			"error_category",
			"dependency_unavailable",
			"failed_checks",
			strings.Join(readiness.Failed, ","),
		)
	}
	return errWorkerDependencyUnavailable
}

func (preclaimReadinessComponent) Shutdown(context.Context) error { return nil }

type workerProcessComponent struct {
	components []lifecycle.Component
	budget     time.Duration
	presence   workerPresenceLifecycle
}

type workerPresenceLifecycle interface {
	Start(context.Context) error
	BeginDrain(context.Context) error
	Shutdown(context.Context) error
	Errors() <-chan error
}

func (workerProcessComponent) Name() string                            { return "river-workers" }
func (component workerProcessComponent) ShutdownBudget() time.Duration { return component.budget }
func (component workerProcessComponent) Errors() <-chan error {
	if component.presence == nil {
		return nil
	}
	return component.presence.Errors()
}

func (component workerProcessComponent) Start(ctx context.Context) error {
	if component.presence != nil {
		if err := component.presence.Start(ctx); err != nil {
			return err
		}
	}
	started := make([]lifecycle.Component, 0, len(component.components))
	for _, child := range component.components {
		if err := child.Start(ctx); err != nil {
			rollbackCtx := context.WithoutCancel(ctx)
			if component.budget > 0 {
				var rollbackCancel context.CancelFunc
				rollbackCtx, rollbackCancel = context.WithTimeout(rollbackCtx, component.budget)
				defer rollbackCancel()
			}
			var rollback []error
			for index := len(started) - 1; index >= 0; index-- {
				if stopErr := started[index].Shutdown(rollbackCtx); stopErr != nil {
					rollback = append(rollback, stopErr)
				}
			}
			if component.presence != nil {
				if stopErr := component.presence.Shutdown(rollbackCtx); stopErr != nil {
					rollback = append(rollback, stopErr)
				}
			}
			return errors.Join(err, errors.Join(rollback...))
		}
		started = append(started, child)
	}
	return nil
}

func (component workerProcessComponent) Shutdown(ctx context.Context) error {
	var shutdownErrors []error
	if component.presence != nil {
		if err := component.presence.BeginDrain(ctx); err != nil {
			shutdownErrors = append(shutdownErrors, err)
		}
	}
	results := make(chan error, len(component.components))
	for _, child := range component.components {
		go func(child lifecycle.Component) { results <- child.Shutdown(ctx) }(child)
	}
	for range component.components {
		if err := <-results; err != nil {
			shutdownErrors = append(shutdownErrors, err)
		}
	}
	if component.presence != nil {
		if err := component.presence.Shutdown(ctx); err != nil {
			shutdownErrors = append(shutdownErrors, err)
		}
	}
	return errors.Join(shutdownErrors...)
}

func configureWorkerDependencies(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
) ([]lifecycle.Component, error) {
	return configureWorkerDependenciesWithLogger(ctx, cfg, registry, slog.Default())
}

func configureWorkerDependenciesWithLogger(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
) ([]lifecycle.Component, error) {
	return configureWorkerDependenciesWithSources(
		ctx,
		cfg,
		registry,
		productionWorkerDependencySources,
		logger,
	)
}

func configureWorkerDependenciesWithSources(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	sources workerDependencySources,
	loggers ...*slog.Logger,
) ([]lifecycle.Component, error) {
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	dependencies := buildWorkerDependencies(ctx, cfg, sources)
	dependencies.logger = logger
	cfg.WorkerInstanceID = dependencies.instanceID
	if registry == nil {
		dependencies.close()
		return nil, errWorkerDependencyUnavailable
	}
	if dependencies.metricsErr != nil || dependencies.metrics == nil {
		dependencies.close()
		return nil, dependencyUnavailable("worker_metrics_unavailable")
	}
	if err := registry.RegisterMetrics("worker_runtime", workerMetricsSource{
		collector:              dependencies.metrics,
		database:               dependencies.database,
		queueTelemetry:         dependencies.queueTelemetry,
		queueTelemetryRequired: dependencies.queueTelemetryRequired,
	}); err != nil {
		dependencies.close()
		return nil, err
	}
	// worker_database_pool_acquire_seconds needs the collector, which is why
	// this happens here rather than at pool construction: NewRuntimePools
	// freezes its pgxpool tracer before dependencies.metrics exists.
	if dependencies.database != nil {
		dependencies.database.AttachPoolAcquireObserver(dependencies.metrics)
	}
	providerRuntimeConstructed := false
	// livenessMonitor is constructed below, only once dependencies.database is
	// confirmed non-nil (it needs a real domain pool). The execution_liveness
	// check is registered NOW, capturing this variable by reference, so the
	// readiness name exists unconditionally like every other check here --
	// including the "database never opened" path, where it correctly reports
	// unavailable via the nil-guard in executionLivenessReady, exactly as
	// dependencies.domainReady already does for that same path.
	var livenessMonitor *selfprobe.Monitor
	// claim (CHAOS-4029, codex round 1) is the real-claim-path half of
	// execution_liveness -- see claim_liveness.go's package doc. Declared
	// here, before the checks slice, so it is available whether or not
	// database construction later succeeds. sources.newClaimLiveness seeds
	// the clock to now rather than the zero value: claimLivenessReady is
	// evaluated by preclaim-readiness BEFORE the River client ever starts,
	// so a zero seed would fail closed forever on any restart with
	// pre-existing queue backlog -- exactly the deadlock a live rebuild
	// against the shared stack surfaced during this ticket's own
	// development. See newClaimLiveness's doc comment for the full
	// reasoning. Injectable (rather than a direct call) so a test can
	// capture the returned pointer and shrink its staleness window.
	newClaim := sources.newClaimLiveness
	if newClaim == nil {
		newClaim = newClaimLiveness
	}
	claim := newClaim(time.Now(), cfg.Queues)
	checks := []struct {
		name  string
		check health.CheckFunc
	}{
		{name: "domain_postgres", check: dependencies.domainReady},
		{name: "github_projects_v2_startup_config", check: githubProjectsV2StartupReadiness(dependencies.database, logger)},
		{name: "job_registry", check: dependencies.jobRegistryReady},
		{name: "queue_completeness", check: dependencies.queuesReady},
		{name: "provider_route_switches", check: providerRouteSwitchesReady(cfg, &providerRuntimeConstructed)},
		{name: "queued_contract_versions", check: dependencies.queuedContractVersionsReady},
		{name: "queue_control_config", check: dependencies.queueControlConfigReady},
		{name: "queue_postgres", check: dependencies.queueReady},
		{name: "river_schema", check: dependencies.riverSchemaReady(cfg.RiverDatabaseSchema)},
		// idempotency_backend (CHAOS-4029): a synchronous, per-poll Begin+
		// Rollback against the SAME pool internal/jobruntime.PostgresIdempotency
		// uses for every real job's claim. domain_postgres proves the runtime
		// role HOLDS the right grants (a role-posture SELECT); this proves a
		// transaction can be opened right now -- the exact primitive that
		// silently failed for two hours on 2026-08-20 while domain_postgres
		// would have kept passing (the pool was reachable; grants were intact;
		// only the pooled connection's transaction path had gone stale).
		{name: "idempotency_backend", check: dependencies.idempotencyBackendReady},
		// execution_liveness (CHAOS-4029): TWO required facts, not one.
		//
		// (1) livenessMonitor.Ready: the domain pool is reachable and can open
		//     a transaction right now (internal/platform/selfprobe's ticking
		//     self-probe), immune to an idle queue.
		// (2) claimLivenessReady: a real job was actually claimed and started
		//     recently, OR every selected queue is confirmed empty right now.
		//
		// (1) alone was a codex-review finding on this ticket's first round: an
		// independent probe goroutine proves the database is reachable but
		// keeps succeeding even if the real River consumer is deadlocked while
		// the database stays healthy -- exactly the "jobs are all terminal-
		// without-execution" failure mode the ticket's Wanted section names.
		// (2) closes that gap by tying liveness to River's OWN JobStarted
		// callback (claim_liveness.go), the same signal every real job
		// execution already produces, with a queue-telemetry idle fallback so
		// a quiet queue still passes. Both are required: neither fact alone is
		// sufficient proof this process can execute work. Registered here,
		// resolved below once livenessMonitor exists (claim's checkfunc reads
		// dependencies/claim directly and needs no such deferral).
		{name: "execution_liveness", check: func() health.CheckFunc {
			claimReady := dependencies.claimLivenessReady(claim)
			return func(ctx context.Context) error {
				if livenessMonitor == nil {
					return errWorkerDependencyUnavailable
				}
				if err := livenessMonitor.Ready(ctx); err != nil {
					return err
				}
				return claimReady(ctx)
			}
		}()},
	}
	for _, check := range checks {
		if err := registry.RegisterRequired(check.name, check.check); err != nil {
			dependencies.close()
			return nil, err
		}
	}
	if dependencies.database == nil {
		if dependencies.databaseErr != nil && !databaseConfigurationRejected(dependencies.databaseErr) {
			// A DSN that was supplied but could not be opened used to return
			// nil, nil: the shell started with no components, readiness failed
			// forever, and nothing terminated the process -- an alive-but-unready
			// zombie instead of an attributable crash-loop (CHAOS-3873).
			dependencies.close()
			return nil, dependencyUnavailable("worker_database_open_failed")
		}
		// A rejected or absent DSN configuration stays live and unready on
		// purpose, so an operator can scrape readiness and see exactly which
		// check names failed rather than reading a crash loop.
		return nil, nil
	}
	components := []lifecycle.Component{workerDatabaseLifecycle{database: dependencies.database}}
	// Native replacement for the monitor-queue-depths Beat task. It reads the
	// same River telemetry the readiness and metrics paths use, so queue depth
	// and age come from the authoritative queue-control tables.
	if monitor := newQueueHealthMonitor(dependencies.queueTelemetry, logger); monitor != nil {
		components = append(components, monitor)
	}
	// CHAOS-4029: the execution-liveness self-probe. Constructed only now that
	// dependencies.database is confirmed non-nil, so it has a real domain pool
	// to probe. Assigning the already-captured livenessMonitor variable makes
	// the execution_liveness check registered above start resolving to it.
	livenessMonitor = selfprobe.New("worker_execution_liveness", dependencies.database.DomainTxOpener(), logger)
	if livenessMonitor != nil {
		// Probe synchronously now, not only when the lifecycle runtime later
		// calls Start on the returned component. Every other required check
		// this function registers is meaningful the instant registration
		// returns; execution_liveness must hold the same property so a caller
		// that constructs dependencies and reads readiness immediately (as
		// several tests do, and as an operator debugging a startup failure
		// might) sees a real result, not an artifact of whether something else
		// happened to call Start yet.
		livenessMonitor.Probe(ctx)
		components = append(components, livenessMonitor)
		if err := registry.RegisterMetrics("worker_execution_liveness", livenessMonitor); err != nil {
			dependencies.close()
			return nil, err
		}
	}
	workers := river.NewWorkers()
	// claimLivenessObserver taps JobFinished so execution_liveness's claim
	// half (claim_liveness.go) sees every REAL job this process's handlers
	// execute, across every constructed family -- the same shared observer
	// every family builder below receives. It embeds the CONCRETE
	// *jobruntime.MetricsCollector (not the Observer interface) specifically
	// so every existing optional type assertion against the observer
	// (daily.go, operational.go, provider_sync.go, sync_dispatch.go,
	// workgraph.go) keeps matching -- see claimLivenessObserver's doc
	// comment for the round-2 codex finding this fixes.
	observer := claimLivenessObserver{MetricsCollector: dependencies.metrics, liveness: claim}
	active, composeErr := composeSelectedWorkerFamilies(
		ctx, cfg, dependencies.database, dependencies.runtimeRegistry,
		observer, logger, workers, sources,
	)
	if composeErr != nil {
		dependencies.close()
		return nil, dependencyUnavailable("worker_family_composition_failed")
	}
	if active.metricsSource != nil {
		if err := registry.RegisterMetrics("provider_foundation", active.metricsSource); err != nil {
			_ = closeWorkerFamily(active)
			dependencies.close()
			return nil, err
		}
	}
	for _, handler := range active.handlers {
		if handler.Kind == jobcontract.KindSyncProviderUnit {
			providerRuntimeConstructed = true
		}
	}
	// Constructed capability is the only capability. Publish it before the
	// readiness gate opens so exact startup validation sees what this binary
	// actually built, then refuse to start when it does not cover the queues.
	dependencies.startup.Handlers = active.handlers
	dependencies.startup.Queues = active.queues
	if len(active.handlers) > 0 || len(active.queues) > 0 {
		if err := dependencies.queuesReady(ctx); err != nil {
			_ = closeWorkerFamily(active)
			dependencies.close()
			return nil, dependencyUnavailable("queue_coverage_validation_failed")
		}
	}
	components = append(components, preclaimReadinessComponent{registry: registry, logger: logger})
	if len(active.queues) == 0 {
		return components, nil
	}
	if sources.buildRiverProcess == nil {
		_ = closeWorkerFamily(active)
		dependencies.close()
		return nil, dependencyUnavailable("river_process_builder_missing")
	}
	workerProcess, err := sources.buildRiverProcess(
		cfg, dependencies.database, workers, active, logger,
	)
	if err != nil || workerProcess == nil {
		_ = closeWorkerFamily(active)
		dependencies.close()
		return nil, dependencyUnavailable("river_process_construction_failed")
	}
	var presence *jobruntime.WorkerPresence
	if database, ok := dependencies.database.(*postgresWorkerDatabase); ok {
		var presenceErr error
		presence, presenceErr = database.NewWorkerPresence(
			dependencies.workerGroup, cfg.Queues, dependencies.instanceID,
		)
		if presenceErr != nil {
			_ = closeWorkerFamily(active)
			dependencies.close()
			return nil, dependencyUnavailable("worker_presence_unavailable")
		}
	}
	logger.InfoContext(ctx, "worker queues configured",
		"worker_group", dependencies.workerGroup,
		"worker_instance_id", dependencies.instanceID,
		"queues", strings.Join(cfg.Queues, ","),
		"queue_workers", formatQueueBudgets(active.queues),
		// Surfaced so an operator can see the effective drain window, including
		// when it was derived from the selection rather than configured.
		"shutdown_timeout", dependencies.shutdownGrace.String(),
		"drain_budget", dependencies.workerDrainBudget.String(),
		"river_client_count", 1,
		"queue_database_max_connections", dependencies.startup.Connections.QueueControl,
		"domain_database_max_connections", dependencies.startup.Connections.Domain,
	)
	components = append(components, workerProcessComponent{
		components: []lifecycle.Component{workerProcess}, budget: dependencies.workerDrainBudget, presence: presence,
	})
	return components, nil
}

// composeSelectedWorkerFamilies runs every selected family builder against one
// shared river.Workers and then registers rescue coverage exactly once, over
// the union of all owned kinds.
//
// The single-registration ordering is the whole point (CHAOS-3864): when each
// family registered its own rescue coverage, the first family registered
// rescue-only workers for every kind it did not own, and the next family's real
// worker for one of those kinds hit River's duplicate-kind rejection -- so any
// selection spanning two families, including the shipped "heavy" group, exited
// at startup. Production and the multi-family boot test share this function so
// the test cannot drift from the composition order it is proving.
func composeSelectedWorkerFamilies(
	ctx context.Context,
	cfg config.Config,
	database workerDatabase,
	runtimeRegistry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
	workers *river.Workers,
	sources workerDependencySources,
) (workerFamily, error) {
	var active workerFamily
	build := func(family workerFamily, err error) error {
		if err != nil {
			_ = closeWorkerFamily(active)
			return err
		}
		combined, composeErr := composeWorkerFamily(active, family)
		if composeErr != nil {
			_ = closeWorkerFamily(family)
			_ = closeWorkerFamily(active)
			return composeErr
		}
		active = combined
		return nil
	}
	for _, builder := range []workerFamilyBuilder{
		sources.buildOperational,
		sources.buildDaily,
		sources.buildWorkgraph,
	} {
		if builder == nil {
			continue
		}
		if err := build(builder(cfg, database, runtimeRegistry, observer, logger, workers)); err != nil {
			return workerFamily{}, err
		}
	}
	for _, builder := range []func(
		context.Context,
		config.Config,
		workerDatabase,
		*jobruntime.Registry,
		jobruntime.Observer,
		*slog.Logger,
		*river.Workers,
	) (workerFamily, error){
		sources.buildReports,
		sources.buildProviderSync,
		sources.buildSyncCoordinator,
	} {
		if builder == nil {
			continue
		}
		if err := build(builder(ctx, cfg, database, runtimeRegistry, observer, logger, workers)); err != nil {
			return workerFamily{}, err
		}
	}
	// A registry that failed to load is already reported by the job_registry
	// readiness check; failing here too would turn an attributable not-ready
	// process into an opaque construction failure.
	if runtimeRegistry != nil {
		if err := registerRescueCoverage(workers, runtimeRegistry, active.handlers, active.ownedKinds...); err != nil {
			_ = closeWorkerFamily(active)
			return workerFamily{}, dependencyUnavailable("rescue_coverage_registration_failed")
		}
	}
	return active, nil
}

func formatQueueBudgets(budgets []jobruntime.QueueBudget) string {
	values := make([]string, 0, len(budgets))
	for _, budget := range budgets {
		values = append(values, fmt.Sprintf("%s=%d", budget.Queue, budget.MaxWorkers))
	}
	sort.Strings(values)
	return strings.Join(values, ",")
}

// githubProjectsV2StartupReadiness implements CHAOS-3506's one warning-only
// D18 bridge. It first executes the existing domain authorization check (which
// proves migration 0088's snapshot-table posture without selecting Alembic
// state), then asks whether any enabled integration owns the durable target
// key. The environment value itself is never logged or read by the hot route.
func githubProjectsV2StartupReadiness(
	database workerDatabase,
	logger *slog.Logger,
) health.CheckFunc {
	if logger == nil {
		logger = slog.Default()
	}
	var warned sync.Once
	return func(ctx context.Context) error {
		orphaned, err := githubProjectsV2EnvironmentNeedsStartupWarning(
			ctx,
			func(ctx context.Context) (bool, error) {
				if database == nil {
					return false, errWorkerDependencyUnavailable
				}
				// Keep migration/least-privilege verification ahead of the
				// advisory query. DomainReady intentionally reaches only
				// postgres.CheckDomainAuthorization, never public.alembic_version.
				if err := database.DomainReady(ctx); err != nil {
					return false, errWorkerDependencyUnavailable
				}
				reader, ok := database.(githubProjectsV2DurableConfigReader)
				if !ok {
					return false, errWorkerDependencyUnavailable
				}
				return reader.GitHubProjectsV2Configured(ctx)
			},
		)
		if err != nil {
			return errWorkerDependencyUnavailable
		}
		if orphaned {
			warned.Do(func() {
				logger.Warn(
					"GITHUB_PROJECTS_V2 is set but no enabled GitHub integration has durable github_projects_v2 configuration; Go ignores the environment setting",
					"environment", "GITHUB_PROJECTS_V2",
				)
			})
		}
		return nil
	}
}

func composeWorkerFamily(existing, additional workerFamily) (workerFamily, error) {
	result := workerFamily{
		handlers:      append([]jobruntime.HandlerSpec(nil), existing.handlers...),
		queues:        append([]jobruntime.QueueBudget(nil), existing.queues...),
		cleanups:      append([]func() error(nil), existing.cleanups...),
		ownedKinds:    append([]string(nil), existing.ownedKinds...),
		metricsSource: existing.metricsSource,
	}
	result.cleanups = append(result.cleanups, additional.cleanups...)
	if additional.metricsSource != nil {
		if result.metricsSource != nil {
			// Two families both claiming an additional metrics fragment would
			// silently drop one of them at registration; fail closed instead.
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		result.metricsSource = additional.metricsSource
	}
	seen := make(map[string]struct{}, len(result.handlers)+len(additional.handlers))
	for _, handler := range result.handlers {
		if handler.Kind == "" {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		if _, duplicate := seen[handler.Kind]; duplicate {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		seen[handler.Kind] = struct{}{}
	}
	for _, handler := range additional.handlers {
		if handler.Kind == "" {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		if _, duplicate := seen[handler.Kind]; duplicate {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		seen[handler.Kind] = struct{}{}
		result.handlers = append(result.handlers, handler)
	}
	// ownedKinds share the kind namespace with handlers: a kind may be claimed
	// exactly once across all families, whether it is reported as a handler
	// spec or registered directly. Fail closed on any overlap so a duplicate
	// registration surfaces here rather than as a River duplicate-kind panic.
	for _, kind := range result.ownedKinds {
		if kind == "" {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		if _, duplicate := seen[kind]; duplicate {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		seen[kind] = struct{}{}
	}
	for _, kind := range additional.ownedKinds {
		if kind == "" {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		if _, duplicate := seen[kind]; duplicate {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		seen[kind] = struct{}{}
		result.ownedKinds = append(result.ownedKinds, kind)
	}
	queues := make(map[string]struct{}, len(result.queues)+len(additional.queues))
	for _, queue := range result.queues {
		queues[queue.Queue] = struct{}{}
	}
	for _, queue := range additional.queues {
		if _, duplicate := queues[queue.Queue]; duplicate {
			// Two families cannot both own one queue's worker budget: the
			// deployment manifest budgets each queue exactly once.
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		queues[queue.Queue] = struct{}{}
		result.queues = append(result.queues, queue)
	}
	return result, nil
}

func closeWorkerFamily(family workerFamily) error {
	var closeErrors []error
	for index := len(family.cleanups) - 1; index >= 0; index-- {
		if family.cleanups[index] == nil {
			continue
		}
		if err := family.cleanups[index](); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	return errors.Join(closeErrors...)
}

func queueSelected(queues []string, target string) bool {
	for _, queue := range queues {
		if queue == target {
			return true
		}
	}
	return false
}

func anyQueueSelected(queues []string, targets ...string) bool {
	for _, target := range targets {
		if queueSelected(queues, target) {
			return true
		}
	}
	return false
}

func selectedQueueBudgets(queues, targets []string, concurrency map[string]int) []jobruntime.QueueBudget {
	selected := make([]jobruntime.QueueBudget, 0, len(targets))
	for _, queue := range targets {
		if queueSelected(queues, queue) {
			selected = append(selected, jobruntime.QueueBudget{
				Queue: queue, MaxWorkers: concurrency[queue],
			})
		}
	}
	return selected
}

// providerRouteSwitchesReady asserts the serving plane agrees with itself.
//
// CHAOS-4054 removed the route enablement plane: capability is always on in the
// binary, so there is no per-route switch to reconcile against the registry.
// What remains is the -Q topology contract — a process that selected the
// provider-unit queue must actually have constructed the provider-sync runtime
// (and validated the work-item runtime config it captures), or it consumes a
// queue whose units nothing can execute.
func providerRouteSwitchesReady(
	cfg config.Config,
	runtimeConstructed *bool,
) health.CheckFunc {
	if !queueSelected(cfg.Queues, providerUnitQueue) {
		return func(context.Context) error { return nil }
	}
	return func(context.Context) error {
		// The same helper feeds the production BuildExecutor closure below.
		// Ambient STATUS_MAPPING_PATH is rejected before either side opens a
		// provider connection (D19).
		if _, err := workItemsRuntimeConfigFrom(cfg); err != nil {
			return errWorkerDependencyUnavailable
		}
		if runtimeConstructed == nil || !*runtimeConstructed {
			return errWorkerDependencyUnavailable
		}
		return nil
	}
}

type workerMetricsSource struct {
	collector              *jobruntime.MetricsCollector
	database               workerDatabase
	queueTelemetry         queueTelemetrySampler
	queueTelemetryRequired bool
}

func (source workerMetricsSource) WritePrometheus(output io.Writer) error {
	if source.collector == nil {
		return errWorkerDependencyUnavailable
	}
	if source.database != nil {
		domain, queueControl := source.database.PoolSaturation()
		if err := source.collector.SetDatabasePoolSaturation("domain", domain); err != nil {
			return err
		}
		if err := source.collector.SetDatabasePoolSaturation("queue_control", queueControl); err != nil {
			return err
		}
	}
	if source.queueTelemetryRequired {
		if source.queueTelemetry == nil {
			return errWorkerDependencyUnavailable
		}
		snapshot, err := source.queueTelemetry.Snapshot(context.Background())
		if err != nil {
			return errWorkerDependencyUnavailable
		}
		for _, job := range snapshot.Jobs {
			if err := source.collector.SetJobsAvailable(jobruntime.JobLabels{
				Queue: job.Queue,
				Kind:  job.Kind,
			}, job.Available); err != nil {
				return err
			}
		}
		for _, queue := range snapshot.Queues {
			if err := source.collector.SetJobOldestAge(queue.Queue, queue.OldestAvailableAge); err != nil {
				return err
			}
		}
		for _, queue := range snapshot.QueueCapacities {
			if err := source.collector.SetExecutionSaturation(queue.Queue, queue.Saturation); err != nil {
				return err
			}
		}
	}
	return source.collector.WritePrometheus(output)
}

func buildWorkerDependencies(
	ctx context.Context,
	cfg config.Config,
	sources workerDependencySources,
) *workerDependencies {
	dependencies := &workerDependencies{}
	if sources.newRiverClientID == nil {
		dependencies.startupErr = errWorkerDependencyUnavailable
		dependencies.instanceID = ""
	} else {
		dependencies.instanceID = sources.newRiverClientID()
	}
	if sources.openDatabase == nil {
		dependencies.databaseErr = errWorkerDependencyUnavailable
	} else {
		dependencies.database, dependencies.databaseErr = sources.openDatabase(ctx, cfg)
		if dependencies.databaseErr != nil && dependencies.database != nil {
			dependencies.database.Close()
			dependencies.database = nil
		}
	}

	if sources.loadRuntimeRegistry == nil || sources.contractRoot == "" {
		dependencies.registryErr = errWorkerDependencyUnavailable
		dependencies.startupErr = errWorkerDependencyUnavailable
		dependencies.metrics, dependencies.metricsErr = buildWorkerMetrics(ctx, cfg, nil)
		return dependencies
	}
	dependencies.runtimeRegistry, dependencies.registryErr = sources.loadRuntimeRegistry(sources.contractRoot)
	dependencies.metrics, dependencies.metricsErr = buildWorkerMetrics(ctx, cfg, dependencies.runtimeRegistry)
	if dependencies.registryErr != nil {
		dependencies.startupErr = errWorkerDependencyUnavailable
		return dependencies
	}
	if len(cfg.Queues) == 0 || len(cfg.WorkerQueueConcurrency) != len(cfg.Queues) {
		dependencies.startupErr = errWorkerDependencyUnavailable
		return dependencies
	}
	descriptors, err := dependencies.runtimeRegistry.SelectedQueues(cfg.Queues)
	if err != nil || len(descriptors) == 0 {
		dependencies.startupErr = errWorkerDependencyUnavailable
		return dependencies
	}
	longestTimeout := time.Duration(0)
	for _, descriptor := range descriptors {
		if descriptor.Timeout > longestTimeout {
			longestTimeout = descriptor.Timeout
		}
	}
	dependencies.workerGroup = cfg.WorkerGroup
	if dependencies.workerGroup == "" {
		dependencies.workerGroup = "worker"
	}
	dependencies.shutdownGrace = cfg.ShutdownTimeout
	// The drain budget is the shutdown grace minus a finalization buffer and
	// must cover the longest selected timeout. The 30s config default cannot
	// satisfy that for any real selection -- it yields a NEGATIVE budget -- so
	// every default-configured worker failed with the opaque sentinel
	// (CHAOS-3873). An unset timeout is derived from the selection; an operator
	// who set one explicitly still gets a hard, attributable failure.
	requiredGrace := longestTimeout + workerFinalizationBuffer
	// Only an unset timeout is derived. A value the operator chose -- including
	// one that happens to equal the default -- still fails closed, so the
	// contract check keeps its teeth.
	if !cfg.ShutdownTimeoutExplicit && dependencies.shutdownGrace <= config.DefaultShutdownTimeout {
		dependencies.shutdownGrace = requiredGrace
	}
	dependencies.workerDrainBudget = dependencies.shutdownGrace - workerFinalizationBuffer
	if dependencies.workerDrainBudget < longestTimeout {
		dependencies.startupErr = dependencyUnavailable("shutdown_timeout_below_drain_budget")
		return dependencies
	}
	// Queues and Handlers stay empty here on purpose: they are filled in only by
	// concretely constructed handler families. The deployment-selected queue
	// concurrency is an expectation to prove against, never a capability claim.
	configuredQueues := make([]jobruntime.QueueBudget, 0, len(cfg.Queues))
	for _, queue := range cfg.Queues {
		workers := cfg.WorkerQueueConcurrency[queue]
		if workers < 1 || workers > 10_000 {
			dependencies.startupErr = errWorkerDependencyUnavailable
			return dependencies
		}
		configuredQueues = append(configuredQueues, jobruntime.QueueBudget{Queue: queue, MaxWorkers: workers})
	}
	dependencies.startup = jobruntime.StartupSpec{
		SelectedQueues:   append([]string(nil), cfg.Queues...),
		ConfiguredQueues: configuredQueues,
		Connections: jobruntime.ConnectionBudget{
			QueueControl: int(cfg.QueueDatabaseMaxConns),
			Domain:       int(cfg.DomainDatabaseMaxConns),
		},
		ConfiguredConnections: jobruntime.ConnectionBudget{
			QueueControl: int(cfg.QueueDatabaseMaxConns),
			Domain:       int(cfg.DomainDatabaseMaxConns),
		},
	}
	dependencies.buildQueueTelemetry(cfg, configuredQueues, descriptors, sources)
	return dependencies
}

func (dependencies *workerDependencies) buildQueueTelemetry(
	cfg config.Config,
	queueBudgets []jobruntime.QueueBudget,
	descriptors []jobruntime.Descriptor,
	sources workerDependencySources,
) {
	if len(descriptors) == 0 || len(queueBudgets) == 0 {
		return
	}
	dependencies.queueTelemetryRequired = true
	if dependencies.databaseErr != nil || dependencies.database == nil || dependencies.instanceID == "" {
		dependencies.queueTelemetryErr = errWorkerDependencyUnavailable
		return
	}
	queues := make([]riverstore.QueueTelemetryQueue, 0, len(queueBudgets))
	for _, queue := range queueBudgets {
		queues = append(queues, riverstore.QueueTelemetryQueue{Name: queue.Queue, MaxWorkers: queue.MaxWorkers})
	}
	jobs := make([]riverstore.QueueTelemetryJob, 0, len(descriptors))
	for _, descriptor := range descriptors {
		jobs = append(jobs, riverstore.QueueTelemetryJob{
			Queue:             descriptor.Queue,
			Kind:              descriptor.Kind,
			SupportedVersions: append([]int(nil), descriptor.SupportedVersions...),
		})
	}
	dependencies.queueTelemetry, dependencies.queueTelemetryErr = dependencies.database.NewQueueTelemetrySampler(
		riverstore.QueueTelemetryConfig{
			Schema:    cfg.RiverDatabaseSchema,
			ClientID:  dependencies.instanceID,
			Queues:    queues,
			Jobs:      jobs,
			Occupants: nonRegistryQueueOccupants(queueBudgets),
		},
	)
}

// nonRegistryQueueOccupants is the second half of "who may legitimately have
// rows in the selected River queues". The first half is the bounded jobs
// registry, already projected into Jobs above from
// jobruntime.Registry.SelectedQueues.
//
// river.river_job is shared by two route planes that are deliberately distinct:
// worker_job_routes governs the bounded registry kinds, and
// sync_dispatch_transport_routes governs the coordinator kinds, which carry no
// registry descriptor at all (see syncdispatchruntime.RegisterWorkers). Nothing
// merges them, and nothing should -- the sync-dispatch kinds are outside the
// registry until CUT-10 brings them in, and giving dispatch_sync_run a
// registry entry today would additionally subject it to registry startup
// validation, which is precisely the coverage this binary does NOT report for
// it. So the reader unions the planes instead of the planes being merged.
//
// The union is derived, not enumerated: the occupancy comes from
// syncdispatchruntime.RiverQueueOccupancy(), which is itself derived from the
// frozen contract kinds. Adding one registry line for dispatch_sync_run would
// have fixed exactly one kind and left the next one to trip the same wire
// (CHAOS-3938).
func nonRegistryQueueOccupants(queueBudgets []jobruntime.QueueBudget) []riverstore.QueueTelemetryOccupant {
	selected := make(map[string]struct{}, len(queueBudgets))
	for _, budget := range queueBudgets {
		selected[budget.Queue] = struct{}{}
	}
	occupants := make([]riverstore.QueueTelemetryOccupant, 0, len(queueBudgets))
	for _, occupancy := range syncdispatchruntime.RiverQueueOccupancy() {
		if _, ok := selected[occupancy.Queue]; !ok {
			continue
		}
		occupants = append(occupants, riverstore.QueueTelemetryOccupant{
			Queue:             occupancy.Queue,
			Kind:              occupancy.Kind,
			SupportedVersions: append([]int(nil), occupancy.SupportedVersions...),
		})
	}
	return occupants
}

func buildWorkerMetrics(
	ctx context.Context,
	cfg config.Config,
	runtimeRegistry *jobruntime.Registry,
) (*jobruntime.MetricsCollector, error) {
	dimensions := jobruntime.MetricDimensions{}
	if runtimeRegistry != nil {
		derived, err := jobruntime.DimensionsForQueues(
			runtimeRegistry, cfg.Queues, nil,
			budgetDimensionsForQueues(cfg.Queues), syncLeaseDimensionsForQueues(cfg.Queues),
			concurrencyBudgetDimensionsForQueues(runtimeRegistry, cfg.Queues),
		)
		if err != nil {
			return nil, err
		}
		dimensions = derived
	}
	collector, err := jobruntime.NewMetricsCollector(dimensions)
	if err != nil {
		return nil, err
	}
	build := version.Current(cfg.Service)
	if err := jobruntime.RegisterRuntime(ctx, collector, jobruntime.RuntimeInfo{
		Version: build.Version,
		Commit:  build.Commit,
	}); err != nil {
		return nil, err
	}
	return collector, nil
}

func concurrencyBudgetDimensionsForQueues(registry *jobruntime.Registry, queues []string) []jobruntime.ConcurrencyBudgetLabels {
	if registry == nil {
		return nil
	}
	descriptors, err := registry.SelectedQueues(queues)
	if err != nil {
		return nil
	}
	seen := make(map[jobruntime.ConcurrencyBudgetLabels]struct{})
	var labels []jobruntime.ConcurrencyBudgetLabels
	for _, descriptor := range descriptors {
		if descriptor.ConcurrencyScope != "fleet" && descriptor.ConcurrencyScope != "organization" {
			continue
		}
		label := jobruntime.ConcurrencyBudgetLabels{Kind: descriptor.Kind, Scope: descriptor.ConcurrencyScope}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		labels = append(labels, label)
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i].Kind == labels[j].Kind {
			return labels[i].Scope < labels[j].Scope
		}
		return labels[i].Kind < labels[j].Kind
	})
	return labels
}

// syncLeaseDimensionsForQueues registers the frozen provider/dataset matrix
// (providersync.MatrixProviders + Capabilities, TRD §10.1) as the bounded
// worker_sync_lease_expired_total dimension set. Only a process that selects
// the provider-unit queue constructs the handler that can observe a recovered
// claim, so other queue selections keep an empty, harmless set. The matrix is static
// deployment configuration, not runtime or tenant data, so this stays well
// under maxMetricSyncLeases regardless of which providers are feature-flag
// enabled today.
func syncLeaseDimensionsForQueues(queues []string) []jobruntime.SyncLeaseLabels {
	if !queueSelected(queues, providerUnitQueue) {
		return nil
	}
	var labels []jobruntime.SyncLeaseLabels
	for _, provider := range providersync.MatrixProviders() {
		for _, capability := range providersync.Capabilities(provider) {
			labels = append(labels, jobruntime.SyncLeaseLabels{
				Provider: capability.Provider, DatasetFamily: capability.Dataset,
			})
		}
	}
	return labels
}

// budgetDimensionsForQueues registers the same frozen provider matrix, paired
// with providersync's three static cost classes, as the bounded
// worker_budget_wait_seconds dimension set. Only a process selecting the
// provider-unit queue builds an executor that acquires a provider cost budget.
func budgetDimensionsForQueues(queues []string) []jobruntime.BudgetLabels {
	if !queueSelected(queues, providerUnitQueue) {
		return nil
	}
	costClasses := []providersync.CostClass{
		providersync.CostLight, providersync.CostMedium, providersync.CostHeavy,
	}
	var labels []jobruntime.BudgetLabels
	for _, provider := range providersync.MatrixProviders() {
		for _, costClass := range costClasses {
			labels = append(labels, jobruntime.BudgetLabels{
				Provider: provider, CostClass: string(costClass),
			})
		}
	}
	return labels
}

func (dependencies *workerDependencies) domainReady(ctx context.Context) error {
	if dependencies == nil || dependencies.databaseErr != nil || dependencies.database == nil {
		return errWorkerDependencyUnavailable
	}
	if err := dependencies.database.DomainReady(ctx); err != nil {
		return errWorkerDependencyUnavailable
	}
	return nil
}

func (dependencies *workerDependencies) queueReady(ctx context.Context) error {
	if dependencies == nil || dependencies.databaseErr != nil || dependencies.database == nil {
		return errWorkerDependencyUnavailable
	}
	if err := dependencies.database.QueueReady(ctx); err != nil {
		return errWorkerDependencyUnavailable
	}
	return nil
}

// idempotencyBackendReady is the idempotency_backend check's CheckFunc (see
// its registration comment in configureWorkerDependenciesWithSources for the
// full rationale). It resolves dependencies.database at call time, exactly
// like domainReady/queueReady above, so it correctly reports unavailable
// during the "database never opened" live-and-unready path.
func (dependencies *workerDependencies) idempotencyBackendReady(ctx context.Context) error {
	if dependencies == nil || dependencies.databaseErr != nil || dependencies.database == nil {
		return errWorkerDependencyUnavailable
	}
	if err := selfprobe.Once(ctx, dependencies.database.DomainTxOpener()); err != nil {
		return errWorkerDependencyUnavailable
	}
	return nil
}

// queueControlConfigReady gives operators one bounded, actionable readiness
// category for queue-control configuration failures. The underlying error is
// never exposed because it may have originated at a DSN parsing boundary.
// Connectivity and schema failures remain separate readiness categories.
func (dependencies *workerDependencies) queueControlConfigReady(context.Context) error {
	if dependencies == nil {
		return errWorkerDependencyUnavailable
	}
	if dependencies.databaseErr == nil {
		return nil
	}
	for _, configurationError := range []error{
		postgres.ErrQueueControlRequired,
		postgres.ErrQueueControlTransactionMode,
		postgres.ErrRuntimeRolesNotSeparated,
		postgres.ErrRuntimeRoleConfiguration,
	} {
		if errors.Is(dependencies.databaseErr, configurationError) {
			return errWorkerDependencyUnavailable
		}
	}
	return nil
}

func (dependencies *workerDependencies) riverSchemaReady(schema string) health.CheckFunc {
	return func(ctx context.Context) error {
		if dependencies == nil || dependencies.databaseErr != nil || dependencies.database == nil {
			return errWorkerDependencyUnavailable
		}
		if err := dependencies.database.RiverSchemaReady(ctx, schema); err != nil {
			return errWorkerDependencyUnavailable
		}
		return nil
	}
}

func (dependencies *workerDependencies) jobRegistryReady(context.Context) error {
	if dependencies == nil || dependencies.registryErr != nil || dependencies.runtimeRegistry == nil {
		return errWorkerDependencyUnavailable
	}
	return nil
}

// queuesReady is the production call site for exact startup validation. It
// proves the registry's executable coverage, the constructed queue consumers,
// and the deployment budget in one place, so no other readiness path can
// approve a partially constructed queue selection.
func (dependencies *workerDependencies) queuesReady(context.Context) error {
	if dependencies == nil || dependencies.registryErr != nil ||
		dependencies.runtimeRegistry == nil || dependencies.startupErr != nil {
		return errWorkerDependencyUnavailable
	}
	if err := dependencies.runtimeRegistry.ValidateStartup(dependencies.startup); err != nil {
		return errWorkerDependencyUnavailable
	}
	return nil
}

func (dependencies *workerDependencies) queuedContractVersionsReady(ctx context.Context) error {
	if dependencies == nil || !dependencies.queueTelemetryRequired {
		return nil
	}
	if dependencies.queueTelemetryErr != nil || dependencies.queueTelemetry == nil {
		return errWorkerDependencyUnavailable
	}
	if err := dependencies.queueTelemetry.CheckAvailableContractVersions(ctx); err != nil {
		// Name the offending contract. The health registry surface reports
		// check names only, so without this an operator sees
		// "failed_checks=queued_contract_versions" and has no way to tell
		// which kind, which queue, or which version refused the start -- which
		// is what turned CHAOS-3938 into a 20-minute outage instead of a
		// one-line diagnosis. Only bounded, re-validated queue/kind/version
		// labels are logged; see riverstore.UnsupportedContractVersionError.
		var unsupported *riverstore.UnsupportedContractVersionError
		if errors.As(err, &unsupported) {
			dependencies.reportUnsupportedContracts(ctx, strings.Join(unsupported.Offenders, ","))
		}
		return errWorkerDependencyUnavailable
	}
	dependencies.reportUnsupportedContracts(ctx, "")
	return nil
}

// reportUnsupportedContracts logs the offender set only when it CHANGES,
// including the change back to empty. A repeat of the same set is the same
// fact restated at probe rate; a different set, or a recurrence after
// recovery, is new information and is logged again.
func (dependencies *workerDependencies) reportUnsupportedContracts(ctx context.Context, offenders string) {
	dependencies.unsupportedContractsMu.Lock()
	changed := dependencies.reportedUnsupportedContracts != offenders
	dependencies.reportedUnsupportedContracts = offenders
	dependencies.unsupportedContractsMu.Unlock()
	if !changed || offenders == "" || dependencies.logger == nil {
		return
	}
	dependencies.logger.ErrorContext(
		ctx,
		"queued contract version is not supported by this worker",
		"error_category", "dependency_unavailable",
		"check", "queued_contract_versions",
		"unsupported_contracts", offenders,
	)
}

func (dependencies *workerDependencies) close() {
	if dependencies != nil && dependencies.database != nil {
		dependencies.database.Close()
	}
}

type workerDatabaseLifecycle struct {
	database workerDatabase
}

func (workerDatabaseLifecycle) Name() string { return "postgres-runtime-pools" }

func (component workerDatabaseLifecycle) Start(context.Context) error {
	if component.database == nil {
		return errWorkerDependencyUnavailable
	}
	return nil
}

func (component workerDatabaseLifecycle) Shutdown(context.Context) error {
	if component.database != nil {
		component.database.Close()
	}
	return nil
}
