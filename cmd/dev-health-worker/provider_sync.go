package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/providerunit"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	valkeygo "github.com/valkey-io/valkey-go"
)

const (
	// providerUnitQueue and its worker budget must match the deployment manifest
	// entry for the sync process; exact startup validation compares the two.
	providerUnitQueue         = "sync_provider"
	providerUnitQueueWorkers  = 2
	providerUnitLeaseDuration = 2 * time.Minute
	providerUnitHeartbeat     = 30 * time.Second
	providerUnitBudgetTTL     = 15 * time.Minute
)

type providerSyncWorkerComponent struct {
	client     *river.Client[pgx.Tx]
	clickhouse driver.Conn
	valkey     valkeygo.Client
}

func (component *providerSyncWorkerComponent) Name() string {
	return "river-provider-sync-worker"
}

func (component *providerSyncWorkerComponent) Start(ctx context.Context) error {
	if component == nil || component.client == nil {
		return errWorkerDependencyUnavailable
	}
	return component.client.Start(ctx)
}

func (component *providerSyncWorkerComponent) Shutdown(ctx context.Context) error {
	if component == nil {
		return nil
	}
	var result error
	if component.client != nil {
		result = component.client.Stop(ctx)
	}
	if component.valkey != nil {
		component.valkey.Close()
	}
	if component.clickhouse != nil {
		if err := component.clickhouse.Close(); result == nil {
			result = err
		}
	}
	return result
}

// budgetWaitObserver bridges providerfoundation's credential-free
// BudgetWaitObserver to the process's shared MetricsCollector. It is a value
// type specifically so a nil collector never becomes a non-nil, panicking
// interface value: ObserveProviderBudgetWait is a no-op until a real
// collector is attached.
type budgetWaitObserver struct {
	collector *jobruntime.MetricsCollector
}

func (o budgetWaitObserver) ObserveProviderBudgetWait(provider, costClass string, wait time.Duration) error {
	if o.collector == nil {
		return nil
	}
	return o.collector.ObserveProviderBudgetWait(jobruntime.BudgetLabels{Provider: provider, CostClass: costClass}, wait)
}

var _ providerfoundation.BudgetWaitObserver = budgetWaitObserver{}

// leaseRecoveryObserver bridges providerunit's expired-lease recovery
// signal to the shared MetricsCollector, for the same nil-safety reason as
// budgetWaitObserver above.
type leaseRecoveryObserver struct {
	collector *jobruntime.MetricsCollector
}

func (o leaseRecoveryObserver) ObserveSyncLeaseExpired(labels jobruntime.SyncLeaseLabels, result jobruntime.SyncLeaseResult) error {
	if o.collector == nil {
		return nil
	}
	return o.collector.ObserveSyncLeaseExpired(labels, result)
}

var _ jobruntime.SyncLeaseObserver = leaseRecoveryObserver{}

func buildProviderSyncWorker(
	ctx context.Context,
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
) (workerFamily, error) {
	if cfg.Profile != "sync" || !cfg.WorkerLaunchDarklyFeatureFlagsEnabled {
		return workerFamily{}, nil
	}
	if registry == nil || observer == nil || logger == nil ||
		!cfg.SettingsEncryptionKey.Configured() {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	spec, ok := registry.Descriptor(jobcontract.KindSyncProviderUnit)
	if !ok || !spec.Executable() || spec.Route != "river_canary" ||
		spec.RollbackRoute != "celery" {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	repository, err := providersync.NewPostgresRepository(
		postgresDatabase.pools.Domain,
	)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(
		cfg.SettingsEncryptionKey, "",
	)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	clickhouseConnection, err := clickhousestore.Open(
		ctx, clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
	)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	valkeyClient, err := valkeystore.Open(
		ctx, valkeystore.DefaultConfig(cfg.ValkeyURI.Reveal()),
	)
	if err != nil {
		_ = clickhouseConnection.Close()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	closeDependencies := func() {
		valkeyClient.Close()
		_ = clickhouseConnection.Close()
	}
	switches := providersync.CompleteRouteSwitches{
		LaunchDarklyFeatureFlags: true,
	}
	// The observer passed in is always the process's one *jobruntime.MetricsCollector
	// in production; the assertion fails closed to a nil collector (both bridge
	// types below are then safe no-ops) for any other Observer implementation,
	// such as a test double.
	collector, _ := observer.(*jobruntime.MetricsCollector)
	// providerMetrics is constructed exactly once per worker process and
	// referenced by every claim's executor, so dev_health_provider_* actually
	// accumulates across dispatches instead of being built and discarded per
	// unit. It is registered with the health.Registry via workerFamily.metricsSource
	// below, which is what makes it a live, scraped series rather than a
	// constructed-but-never-registered family.
	providerMetrics := providerfoundation.NewMetrics()
	handler := &providerunit.Handler{
		Repository:    repository,
		Switches:      switches,
		LeaseDuration: providerUnitLeaseDuration,
		Heartbeat:     providerUnitHeartbeat,
		// LeaseMetrics observes worker_sync_lease_expired_total. Only claims
		// this handler itself resolved after Repository.Claim reported
		// claim.Recovered (an expired lease was recovered into this attempt)
		// are ever observed; see observeLeaseRecovery below.
		LeaseMetrics: leaseRecoveryObserver{collector: collector},
		// A route fault means the Python producer gate routed a scope the Go
		// capability system does not serve. The unit is never terminalized
		// (TRD non-negotiable #3), so this log is the operator's only signal
		// that a producer gate needs reconciliation. Fields are bounded: no
		// credentials, URLs, or provider payloads.
		OnRouteFault: func(fault providerunit.RouteFault) {
			logger.Error(
				"provider unit route reconciliation required",
				"provider", fault.Provider,
				"dataset", fault.Dataset,
				"descriptor_present", fault.DescriptorPresent,
				"route_ready", fault.RouteReady,
				"route_enabled", fault.RouteEnabled,
				"attempt", fault.Attempt,
				"max_attempts", fault.MaxAttempts,
				"released_for_retry", fault.Released,
				"terminal_reconciliation_required", fault.Terminal,
			)
		},
		BuildExecutor: func(
			session *providersync.LeaseSession,
		) (providersync.CompleteRouteExecutor, error) {
			if session == nil {
				return providersync.CompleteRouteExecutor{},
					errWorkerDependencyUnavailable
			}
			sink := providersync.LaunchDarklyClickHouseEffects{
				Conn: clickhouseConnection, Lease: session,
			}
			return providersync.CompleteRouteExecutor{
				Credentials: providerfoundation.CredentialResolver{
					Repository: providerfoundation.PostgresCredentialRepository{
						Pool: postgresDatabase.pools.Domain,
					},
					Decryptor: decryptor,
				},
				Doer:  &http.Client{Timeout: 45 * time.Second},
				Retry: providerfoundation.DefaultRetryPolicy(),
				Budget: providerfoundation.ValkeyBudgetStore{
					Client:   valkeyClient,
					Observer: budgetWaitObserver{collector: collector},
				},
				BudgetLimits: map[providersync.CostClass]int{
					providersync.CostLight:  4,
					providersync.CostMedium: 2,
					providersync.CostHeavy:  1,
				},
				BudgetTTL: providerUnitBudgetTTL,
				Gate: func(
					claim providersync.Claim,
					client *providerfoundation.HTTPClient,
				) providerfoundation.BackoffGate {
					if client == nil || client.BaseURL == nil {
						return nil
					}
					return providerfoundation.ValkeyBackoffGate{
						Client: valkeyClient, Provider: claim.Provider,
						OrgID: claim.OrgID, Host: client.BaseURL.Hostname(),
						MaxBackoff: 5 * time.Minute,
						CostClass:  string(claim.CostClass),
						Observer:   budgetWaitObserver{collector: collector},
					}
				},
				Metrics: providerMetrics,
				Handler: providersync.LaunchDarklyRouteHandler{
					CodeReferences: providersync.LaunchDarklyClickHouseReferences{
						Conn: clickhouseConnection, Lease: session,
					},
				},
				Comparator: providersync.ProductionContractComparator{},
				Committer: providersync.EffectCommitter{
					Ledger: repository, Sink: sink, Readback: sink,
				},
				HeartbeatInterval: providerUnitHeartbeat,
			}, nil
		},
	}
	adapter, err := jobruntime.NewAdapter[jobruntime.ProviderUnitArgs](
		registry, spec, handler, jobruntime.Dependencies{
			Logger: logger, Observer: observer,
			TenantScope: providerUnitTenantScope{},
			Budget:      newOperationalBudget(),
			Idempotency: providerunit.AuthoritativeIdempotency{},
		},
	)
	if err != nil {
		closeDependencies()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, adapter); err != nil {
		closeDependencies()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	client, err := river.NewClient(
		riverpgxv5.New(postgresDatabase.pools.QueueControl),
		providerSyncRiverConfig(
			logger, workers, cfg.RiverDatabaseSchema,
		),
	)
	if err != nil {
		closeDependencies()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	return workerFamily{
		component: &providerSyncWorkerComponent{
			client: client, clickhouse: clickhouseConnection, valkey: valkeyClient,
		},
		handlers: []jobruntime.HandlerSpec{adapter.Spec()},
		queues: []jobruntime.QueueBudget{
			{Queue: providerUnitQueue, MaxWorkers: providerUnitQueueWorkers},
		},
		metricsSource: providerMetrics,
	}, nil
}

func providerSyncRiverConfig(
	logger *slog.Logger,
	workers *river.Workers,
	schema string,
) *river.Config {
	return &river.Config{
		Logger: logger,
		Queues: map[string]river.QueueConfig{
			providerUnitQueue: {MaxWorkers: providerUnitQueueWorkers},
		},
		Schema:  schema,
		Workers: workers,
	}
}

type providerUnitTenantScope struct{}

func (providerUnitTenantScope) Supports(scope string) bool {
	return scope == "tenant"
}

func (providerUnitTenantScope) Resolve(
	ctx context.Context,
	request jobruntime.ScopeRequest,
) (context.Context, error) {
	if ctx == nil || request.OrganizationScope != "tenant" ||
		request.OrganizationID == nil ||
		request.Domain.Type != "sync_run_unit" {
		return nil, jobruntime.DomainMismatch(errWorkerDependencyUnavailable)
	}
	return ctx, nil
}
