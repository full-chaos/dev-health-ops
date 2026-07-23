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
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
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

func buildProviderSyncWorker(
	ctx context.Context,
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
) (lifecycle.Component, []jobruntime.HandlerSpec, error) {
	if cfg.Profile != "sync" || !cfg.WorkerLaunchDarklyFeatureFlagsEnabled {
		return nil, nil, nil
	}
	if registry == nil || observer == nil || logger == nil ||
		!cfg.SettingsEncryptionKey.Configured() {
		return nil, nil, errWorkerDependencyUnavailable
	}
	spec, ok := registry.Descriptor(jobcontract.KindSyncProviderUnit)
	if !ok || !spec.Executable() || spec.Route != "river_canary" ||
		spec.RollbackRoute != "celery" {
		return nil, nil, errWorkerDependencyUnavailable
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	repository, err := providersync.NewPostgresRepository(
		postgresDatabase.pools.Domain,
	)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(
		cfg.SettingsEncryptionKey, "",
	)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	clickhouseConnection, err := clickhousestore.Open(
		ctx, clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
	)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	valkeyClient, err := valkeystore.Open(
		ctx, valkeystore.DefaultConfig(cfg.ValkeyURI.Reveal()),
	)
	if err != nil {
		_ = clickhouseConnection.Close()
		return nil, nil, errWorkerDependencyUnavailable
	}
	closeDependencies := func() {
		valkeyClient.Close()
		_ = clickhouseConnection.Close()
	}
	switches := providersync.CompleteRouteSwitches{
		LaunchDarklyFeatureFlags: true,
	}
	handler := &providerunit.Handler{
		Repository:    repository,
		Switches:      switches,
		LeaseDuration: providerUnitLeaseDuration,
		Heartbeat:     providerUnitHeartbeat,
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
					Client: valkeyClient,
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
					}
				},
				Metrics: providerfoundation.NewMetrics(),
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
		return nil, nil, errWorkerDependencyUnavailable
	}
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, adapter); err != nil {
		closeDependencies()
		return nil, nil, errWorkerDependencyUnavailable
	}
	client, err := river.NewClient(
		riverpgxv5.New(postgresDatabase.pools.QueueControl),
		&river.Config{
			Logger: logger,
			Queues: map[string]river.QueueConfig{
				"sync": {MaxWorkers: 2},
			},
			Schema:  cfg.RiverDatabaseSchema,
			Workers: workers,
		},
	)
	if err != nil {
		closeDependencies()
		return nil, nil, errWorkerDependencyUnavailable
	}
	return &providerSyncWorkerComponent{
		client: client, clickhouse: clickhouseConnection, valkey: valkeyClient,
	}, []jobruntime.HandlerSpec{adapter.Spec()}, nil
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
