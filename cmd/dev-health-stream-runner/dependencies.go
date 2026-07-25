package main

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/externalrecompute"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/processreadiness"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

var errStreamDependencyUnavailable = errors.New("stream-runner dependency is unavailable")

type streamHandlerKind string

const (
	internalIngestHandlerKind   streamHandlerKind = "internal-ingest"
	productTelemetryHandlerKind streamHandlerKind = "product-telemetry"
	externalIngestHandlerKind   streamHandlerKind = "external-ingest"
)

type streamStorage interface {
	ClickHouseReady(context.Context) error
	DomainPostgresReady(context.Context) error
	ValkeyReady(context.Context) error
	Handler(streamHandlerKind) (streamrunner.Handler, error)
	NewTransport() (streamrunner.Transport, error)
	ControlComponents() []lifecycle.Component
	Close()
}

type productionStreamStorage struct {
	clickHouse  driver.Conn
	domainPool  *pgxpool.Pool
	valkey      valkeygo.Client
	domainRole  string
	riverSchema string
	recompute   *externalrecompute.Controller
}

func openProductionStreamStorage(ctx context.Context, cfg config.Config) (streamStorage, error) {
	if !cfg.ClickHouseURI.Configured() || !cfg.DomainDatabaseURI.Configured() || !cfg.ValkeyURI.Configured() {
		return nil, errStreamDependencyUnavailable
	}
	domainConfig := postgres.DefaultConfig(cfg.DomainDatabaseURI.Reveal())
	domainConfig.MaxConns = cfg.DomainDatabaseMaxConns
	domainPool, err := postgres.New(ctx, domainConfig)
	if err != nil {
		return nil, errStreamDependencyUnavailable
	}
	clickHouse, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(cfg.ClickHouseURI.Reveal()))
	if err != nil {
		domainPool.Close()
		return nil, errStreamDependencyUnavailable
	}
	valkeyConfig := valkey.DefaultConfig(cfg.ValkeyURI.Reveal())
	valkeyConfig.ClientName = "dev-health-stream-runner-" + cfg.Profile
	valkeyClient, err := valkey.Open(ctx, valkeyConfig)
	if err != nil {
		_ = clickHouse.Close()
		domainPool.Close()
		return nil, errStreamDependencyUnavailable
	}
	return &productionStreamStorage{
		clickHouse: clickHouse, domainPool: domainPool, valkey: valkeyClient,
		domainRole: cfg.DomainDatabaseRole, riverSchema: cfg.RiverDatabaseSchema,
	}, nil
}

func (storage *productionStreamStorage) ClickHouseReady(ctx context.Context) error {
	if storage == nil || storage.clickHouse == nil || storage.clickHouse.Ping(ctx) != nil {
		return errStreamDependencyUnavailable
	}
	return nil
}

func (storage *productionStreamStorage) DomainPostgresReady(ctx context.Context) error {
	if storage == nil || storage.domainPool == nil {
		return errStreamDependencyUnavailable
	}
	if err := postgres.CheckDomainAuthorization(ctx, storage.domainPool, storage.domainRole, storage.riverSchema); err != nil {
		return errStreamDependencyUnavailable
	}
	return nil
}

func (storage *productionStreamStorage) ValkeyReady(ctx context.Context) error {
	if storage == nil || storage.valkey == nil ||
		storage.valkey.Do(ctx, storage.valkey.B().Ping().Build()).Error() != nil {
		return errStreamDependencyUnavailable
	}
	return nil
}

func (storage *productionStreamStorage) Handler(kind streamHandlerKind) (streamrunner.Handler, error) {
	if storage == nil || storage.clickHouse == nil {
		return nil, errStreamDependencyUnavailable
	}
	switch kind {
	case internalIngestHandlerKind:
		return streamhandlers.NewInternalIngestHandler(storage.clickHouse)
	case productTelemetryHandlerKind:
		return streamhandlers.NewProductTelemetryHandler(storage.clickHouse)
	case externalIngestHandlerKind:
		if storage.recompute != nil {
			return nil, streamrunner.ErrInvalidConfig
		}
		repository, err := streamhandlers.NewPostgresExternalBatchRepository(storage.domainPool)
		if err != nil {
			return nil, err
		}
		sink, err := streamhandlers.NewClickHouseExternalBatchSink(storage.clickHouse)
		if err != nil {
			return nil, err
		}
		recomputeStore, err := externalrecompute.NewValkeyStore(storage.valkey)
		if err != nil {
			return nil, err
		}
		dispatcher, err := externalrecompute.NewPostgresCompatibilityDispatcher(storage.domainPool)
		if err != nil {
			return nil, err
		}
		storage.recompute, err = externalrecompute.New(
			recomputeStore,
			dispatcher,
			externalrecompute.DefaultConfig(),
		)
		if err != nil {
			return nil, err
		}
		return streamhandlers.NewExternalIngestHandler(repository, sink, storage.recompute)
	default:
		return nil, streamrunner.ErrInvalidConfig
	}
}

func (storage *productionStreamStorage) ControlComponents() []lifecycle.Component {
	if storage == nil || storage.recompute == nil {
		return nil
	}
	return []lifecycle.Component{storage.recompute}
}

func (storage *productionStreamStorage) NewTransport() (streamrunner.Transport, error) {
	if storage == nil {
		return nil, errStreamDependencyUnavailable
	}
	return streamrunner.NewSharedValkeyTransport(storage.valkey)
}

func (storage *productionStreamStorage) Close() {
	if storage == nil {
		return
	}
	if storage.valkey != nil {
		storage.valkey.Close()
	}
	if storage.clickHouse != nil {
		_ = storage.clickHouse.Close()
	}
	if storage.domainPool != nil {
		storage.domainPool.Close()
	}
}

type streamDependencySources struct {
	openStorage func(context.Context, config.Config) (streamStorage, error)
}

var productionStreamDependencySources = streamDependencySources{
	openStorage: openProductionStreamStorage,
}

func configureStreamRunnerDependencies(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
) ([]lifecycle.Component, error) {
	return configureStreamRunnerDependenciesWithSources(
		ctx,
		cfg,
		registry,
		productionStreamDependencySources,
	)
}

func configureStreamRunnerDependenciesWithSources(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	sources streamDependencySources,
) ([]lifecycle.Component, error) {
	if registry == nil || sources.openStorage == nil {
		return nil, errStreamDependencyUnavailable
	}
	storage, err := sources.openStorage(ctx, cfg)
	if err != nil || storage == nil {
		return nil, processreadiness.RegisterUnavailable(
			registry,
			"clickhouse",
			"domain_postgres",
			"stream_consumer",
			"valkey",
		)
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			storage.Close()
		}
	}()
	storageChecks := []struct {
		name  string
		check health.CheckFunc
	}{
		{name: "clickhouse", check: storage.ClickHouseReady},
		{name: "domain_postgres", check: storage.DomainPostgresReady},
		{name: "valkey", check: storage.ValkeyReady},
	}
	streamConsumerConfigured := false
	checks := append(storageChecks, struct {
		name  string
		check health.CheckFunc
	}{
		name: "stream_consumer", check: func(context.Context) error {
			if !streamConsumerConfigured {
				return errStreamDependencyUnavailable
			}
			return nil
		},
	})
	for _, check := range checks {
		if err := registry.RegisterRequired(check.name, check.check); err != nil {
			return nil, err
		}
	}
	bootstrapTimeout := cfg.HealthCheckTimeout
	if bootstrapTimeout <= 0 {
		bootstrapTimeout = 2 * time.Second
	}
	bootstrapContext, cancelBootstrap := context.WithTimeout(ctx, bootstrapTimeout)
	defer cancelBootstrap()
	for _, check := range storageChecks {
		if check.check(bootstrapContext) != nil {
			return nil, nil
		}
	}

	components := []lifecycle.Component{streamStorageLifecycle{storage: storage}}
	replicas := cfg.StreamConfiguredReplicas
	if replicas == 0 {
		replicas = 1
	}
	switch cfg.Profile {
	case "ingest":
		for _, specification := range []struct {
			kind   streamHandlerKind
			config streamrunner.Config
		}{
			{
				kind:   internalIngestHandlerKind,
				config: internalIngestRunnerConfig(replicas),
			},
			{
				kind:   productTelemetryHandlerKind,
				config: productTelemetryRunnerConfig(replicas),
			},
		} {
			runner, err := buildStreamRunner(storage, registry, specification.kind, specification.config)
			if err != nil {
				if errors.Is(err, streamrunner.ErrInvalidConfig) {
					return nil, err
				}
				return nil, nil
			}
			components = append(components, runner)
		}
	case "external":
		runner, err := buildStreamRunner(
			storage,
			registry,
			externalIngestHandlerKind,
			externalIngestRunnerConfig(replicas),
		)
		if err != nil {
			if errors.Is(err, streamrunner.ErrInvalidConfig) {
				return nil, err
			}
			return nil, nil
		}
		components = append(components, storage.ControlComponents()...)
		components = append(components, runner)
	default:
		return nil, streamrunner.ErrInvalidConfig
	}
	streamConsumerConfigured = true
	closeOnError = false
	return components, nil
}

func buildStreamRunner(
	storage streamStorage,
	registry *health.Registry,
	kind streamHandlerKind,
	cfg streamrunner.Config,
) (*streamrunner.Runner, error) {
	handler, err := storage.Handler(kind)
	if err != nil {
		return nil, err
	}
	transport, err := storage.NewTransport()
	if err != nil {
		return nil, err
	}
	runner, err := streamrunner.New(transport, handler, cfg, registry)
	if err != nil {
		transport.Close()
		return nil, err
	}
	return runner, nil
}

func internalIngestRunnerConfig(replicas int) streamrunner.Config {
	return streamrunner.Config{
		Name: "internal_ingest",
		Patterns: []string{
			"ingest:*:commits",
			"ingest:*:deployments",
			"ingest:*:incidents",
			"ingest:*:pull-requests",
			"ingest:*:work-items",
		},
		ConsumerGroup: "ingest-consumers", ConsumerName: "go-internal-ingest",
		BatchSize: 100, DiscoveryLimit: 10_000, Block: 5 * time.Second,
		ReclaimEvery: time.Minute, ReclaimIdle: 5 * time.Minute,
		MaxDeliveries: 5, ShutdownDrain: 10 * time.Second,
		ConfiguredReplicas: replicas,
	}
}

func productTelemetryRunnerConfig(replicas int) streamrunner.Config {
	return streamrunner.Config{
		Name: "product_telemetry", Patterns: []string{"product-telemetry:*:events"},
		ConsumerGroup: "product-telemetry-consumers", ConsumerName: "go-product-telemetry",
		BatchSize: 100, DiscoveryLimit: 10_000, Block: 5 * time.Second,
		ReclaimEvery: time.Minute, ReclaimIdle: 5 * time.Minute,
		MaxDeliveries: 5, ShutdownDrain: 10 * time.Second,
		ConfiguredReplicas: replicas,
	}
}

func externalIngestRunnerConfig(replicas int) streamrunner.Config {
	return streamrunner.Config{
		Name: "external_ingest", Patterns: []string{"external-ingest:*:batches"},
		ConsumerGroup: "external-ingest-consumers", ConsumerName: "go-external-ingest",
		BatchSize: 50, DiscoveryLimit: 10_000, Block: 5 * time.Second,
		ReclaimEvery: time.Minute, ReclaimIdle: 15 * time.Minute,
		MaxDeliveries: 5, ShutdownDrain: 20 * time.Second,
		Singleton: true, ConfiguredReplicas: replicas,
	}
}

type streamStorageLifecycle struct{ storage streamStorage }

func (streamStorageLifecycle) Name() string { return "stream-storage" }

func (component streamStorageLifecycle) Start(context.Context) error {
	if component.storage == nil {
		return errStreamDependencyUnavailable
	}
	return nil
}

func (component streamStorageLifecycle) Shutdown(context.Context) error {
	if component.storage != nil {
		component.storage.Close()
	}
	return nil
}
