package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// metricsQueue and its worker budget must match the deployment manifest entry
// for the heavy process; exact startup validation compares the two.
const (
	metricsQueue        = "metrics"
	metricsQueueWorkers = 2
)

type metricsWorkerComponent struct {
	client     *river.Client[pgx.Tx]
	clickhouse driver.Conn
}

func (component metricsWorkerComponent) Name() string { return "river-heavy-metrics-worker" }
func (component metricsWorkerComponent) Start(ctx context.Context) error {
	return component.client.Start(ctx)
}
func (component metricsWorkerComponent) Shutdown(ctx context.Context) error {
	if component.client == nil {
		return nil
	}
	result := component.client.Stop(ctx)
	if component.clickhouse != nil {
		if err := component.clickhouse.Close(); result == nil {
			result = err
		}
	}
	return result
}

func buildDailyWorker(
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
) (workerFamily, error) {
	if cfg.Profile != "heavy" || registry == nil {
		return workerFamily{}, nil
	}
	dailyKinds := []string{
		jobcontract.KindDailyMetricsDispatch,
		jobcontract.KindDailyMetricsPartition,
		jobcontract.KindDailyMetricsFinalize,
	}
	dailySpecs := make([]jobruntime.HandlerSpec, 0, len(dailyKinds))
	for _, kind := range dailyKinds {
		descriptor, ok := registry.Descriptor(kind)
		if !ok {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		if descriptor.Executable() {
			dailySpecs = append(dailySpecs, descriptor)
		}
	}
	if len(dailySpecs) != 0 && len(dailySpecs) != len(dailyKinds) {
		return workerFamily{}, errWorkerDependencyUnavailable
	}

	inventory, err := remaining.Load()
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	remainingSpecs := make([]jobruntime.HandlerSpec, 0, len(inventory.Families))
	remainingFamilies := make(map[string]remaining.Family, len(inventory.Families))
	for _, family := range inventory.Families {
		descriptor, ok := registry.Descriptor(family.RouteKey)
		if !ok || validateRemainingFamilyDescriptor(family, descriptor) != nil {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		remainingFamilies[family.RouteKey] = family
		if descriptor.Executable() {
			remainingSpecs = append(remainingSpecs, descriptor)
		}
	}
	if len(dailySpecs) == 0 && len(remainingSpecs) == 0 {
		return workerFamily{}, nil
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil || observer == nil || logger == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	baseURL := strings.TrimRight(cfg.OperationalBridgeURL, "/")
	idempotency, err := jobruntime.NewPostgresIdempotency(postgresDatabase.pools.Domain)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	dailyDependencies := jobruntime.Dependencies{
		Logger: logger, Observer: observer, TenantScope: operationalTenantScope{},
		Budget: newOperationalBudget(), Idempotency: idempotency,
	}
	workers := river.NewWorkers()
	registered := make([]jobruntime.HandlerSpec, 0, len(dailySpecs)+len(remainingSpecs))
	var metricsClickHouse driver.Conn
	if len(dailySpecs) > 0 {
		store, storeErr := daily.NewPostgresStore(postgresDatabase.pools.Domain)
		publisher, publisherErr := daily.NewPostgresPublisher(postgresDatabase.pools.Domain, registry)
		clickhouseConnection, clickhouseErr := clickhousestore.Open(
			context.Background(), clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
		)
		discoverer, discovererErr := daily.NewClickHouseRepositoryDiscoverer(clickhouseConnection)
		compatibility, compatibilityErr := daily.NewHTTPCompatibilityExecutor(
			metricCompatibilityHTTPClient(cfg.OperationalBridgeTimeout),
			daily.HTTPCompatibilityConfig{
				Endpoint:              baseURL + "/internal/worker/daily-metrics/v1/execute",
				BearerToken:           cfg.OperationalBridgeToken.Reveal(),
				AllowInsecureInternal: cfg.OperationalBridgeAllowInsecure,
			},
		)
		if storeErr != nil || publisherErr != nil || clickhouseErr != nil || discovererErr != nil || compatibilityErr != nil {
			if clickhouseConnection != nil {
				_ = clickhouseConnection.Close()
			}
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		metricsClickHouse = clickhouseConnection
		for _, spec := range dailySpecs {
			switch spec.Kind {
			case jobcontract.KindDailyMetricsDispatch:
				handler, handlerErr := daily.NewDispatcher(store, publisher, discoverer)
				if handlerErr != nil {
					_ = clickhouseConnection.Close()
					return workerFamily{}, errWorkerDependencyUnavailable
				}
				adapter, adapterErr := jobruntime.NewAdapter[jobruntime.DailyMetricsDispatchArgs](
					registry, spec, handler, dailyDependencies,
				)
				if adapterErr != nil || river.AddWorkerSafely(workers, adapter) != nil {
					_ = clickhouseConnection.Close()
					return workerFamily{}, errWorkerDependencyUnavailable
				}
				registered = append(registered, adapter.Spec())
			case jobcontract.KindDailyMetricsPartition:
				handler, handlerErr := daily.NewPartitionHandler(store, publisher, compatibility)
				if handlerErr != nil {
					_ = clickhouseConnection.Close()
					return workerFamily{}, errWorkerDependencyUnavailable
				}
				adapter, adapterErr := jobruntime.NewAdapter[jobruntime.DailyMetricsPartitionArgs](
					registry, spec, handler, dailyDependencies,
				)
				if adapterErr != nil || river.AddWorkerSafely(workers, adapter) != nil {
					_ = clickhouseConnection.Close()
					return workerFamily{}, errWorkerDependencyUnavailable
				}
				registered = append(registered, adapter.Spec())
			case jobcontract.KindDailyMetricsFinalize:
				handler, handlerErr := daily.NewFinalizeHandler(store, compatibility)
				if handlerErr != nil {
					_ = clickhouseConnection.Close()
					return workerFamily{}, errWorkerDependencyUnavailable
				}
				adapter, adapterErr := jobruntime.NewAdapter[jobruntime.DailyMetricsFinalizeArgs](
					registry, spec, handler, dailyDependencies,
				)
				if adapterErr != nil || river.AddWorkerSafely(workers, adapter) != nil {
					_ = clickhouseConnection.Close()
					return workerFamily{}, errWorkerDependencyUnavailable
				}
				registered = append(registered, adapter.Spec())
			}
		}
	}

	if len(remainingSpecs) > 0 {
		store, storeErr := remaining.NewPostgresStore(postgresDatabase.pools.Domain)
		compatibility, compatibilityErr := remaining.NewHTTPCompatibilityExecutor(
			metricCompatibilityHTTPClient(cfg.OperationalBridgeTimeout),
			remaining.HTTPCompatibilityConfig{
				Endpoint:              baseURL + "/internal/worker/remaining-metrics/v1/execute",
				BearerToken:           cfg.OperationalBridgeToken.Reveal(),
				AllowInsecureInternal: cfg.OperationalBridgeAllowInsecure,
			},
		)
		budget, budgetErr := remaining.NewBudget(inventory)
		if storeErr != nil || compatibilityErr != nil || budgetErr != nil {
			if metricsClickHouse != nil {
				_ = metricsClickHouse.Close()
			}
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		dependencies := jobruntime.Dependencies{
			Logger: logger, Observer: observer, TenantScope: operationalTenantScope{},
			Budget: budget, Idempotency: idempotency,
		}
		for _, spec := range remainingSpecs {
			family := remainingFamilies[spec.Kind]
			var registeredSpec jobruntime.HandlerSpec
			var registrationErr error
			switch spec.Kind {
			case jobcontract.KindRemainingCapacity:
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingCapacityArgs](
					workers, registry, spec, store, compatibility, dependencies, family.Name,
				)
			case jobcontract.KindRemainingComplexity:
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingComplexityArgs](
					workers, registry, spec, store, compatibility, dependencies, family.Name,
				)
			case jobcontract.KindRemainingDORA:
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingDORAArgs](
					workers, registry, spec, store, compatibility, dependencies, family.Name,
				)
			case jobcontract.KindRemainingExtraMetrics:
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingExtraMetricsArgs](
					workers, registry, spec, store, compatibility, dependencies, family.Name,
				)
			case jobcontract.KindRemainingMembership:
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingMembershipArgs](
					workers, registry, spec, store, compatibility, dependencies, family.Name,
				)
			case jobcontract.KindRemainingRecommendations:
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingRecommendationsArgs](
					workers, registry, spec, store, compatibility, dependencies, family.Name,
				)
			case jobcontract.KindRemainingReleaseImpact:
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingReleaseImpactArgs](
					workers, registry, spec, store, compatibility, dependencies, family.Name,
				)
			case jobcontract.KindRemainingTeamMetrics:
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingTeamMetricsArgs](
					workers, registry, spec, store, compatibility, dependencies, family.Name,
				)
			default:
				registrationErr = errWorkerDependencyUnavailable
			}
			if registrationErr != nil {
				if metricsClickHouse != nil {
					_ = metricsClickHouse.Close()
				}
				return workerFamily{}, errWorkerDependencyUnavailable
			}
			registered = append(registered, registeredSpec)
		}
	}
	if err := registerRescueCoverage(workers, registry, registered); err != nil {
		if metricsClickHouse != nil {
			_ = metricsClickHouse.Close()
		}
		return workerFamily{}, errWorkerDependencyUnavailable
	}

	client, err := river.NewClient(
		riverpgxv5.New(postgresDatabase.pools.QueueControl),
		&river.Config{
			Logger: logger,
			Queues: map[string]river.QueueConfig{
				metricsQueue: {MaxWorkers: metricsQueueWorkers},
			},
			Schema:  cfg.RiverDatabaseSchema,
			Workers: workers,
		},
	)
	if err != nil {
		if metricsClickHouse != nil {
			_ = metricsClickHouse.Close()
		}
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	return workerFamily{
		component: metricsWorkerComponent{client: client, clickhouse: metricsClickHouse},
		handlers:  registered,
		queues:    []jobruntime.QueueBudget{{Queue: metricsQueue, MaxWorkers: metricsQueueWorkers}},
	}, nil
}

func contractDeadlineHTTPClient(connectTimeout time.Duration) *http.Client {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		transport = &http.Transport{}
	} else {
		transport = transport.Clone()
	}
	dialer := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
	transport.DialContext = dialer.DialContext
	transport.TLSHandshakeTimeout = connectTimeout
	// Do not set Client.Timeout or ResponseHeaderTimeout: compatibility APIs
	// return headers only after the effect finishes, so the descriptor-derived
	// River context is the authoritative whole-request deadline.
	return &http.Client{Transport: transport}
}

func metricCompatibilityHTTPClient(connectTimeout time.Duration) *http.Client {
	return contractDeadlineHTTPClient(connectTimeout)
}

func validateRemainingFamilyDescriptor(
	family remaining.Family,
	descriptor jobruntime.Descriptor,
) error {
	if descriptor.Kind != family.RouteKey || descriptor.Profile != family.Profile ||
		descriptor.Queue != "metrics" ||
		descriptor.ConcurrencyScope != "organization" ||
		descriptor.ConcurrencyLimit != family.MaxConcurrency ||
		descriptor.Idempotency != "remaining_metrics_partition" ||
		descriptor.DomainLink != "remaining_metric_partition" ||
		descriptor.OrganizationScope != "tenant" ||
		descriptor.Route != family.Route ||
		descriptor.RollbackRoute != family.RollbackRoute ||
		descriptor.Executable() != family.Executable() {
		return errWorkerDependencyUnavailable
	}
	return nil
}

func addRemainingWorker[T jobruntime.ContractArgs](
	workers *river.Workers,
	registry *jobruntime.Registry,
	spec jobruntime.HandlerSpec,
	store remaining.Store,
	compatibility remaining.CompatibilityExecutor,
	dependencies jobruntime.Dependencies,
	family string,
) (jobruntime.HandlerSpec, error) {
	handler, err := remaining.NewPartitionHandler[T](store, compatibility, family)
	if err != nil {
		return jobruntime.HandlerSpec{}, err
	}
	adapter, err := jobruntime.NewAdapter[T](registry, spec, handler, dependencies)
	if err != nil || river.AddWorkerSafely(workers, adapter) != nil {
		return jobruntime.HandlerSpec{}, errWorkerDependencyUnavailable
	}
	return adapter.Spec(), nil
}
