package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/riverqueue/river"
)

const metricsQueue = "metrics"

func buildDailyWorker(
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
	workers *river.Workers,
) (workerFamily, error) {
	if !queueSelected(cfg.Queues, metricsQueue) || registry == nil {
		return workerFamily{}, nil
	}
	if workers == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
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
	idempotency, err := newOperationalIdempotency(postgresDatabase.pools.Domain, observer)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	dailyDependencies := jobruntime.Dependencies{
		Logger: logger, Observer: observer, TenantScope: operationalTenantScope{},
		Budget: newOperationalBudget(postgresDatabase.pools.Domain, observer), Idempotency: idempotency,
	}
	registered := make([]jobruntime.HandlerSpec, 0, len(dailySpecs)+len(remainingSpecs))
	var metricsClickHouse driver.Conn
	if len(dailySpecs) > 0 {
		// The daily store reports lease encounters directly: generic middleware
		// cannot tell a claim that parked for a live lease from one that found
		// nothing to do, and only the former means a run may be stalling.
		var leaseObservers []jobruntime.DailyMetricsLeaseObserver
		if leaseObserver, ok := observer.(jobruntime.DailyMetricsLeaseObserver); ok {
			leaseObservers = append(leaseObservers, leaseObserver)
		}
		store, storeErr := daily.NewPostgresStore(postgresDatabase.pools.Domain, leaseObservers...)
		if discoveryObserver, ok := observer.(jobruntime.DailyMetricsDiscoveryObserver); ok {
			store.SetDiscoveryObserver(discoveryObserver)
		}
		if redriveObserver, ok := observer.(jobruntime.DailyMetricsRedriveObserver); ok {
			store.SetRedriveObserver(redriveObserver)
		}
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
				// CHAOS-4319: wires the terminal ambiguous_refused counter.
				// Optional, like every other observer here: telemetry must
				// never gate the durable failed_permanent write itself.
				if compatRetryObserver, ok := observer.(jobruntime.DailyMetricsCompatRetryObserver); ok {
					handler.SetCompatRetryObserver(compatRetryObserver)
				}
				// CHAOS-4316: work-size-derived backstop ceiling on the
				// compatibility bridge call, behind the bridge's own
				// progress-based watchdog. See SetLivenessCeiling's doc
				// comment for why this is a backstop, not the primary bound.
				handler.SetLivenessCeiling(
					cfg.DailyPartitionLivenessCeilingBase,
					cfg.DailyPartitionLivenessCeilingPerRepo,
				)
				// Optional (CHAOS-4263): a partition whose source data exists
				// but whose family output is empty must not report success.
				// Both dependencies are already validated non-nil above, so
				// this only fails defensively; a failure here degrades to no
				// check rather than blocking daily-metrics startup.
				if sourceChecker, sourceCheckerErr := daily.NewClickHouseSourceDataChecker(
					postgresDatabase.pools.Domain, clickhouseConnection,
				); sourceCheckerErr == nil {
					handler.SetSourceDataChecker(sourceChecker)
					if zeroRowsObserver, ok := observer.(jobruntime.DailyMetricsZeroRowsObserver); ok {
						handler.SetZeroRowsObserver(zeroRowsObserver)
					}
				}
				// CHAOS-4276/CHAOS-4275: team_wellbeing and repo_user_commit
				// are the daily bridge's first two native families. UNLIKE
				// dora/capacity's per-KIND refusal below (which takes a
				// whole River kind out of service), a native FAMILY
				// construction failure here simply leaves that ONE family
				// off the native map: the daily_partition kind still
				// registers, and the Python compatibility bridge still
				// computes that family for every partition, exactly as it
				// did before its executor existed. One family degrading
				// must not take the other 22 daily families down with it.
				//
				// SetNativeFamilies REPLACES its map on every call (it does
				// not merge), so every native family must be accumulated
				// into ONE map and registered in a single call.
				nativeFamilies := map[string]daily.NativeFamilyExecutor{}
				if teamWellbeingExecutor, teamWellbeingErr := daily.NewTeamWellbeingExecutor(clickhouseConnection); teamWellbeingErr == nil {
					nativeFamilies["team_wellbeing"] = teamWellbeingExecutor
					// CHAOS-4329: per-team repo fan-out telemetry -- optional,
					// same fail-open discipline as the native family observer
					// registered below once every native family is accumulated.
					if repoCountObserver, ok := observer.(jobruntime.TeamMetricsDailyRepoCountObserver); ok {
						teamWellbeingExecutor.SetRepoCountObserver(repoCountObserver)
					}
				} else {
					logger.Error(
						"team_wellbeing native executor refused; the family "+
							"stays on the Python compatibility bridge for "+
							"every partition. Every other daily-metrics "+
							"family is unaffected.",
						"error", teamWellbeingErr,
					)
				}
				if repoUserCommitExecutor, repoUserCommitErr := daily.NewRepoUserCommitExecutor(clickhouseConnection); repoUserCommitErr == nil {
					nativeFamilies["repo_user_commit"] = repoUserCommitExecutor
				} else {
					logger.Error(
						"repo_user_commit native executor refused; the family "+
							"stays on the Python compatibility bridge for "+
							"every partition. Every other daily-metrics "+
							"family is unaffected.",
						"error", repoUserCommitErr,
					)
				}
				if len(nativeFamilies) > 0 {
					handler.SetNativeFamilies(nativeFamilies)
					if nativeObserver, ok := observer.(jobruntime.DailyMetricsNativeFamilyObserver); ok {
						handler.SetNativeFamilyObserver(nativeObserver)
					}
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
		// The remaining-metrics store reports a release-lost lease directly, the
		// same way the daily store does above (CHAOS-4002): generic middleware
		// cannot tell that outcome apart from an ordinary release.
		var remainingLeaseObservers []jobruntime.RemainingMetricsLeaseObserver
		if leaseObserver, ok := observer.(jobruntime.RemainingMetricsLeaseObserver); ok {
			remainingLeaseObservers = append(remainingLeaseObservers, leaseObserver)
		}
		store, storeErr := remaining.NewPostgresStore(postgresDatabase.pools.Domain, remainingLeaseObservers...)
		// CHAOS-4384's OpenDayZeroRowObserver is NOT wired on this store: it
		// only ever backs PartitionHandler's Claim/CompletePartition here,
		// never StartRunTx -- see sync_dispatch.go's remainingStore, the
		// store post-sync's dora coverage check actually runs through.
		var compatibilityObserver remaining.CompatibilityObserver
		if candidate, ok := observer.(remaining.CompatibilityObserver); ok {
			compatibilityObserver = candidate
		}
		compatibility, compatibilityErr := remaining.NewHTTPCompatibilityExecutor(
			metricCompatibilityHTTPClient(cfg.OperationalBridgeTimeout),
			remaining.HTTPCompatibilityConfig{
				Endpoint:              baseURL + "/internal/worker/remaining-metrics/v1/execute",
				BearerToken:           cfg.OperationalBridgeToken.Reveal(),
				AllowInsecureInternal: cfg.OperationalBridgeAllowInsecure,
				Logger:                logger,
				Observer:              compatibilityObserver,
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

		// CHAOS-3092 R1: the dora kind computes natively instead of posting to
		// the compatibility bridge. The swap is per KIND and wholesale -- there
		// is no environment switch and no fallback, so a worker that cannot
		// build the native executor does not serve dora at all rather than
		// quietly reverting to Python. Every other kind keeps the bridge until
		// it has its own parity proof.
		//
		// THE REFUSAL IS SCOPED TO THIS KIND, NOT THE FAMILY. An earlier
		// version returned from buildDailyWorker on this error, which left
		// capacity, complexity, membership, recommendations and release-impact
		// unregistered even though their own dependencies were healthy -- and
		// made a transient ClickHouse inspection failure enough to down six
		// working kinds. That is fail-closed aimed at the wrong target: the
		// thing that must not happen is DORA computing wrong numbers, not the
		// rest of the family going dark alongside it.
		//
		// Not registering dora is still fail-closed. Its partitions go
		// unclaimed, which is a backlog rather than wrong data -- and because
		// an unmoving metric is indistinguishable from a quiet day, the
		// refusal emits a POSITIVE signal (worker_dora_native_refused_total,
		// labelled by reason) next to an error log, so an alert has something
		// to bind to.
		var doraExecutor *remaining.DORAExecutor
		if slices.ContainsFunc(remainingSpecs, func(spec jobruntime.HandlerSpec) bool {
			return spec.Kind == jobcontract.KindRemainingDORA
		}) {
			// The daily block above opens this connection only when daily
			// specs are present. A worker serving remaining kinds WITHOUT
			// daily would otherwise reach the native executor with a nil
			// connection and refuse the whole family -- a regression for a
			// topology that works today.
			if metricsClickHouse == nil {
				connection, connectionErr := clickhousestore.Open(
					context.Background(),
					clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
				)
				if connectionErr != nil {
					return workerFamily{}, errWorkerDependencyUnavailable
				}
				metricsClickHouse = connection
			}
			var doraObserver remaining.DORAObserver
			if candidate, ok := observer.(remaining.DORAObserver); ok {
				doraObserver = candidate
			}
			executor, executorErr := remaining.NewDORAExecutor(context.Background(), metricsClickHouse, doraObserver)
			if executorErr != nil {
				logger.Error(
					"dora native executor refused; the dora kind will not be "+
						"served and its partitions will accumulate unclaimed. "+
						"Every other remaining kind is unaffected.",
					"error", executorErr,
					"reason", doraRefusalReason(executorErr),
				)
				if refusalObserver, ok := observer.(doraRefusalObserver); ok {
					_ = refusalObserver.ObserveDORARefused(doraRefusalReason(executorErr))
				}
			} else {
				doraExecutor = executor
			}
		}

		// CUT-20 R2: the capacity kind computes natively too. Same per-kind
		// discipline as dora -- a refusal takes THIS KIND out of service and
		// leaves its siblings registered, with a positive signal rather than a
		// metric that merely stops moving.
		var capacityExecutor *remaining.CapacityExecutor
		if slices.ContainsFunc(remainingSpecs, func(spec jobruntime.HandlerSpec) bool {
			return spec.Kind == jobcontract.KindRemainingCapacity
		}) {
			if metricsClickHouse == nil {
				connection, connectionErr := clickhousestore.Open(
					context.Background(),
					clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
				)
				if connectionErr != nil {
					return workerFamily{}, errWorkerDependencyUnavailable
				}
				metricsClickHouse = connection
			}
			var capacityObserver remaining.CapacityObserver
			if candidate, ok := observer.(remaining.CapacityObserver); ok {
				capacityObserver = candidate
			}
			executor, executorErr := remaining.NewCapacityExecutor(
				context.Background(), metricsClickHouse, capacityObserver)
			if executorErr != nil {
				logger.Error(
					"capacity native executor refused; the capacity kind will "+
						"not be served and its partitions will accumulate "+
						"unclaimed. Every other remaining kind is unaffected.",
					"error", executorErr,
				)
				if refusalObserver, ok := observer.(capacityRefusalObserver); ok {
					_ = refusalObserver.ObserveCapacityRefused(
						capacityRefusalReason(executorErr))
				}
			} else {
				capacityExecutor = executor
			}
		}

		for _, spec := range remainingSpecs {
			family := remainingFamilies[spec.Kind]
			var registeredSpec jobruntime.HandlerSpec
			var registrationErr error
			switch spec.Kind {
			case jobcontract.KindRemainingCapacity:
				if capacityExecutor == nil {
					// Refused above. Skip rather than register a handler around
					// a nil executor, which would claim partitions and fail
					// each one.
					continue
				}
				// Native, not `compatibility` -- this is the R2 cutover.
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingCapacityArgs](
					workers, registry, spec, store, capacityExecutor, dependencies, family.Name,
				)
			case jobcontract.KindRemainingComplexity:
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingComplexityArgs](
					workers, registry, spec, store, compatibility, dependencies, family.Name,
				)
			case jobcontract.KindRemainingDORA:
				if doraExecutor == nil {
					// The native executor refused above. SKIP the kind rather
					// than registering a handler around a nil executor, which
					// would claim partitions and then fail each one -- turning
					// a clean "not served" into a retry loop that looks like
					// flapping. The refusal was already logged and counted.
					continue
				}
				// Native, not `compatibility` -- this is the R1 cutover.
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingDORAArgs](
					workers, registry, spec, store, doraExecutor, dependencies, family.Name,
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

	var cleanups []func() error
	if metricsClickHouse != nil {
		cleanups = append(cleanups, metricsClickHouse.Close)
	}
	return workerFamily{
		handlers: registered,
		queues: selectedQueueBudgets(
			cfg.Queues, []string{metricsQueue}, cfg.WorkerQueueConcurrency,
		),
		cleanups: cleanups,
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
	if descriptor.Kind != family.RouteKey || descriptor.Queue != "metrics" ||
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

// doraRefusalObserver is the narrow capability the dora cutover needs to make
// a refusal visible. Kept as a local interface so a collector that does not
// implement it degrades to log-only rather than failing the build.
type doraRefusalObserver interface {
	ObserveDORARefused(reason string) error
}

// doraRefusalReason maps a construction error onto the closed label set.
//
// The reasons are distinguished because they call for different operator
// actions: a mismatch means finish or roll back a migration, unparseable means
// fix the environment variable, an unknown schema means something changed the
// table outside the migration chain, and inspect_failed means look at
// ClickHouse itself. Collapsing them into one counter would report that DORA is
// down without saying which of those four to go and do.
func doraRefusalReason(err error) string {
	switch {
	case errors.Is(err, remaining.ErrOrderingContractMismatch):
		return jobruntime.DORARefusedOrderingContractMismatch
	case errors.Is(err, remaining.ErrOrderingContractUnparseable):
		return jobruntime.DORARefusedContractUnparseable
	case errors.Is(err, remaining.ErrOrderingContractUnknownSchema):
		return jobruntime.DORARefusedUnknownSchema
	default:
		return jobruntime.DORARefusedInspectFailed
	}
}

// capacityRefusalObserver is the narrow capability the capacity cutover needs
// to make a refusal visible, kept local so a collector without it degrades to
// log-only rather than failing the build.
type capacityRefusalObserver interface {
	ObserveCapacityRefused(reason string) error
}

// capacityRefusalReason maps a construction error onto the closed label set.
//
// The two are distinguished because they call for different actions: an
// incompatible schema means finish or roll back a migration, while a failed
// inspection means look at ClickHouse itself.
func capacityRefusalReason(err error) string {
	if errors.Is(err, remaining.ErrCapacitySchemaIncompatible) {
		return jobruntime.CapacityRefusedSchemaIncompatible
	}
	return jobruntime.CapacityRefusedInspectFailed
}
