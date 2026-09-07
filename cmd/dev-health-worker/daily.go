package main

import (
	"context"
	"errors"
	"fmt"
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

// metricsCollectorFromObserver resolves the concrete *jobruntime.MetricsCollector
// out of an observer, whether it IS one directly (a test double, typically) or
// wraps one (production's claimLivenessObserver, which embeds the collector but
// does not satisfy an exact-type assertion against it directly -- embedding
// promotes methods, not concrete type identity; see
// claim_liveness_test.go's TestClaimLivenessObserverUnwrapReturnsTheEmbeddedCollector
// for the proof). Mirrors provider_sync.go's identical fallback.
//
// Round-2 codex finding, #2177 (CHAOS-4282): the membership wiring used to do
// the bare assertion inline with no fallback, so it silently never matched in
// production -- every membership run/prune-failure counter was dead despite
// passing every unit test (those construct *jobruntime.MetricsCollector
// directly, never wrapped). Round-2 codex finding, #2173 (same class, this
// PR): the recommendations readiness wiring had the identical bare-assertion
// gap. work_item_attribution (CHAOS-3092 PR-B) uses this helper from the
// start rather than risk being the third instance found the hard way.
// Extracted here so the resolution logic itself is unit-testable without
// constructing the rest of buildDailyWorker's dependencies.
func metricsCollectorFromObserver(observer jobruntime.Observer) *jobruntime.MetricsCollector {
	if collector, ok := observer.(*jobruntime.MetricsCollector); ok {
		return collector
	}
	if unwrapper, ok := observer.(interface {
		Unwrap() *jobruntime.MetricsCollector
	}); ok {
		return unwrapper.Unwrap()
	}
	return nil
}

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
		if finalizeSweepObserver, ok := observer.(jobruntime.DailyMetricsFinalizeSweepObserver); ok {
			store.SetFinalizeSweepObserver(finalizeSweepObserver)
		}
		if finalizeRedriveObserver, ok := observer.(jobruntime.DailyMetricsFinalizeRedriveObserver); ok {
			store.SetFinalizeRedriveObserver(finalizeRedriveObserver)
		}
		publisher, publisherErr := daily.NewPostgresPublisher(postgresDatabase.pools.Domain, registry)
		clickhouseConnection, clickhouseErr := clickhousestore.Open(
			context.Background(), clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
		)
		discoverer, discovererErr := daily.NewClickHouseRepositoryDiscoverer(clickhouseConnection)
		if storeErr != nil || publisherErr != nil || clickhouseErr != nil || discovererErr != nil {
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
				// CHAOS-5040: the fan-out is the only genuinely periodic,
				// per-organization thing in this family, so it is where the
				// blocked-run marker is kept current.
				if blockedObserver, ok := observer.(jobruntime.DailyMetricsBlockedRunObserver); ok {
					handler.SetBlockedRunObserver(blockedObserver)
				}
				// Said once, at boot, not counted per pass. A store without
				// the capability is a deployment-constant wiring fact, but it
				// silently disables the marker -- so if a future refactor
				// swaps the store, the boot log is where that becomes visible.
				if !daily.SupportsBlockedRunReconcile(store) {
					logger.Warn("blocked-run reconcile disabled: store does not implement it")
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
				handler, handlerErr := daily.NewPartitionHandler(store, publisher)
				if handlerErr != nil {
					_ = clickhouseConnection.Close()
					return workerFamily{}, errWorkerDependencyUnavailable
				}
				// Wires the counter for a partition released 'failed'
				// rather than completed (CHAOS-4319's original
				// ambiguous_refused disposition is gone with the bridge --
				// CHAOS-3092 PR-A -- leaving the two native-family-
				// incomplete ones). Optional, like every other observer
				// here: telemetry must never gate the durable write itself.
				if compatRetryObserver, ok := observer.(jobruntime.DailyMetricsCompatRetryObserver); ok {
					handler.SetCompatRetryObserver(compatRetryObserver)
				}
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
				// CHAOS-3092 (PR-A): a partition-scope native FAMILY that
				// cannot be constructed is a STARTUP ERROR naming the
				// family and its error, not a family quietly left off the
				// map. The old policy ("one family degrading must not take
				// the other 22 down with it") was only defensible while the
				// Python compatibility bridge still computed the missing
				// family for every partition. That bridge is deleted: an
				// unregistered family now means its rows are never written
				// by anyone, silently, for the worker's whole process
				// lifetime. Failing construction is the only honest
				// response -- the same rule the families.json run-order
				// failure below already follows.
				//
				// SetNativeFamilies REPLACES its map on every call (it does
				// not merge), so every native family must be accumulated
				// into ONE map and registered in a single call.
				//
				// CHAOS-4292 rebase-gate class fix: construction lives in
				// dailyNativeFamilyRegistrations, a pure function, so its
				// return values are what the two setter calls below
				// receive DIRECTLY -- no local variable sits between
				// construction and use for a stray delete()/reassignment to
				// silently undo before SetNativeFamilies/
				// SetPostBridgeNativeFamilies ever see it (codex found and
				// executed exactly that construction against the prior
				// shape: `delete(nativeFamilies, "incident")` inserted
				// before the setter call left every drift test green). The
				// test that matters now calls dailyNativeFamilyRegistrations
				// directly and asserts on its actual return value, not on
				// source text.
				nativeFamilies, postBridgeFamilies, _, familyRefusals := dailyNativeFamilyRegistrations(store, clickhouseConnection, observer, logger)
				if len(familyRefusals) > 0 {
					refusedNames := make([]string, 0, len(familyRefusals))
					for _, refusal := range familyRefusals {
						refusedNames = append(refusedNames, refusal.family)
						logger.Error(
							"native daily family executor could not be constructed; "+
								"CHAOS-3092 deleted the Python compatibility bridge, so "+
								"there is nothing left to compute this family -- failing "+
								"worker construction rather than starting with a family "+
								"that would silently never be written",
							"family", refusal.family,
							"error", refusal.err,
						)
					}
					_ = clickhouseConnection.Close()
					return workerFamily{}, fmt.Errorf(
						"%w: native daily family construction refused: %s",
						errWorkerDependencyUnavailable, strings.Join(refusedNames, ","),
					)
				}
				if len(nativeFamilies) > 0 || len(postBridgeFamilies) > 0 {
					if nativeObserver, ok := observer.(jobruntime.DailyMetricsNativeFamilyObserver); ok {
						handler.SetNativeFamilyObserver(nativeObserver)
					}
					// CHAOS-5139: the per-partition refusal counter alone
					// never says WHY a native family failed -- CHAOS-5138
					// hit exactly this, cicd's runtime error was
					// unrecoverable from any CI artifact. *slog.Logger
					// satisfies daily.NativeFamilyRefusalLogger directly
					// (Error + Info).
					handler.SetNativeFamilyLogger(logger)
				}
				// CHAOS-3092 (PR-A) observability gate: say ONCE, at boot,
				// exactly which native daily families this process
				// constructed. With no bridge behind them, that list IS the
				// set of families this worker can produce at all -- a
				// shrinking list is the single most important thing an
				// operator can notice, and it must not have to be inferred
				// from the absence of per-family counters.
				constructedFamilies := make([]string, 0, len(nativeFamilies)+len(postBridgeFamilies))
				for name := range nativeFamilies {
					constructedFamilies = append(constructedFamilies, name)
				}
				for name := range postBridgeFamilies {
					constructedFamilies = append(constructedFamilies, name)
				}
				slices.Sort(constructedFamilies)
				logger.Info(
					"native daily metrics families constructed",
					"count", len(constructedFamilies),
					"families", constructedFamilies,
					"pre_bridge_count", len(nativeFamilies),
					"post_bridge_count", len(postBridgeFamilies),
				)
				// CHAOS-5078 codex r1 F3 fix: an `after` ORDERING failure
				// (a genuine graph cycle, or a `after` name that does not
				// exist in families.json) is a CONSTRUCTION-time contract
				// defect, not a data condition to degrade around at
				// runtime -- ErrFamilyOrderCycle/ErrFamilyOrderUnknown's own
				// doc comments say exactly this ("failing loudly at startup
				// is the only honest response"). Logging and continuing
				// contradicted that: it left the daily worker family
				// registered and running with EVERY native family silently
				// diverted to the Python bridge, which is a much harder
				// condition to notice in production than a construction
				// failure that fails CI/deploy immediately. Mirrors the
				// adapter/river-registration failures a few lines below,
				// which already fail construction the same way.
				if len(nativeFamilies) > 0 {
					if err := handler.SetNativeFamilies(nativeFamilies); err != nil {
						logger.Error(
							"native daily families NOT registered: families.json "+
								"run order could not be derived; failing worker "+
								"construction rather than starting with no native "+
								"family registered at all",
							"error", err,
						)
						_ = clickhouseConnection.Close()
						return workerFamily{}, fmt.Errorf("%w: %v", errWorkerDependencyUnavailable, err)
					}
				}
				if len(postBridgeFamilies) > 0 {
					if err := handler.SetPostBridgeNativeFamilies(postBridgeFamilies); err != nil {
						logger.Error(
							"post_bridge daily families NOT registered: families.json "+
								"run order could not be derived; failing worker "+
								"construction rather than starting with no post_bridge "+
								"family registered at all",
							"error", err,
						)
						_ = clickhouseConnection.Close()
						return workerFamily{}, fmt.Errorf("%w: %v", errWorkerDependencyUnavailable, err)
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
				handler, handlerErr := daily.NewFinalizeHandler(store)
				if handlerErr != nil {
					_ = clickhouseConnection.Close()
					return workerFamily{}, errWorkerDependencyUnavailable
				}
				// CHAOS-4290: RUN-scoped native families. dailyNativeFamilyRegistrations
				// is a pure function (see the partition case's comment on why the
				// drift test calls it directly), so calling it again here is safe
				// and keeps each case owning the maps it actually registers --
				// the partition arm's locals are out of scope in this one.
				// Registering an empty map is a no-op, so a build with no finalize
				// executor behaves exactly as before this capability existed.
				// CHAOS-5141: team_cognitive_load is constructed inside
				// dailyNativeFamilyRegistrations alongside ic_finalize (see
				// that function) so both land in ONE finalizeFamilies map and
				// go through ONE SetNativeFinalizeFamilies call -- the setter
				// validates every name in the map together against
				// pythonRecognisedFinalizeFamilies in a single pass.
				_, _, finalizeFamilies, _ := dailyNativeFamilyRegistrations(store, clickhouseConnection, observer, logger)
				handler.SetNativeFinalizeFamilies(finalizeFamilies)
				// Same fail-open discipline as the partition path: telemetry
				// never gates, but a fail-open path with no counter cannot be
				// distinguished from one that is working.
				if nativeObserver, ok := observer.(jobruntime.DailyMetricsNativeFamilyObserver); ok {
					handler.SetNativeFinalizeFamilyObserver(nativeObserver)
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
		//
		// CHAOS-4291 deleted the HTTP compatibility-bridge executor this
		// block used to construct here: complexity was the last
		// remaining-metrics kind still served through it (see
		// remaining.ComplexityExecutor's own doc), and every kind below now
		// constructs and refuses its own native executor independently.
		budget, budgetErr := remaining.NewBudget(inventory)
		if storeErr != nil || budgetErr != nil {
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
			executor, executorErr := remaining.NewDORAExecutor(context.Background(), metricsClickHouse, doraObserver, logger)
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
				context.Background(), metricsClickHouse, capacityObserver, logger)
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

		// CHAOS-4291: the complexity kind computes natively too -- the last
		// remaining kind still served through the HTTP compatibility bridge
		// (see remaining.ComplexityExecutor's own doc). Same per-kind
		// discipline as dora/capacity above -- a refusal takes THIS KIND out
		// of service and leaves its siblings registered, with a positive
		// signal rather than a metric that merely stops moving.
		var complexityExecutor *remaining.ComplexityExecutor
		if slices.ContainsFunc(remainingSpecs, func(spec jobruntime.HandlerSpec) bool {
			return spec.Kind == jobcontract.KindRemainingComplexity
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
			var complexityObserver remaining.ComplexityObserver
			if candidate, ok := observer.(remaining.ComplexityObserver); ok {
				complexityObserver = candidate
			}
			executor, executorErr := remaining.NewComplexityExecutor(
				context.Background(), metricsClickHouse, cfg.WorkerRemainingComplexityConfigPath, complexityObserver,
			)
			if executorErr != nil {
				logger.Error(
					"complexity native executor refused; the complexity kind "+
						"will not be served and its partitions will accumulate "+
						"unclaimed. Every other remaining kind is unaffected.",
					"error", executorErr,
					"reason", complexityRefusalReason(executorErr),
				)
				if refusalObserver, ok := observer.(complexityRefusalObserver); ok {
					_ = refusalObserver.ObserveComplexityRefused(complexityRefusalReason(executorErr))
				}
			} else {
				complexityExecutor = executor
			}
		}

		// CHAOS-3092: the recommendations kind computes natively too. Same
		// per-kind discipline as dora/capacity above -- a refusal takes THIS
		// KIND out of service and leaves its siblings registered, with a
		// positive signal rather than a metric that merely stops moving.
		var recommendationsExecutor *remaining.RecommendationsExecutor
		if slices.ContainsFunc(remainingSpecs, func(spec jobruntime.HandlerSpec) bool {
			return spec.Kind == jobcontract.KindRemainingRecommendations
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
			// orgID "" -- the executor is a family-wide singleton, not bound to
			// one org. ComputePartition always supplies the real org per call,
			// and LoadTeamMetricsWindow re-scopes the loader to it (see
			// recommendations_loader.go), so the constructor's own orgID never
			// gates a live query.
			executor, executorErr := remaining.NewRecommendationsExecutor(
				context.Background(), metricsClickHouse, postgresDatabase.pools.Domain, "",
			)
			if executorErr != nil {
				logger.Error(
					"recommendations native executor refused; the recommendations "+
						"kind will not be served and its partitions will accumulate "+
						"unclaimed. Every other remaining kind is unaffected.",
					"error", executorErr,
					"reason", recommendationsRefusalReason(executorErr),
				)
				if refusalObserver, ok := observer.(recommendationsRefusalObserver); ok {
					_ = refusalObserver.ObserveRecommendationsRefused(
						recommendationsRefusalReason(executorErr))
				}
			} else {
				if collector := metricsCollectorFromObserver(observer); collector != nil {
					executor.SetReadinessObserver(
						remaining.CollectorReadinessObserver{Collector: collector})
				}
				executor.SetReadinessLogger(logger)
				recommendationsExecutor = executor
			}
		}

		// CHAOS-4282: the membership-backfill kind computes natively too --
		// no-LLM, projected from theme/subcategory distributions the
		// (still-Python) LLM materializer already persisted. Same per-kind
		// refusal discipline as dora/capacity above.
		var membershipExecutor *remaining.MembershipExecutor
		if slices.ContainsFunc(remainingSpecs, func(spec jobruntime.HandlerSpec) bool {
			return spec.Kind == jobcontract.KindRemainingMembership
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
			membershipWriter, membershipWriterErr := remaining.NewMembershipClickHouseWriter(metricsClickHouse)
			var executor *remaining.MembershipExecutor
			var executorErr error
			if membershipWriterErr != nil {
				executorErr = membershipWriterErr
			} else {
				executor, executorErr = remaining.NewMembershipExecutor(
					context.Background(), metricsClickHouse, membershipWriter,
				)
			}
			if executorErr != nil {
				logger.Error(
					"membership-backfill native executor refused; the kind will "+
						"not be served and its partitions will accumulate "+
						"unclaimed. Every other remaining kind is unaffected.",
					"error", executorErr,
					"reason", membershipRefusalReason(executorErr),
				)
				if refusalObserver, ok := observer.(membershipRefusalObserver); ok {
					_ = refusalObserver.ObserveMembershipRefused(membershipRefusalReason(executorErr))
				}
			} else {
				if collector := metricsCollectorFromObserver(observer); collector != nil {
					executor.SetObserver(remaining.CollectorMembershipObserver{Collector: collector})
				}
				executor.SetLogger(logger)
				membershipExecutor = executor
			}
		}

		// CHAOS-3092 PR-B: the work_item_attribution backstop kind computes
		// natively too -- the staleness-window backstop for the sync-time
		// deriver's incremental watermark, scoped to the affected
		// repo/project/org (see WorkItemAttributionExecutor.ComputeOrg's doc
		// comment). Same per-kind refusal discipline as dora/capacity above.
		var workItemAttributionExecutor *remaining.WorkItemAttributionExecutor
		if slices.ContainsFunc(remainingSpecs, func(spec jobruntime.HandlerSpec) bool {
			return spec.Kind == jobcontract.KindRemainingWorkItemAttribution
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
			workItemAttributionWriter, workItemAttributionWriterErr := remaining.NewWorkItemAttributionClickHouseWriter(metricsClickHouse)
			var executor *remaining.WorkItemAttributionExecutor
			var executorErr error
			if workItemAttributionWriterErr != nil {
				executorErr = workItemAttributionWriterErr
			} else {
				executor, executorErr = remaining.NewWorkItemAttributionExecutor(
					context.Background(), metricsClickHouse, workItemAttributionWriter,
				)
			}
			if executorErr != nil {
				logger.Error(
					"work item attribution backstop native executor refused; "+
						"the kind will not be served and its partitions will "+
						"accumulate unclaimed. Every other remaining kind is "+
						"unaffected.",
					"error", executorErr,
					"reason", workItemAttributionRefusalReason(executorErr),
				)
				if refusalObserver, ok := observer.(workItemAttributionRefusalObserver); ok {
					_ = refusalObserver.ObserveWorkItemAttributionRefused(
						workItemAttributionRefusalReason(executorErr))
				}
			} else {
				if collector := metricsCollectorFromObserver(observer); collector != nil {
					executor.SetObserver(remaining.CollectorWorkItemAttributionObserver{Collector: collector})
				}
				executor.SetLogger(logger)
				workItemAttributionExecutor = executor
			}
		}

		// CHAOS-4296: the release_impact kind computes natively too. Same
		// per-kind discipline as dora/capacity/recommendations above -- a
		// refusal takes THIS KIND out of service and leaves its siblings
		// registered, with an error log rather than a metric that merely
		// stops moving.
		var releaseImpactExecutor *remaining.ReleaseImpactExecutor
		if slices.ContainsFunc(remainingSpecs, func(spec jobruntime.HandlerSpec) bool {
			return spec.Kind == jobcontract.KindRemainingReleaseImpact
		}) {
			if metricsClickHouse == nil {
				connection, connectionErr := clickhousestore.Open(
					context.Background(),
					clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
				)
				if connectionErr != nil {
					// codex r2 (CHAOS-4296/#2262): connectionErr was checked
					// but never logged -- the caller saw only the generic
					// errWorkerDependencyUnavailable sentinel, with no way to
					// tell a DNS failure from a bad password from a refused
					// connection. This exact discard is pre-existing at every
					// other clickhousestore.Open call site in this function
					// (dora/capacity/work_item_attribution/etc.), tracked
					// fleet-wide as CHAOS-5102's sibling gap and out of scope
					// to fix everywhere in this PR; fixed here because this
					// call site is this PR's own diff.
					logger.Error("release_impact ClickHouse connection failed",
						"error", connectionErr)
					return workerFamily{}, errWorkerDependencyUnavailable
				}
				metricsClickHouse = connection
			}
			var releaseImpactObserver remaining.ReleaseImpactObserver
			if candidate, ok := observer.(remaining.ReleaseImpactObserver); ok {
				releaseImpactObserver = candidate
			}
			executor, executorErr := remaining.NewReleaseImpactExecutor(
				context.Background(), metricsClickHouse, releaseImpactObserver, logger)
			if executorErr != nil {
				logger.Error(
					"release impact native executor refused; the release_impact "+
						"kind will not be served and its partitions will "+
						"accumulate unclaimed. Every other remaining kind is "+
						"unaffected.",
					"error", executorErr,
				)
			} else {
				releaseImpactExecutor = executor
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
				if complexityExecutor == nil {
					// Refused above. Skip rather than register a handler
					// around a nil executor, which would claim partitions
					// and fail each one.
					continue
				}
				// Native, not `compatibility` -- this is the CHAOS-4291
				// cutover.
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingComplexityArgs](
					workers, registry, spec, store, complexityExecutor, dependencies, family.Name,
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
				if membershipExecutor == nil {
					// Refused above. Skip rather than register a handler
					// around a nil executor, which would claim partitions
					// and fail each one.
					continue
				}
				// Native, not `compatibility` -- this is the CHAOS-4282 cutover.
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingMembershipArgs](
					workers, registry, spec, store, membershipExecutor, dependencies, family.Name,
				)
			case jobcontract.KindRemainingRecommendations:
				if recommendationsExecutor == nil {
					// Refused above. Skip rather than register a handler around
					// a nil executor, which would claim partitions and fail
					// each one.
					continue
				}
				// Native, not `compatibility` -- this is the CHAOS-3092 cutover.
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingRecommendationsArgs](
					workers, registry, spec, store, recommendationsExecutor, dependencies, family.Name,
				)
			case jobcontract.KindRemainingReleaseImpact:
				if releaseImpactExecutor == nil {
					// The native executor refused above (or was never
					// constructed because no release_impact spec is being
					// served). Skip rather than register a handler around a
					// nil executor, which would claim partitions and then
					// fail each one -- turning a clean "not served" into a
					// retry loop that looks like flapping.
					continue
				}
				// Native, not `compatibility` -- this is the CHAOS-4296 cutover.
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingReleaseImpactArgs](
					workers, registry, spec, store, releaseImpactExecutor, dependencies, family.Name,
				)
			case jobcontract.KindRemainingWorkItemAttribution:
				if workItemAttributionExecutor == nil {
					// Refused above. Skip rather than register a handler
					// around a nil executor, which would claim partitions
					// and fail each one -- same discipline as dora/capacity.
					continue
				}
				registeredSpec, registrationErr = addRemainingWorker[jobruntime.RemainingWorkItemAttributionArgs](
					workers, registry, spec, store, workItemAttributionExecutor, dependencies, family.Name,
				)
			default:
				registrationErr = errWorkerDependencyUnavailable
			}
			if registrationErr != nil {
				if metricsClickHouse != nil {
					_ = metricsClickHouse.Close()
				}
				logger.Error("remaining kind registration failed", "kind", spec.Kind, "error", registrationErr)
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

// dailyFamilyRefusal names one partition-scope daily family whose native
// executor could not be constructed, with the constructor's own error.
// CHAOS-3092 (PR-A): with the Python compatibility bridge deleted there is
// nothing left for such a family to fall open to, so this is a startup
// error rather than a log line -- see dailyNativeFamilyRegistrations.
type dailyFamilyRefusal struct {
	family string
	err    error
}

// dailyNativeFamilyRegistrations builds the native and post-bridge family
// maps buildDailyWorker registers with the partition handler. Extracted as
// a pure function (CHAOS-4292 rebase-gate class fix) precisely so its
// return values -- the actual maps SetNativeFamilies/
// SetPostBridgeNativeFamilies receive -- can be asserted on directly in a
// test, rather than approximated by parsing source text for assignment
// statements a later delete()/reassignment could silently undo before the
// setter calls ever run. buildDailyWorker passes clickhouseConnection
// (opened once, kept open for the worker's lifetime -- this function never
// closes it), observer, and logger straight through; every family's
// construction/fail-open/optional-observer-wiring logic is unchanged from
// before this extraction, only relocated.
//
// store (CHAOS-5194) is the first Postgres dependency this function has
// needed: BenchmarkingFinalizeExecutor verifies its own partition-completion
// barrier against daily_metrics_partitions, which lives in Postgres, not
// ClickHouse.
//
// CHAOS-3092 (PR-A): a PARTITION-scope family (native/postBridge) that
// cannot be constructed is no longer left quietly off the map. It used to
// be, because "the Python compatibility bridge still computes that family
// for every partition" -- that bridge is deleted, so an unregistered family
// means its rows are never written by anyone, for every partition, for the
// worker's whole process lifetime. Every refusal is returned in `refusals`
// and buildDailyWorker turns it into a startup error naming the family and
// its error: fail fast, never fall open. FINALIZE-scope refusals keep their
// existing policy (logged here, then failed loudly per run with
// ErrFinalizeFamilyIncomplete -- CHAOS-3092 PR-A'), since that path already
// has no fallback and its own loud failure.
func dailyNativeFamilyRegistrations(
	store daily.Store,
	clickhouseConnection driver.Conn,
	observer jobruntime.Observer,
	logger *slog.Logger,
) (
	native map[string]daily.NativeFamilyExecutor,
	postBridge map[string]daily.NativeFamilyExecutor,
	finalize map[string]daily.NativeFinalizeFamilyExecutor,
	refusals []dailyFamilyRefusal,
) {
	native = map[string]daily.NativeFamilyExecutor{}
	// CHAOS-4290: RUN-scoped families. Kept in their own map because
	// FinalizeHandler registers them through SetNativeFinalizeFamilies -- a
	// finalize family placed in either partition map would run once per
	// PARTITION rather than once per run.
	finalize = map[string]daily.NativeFinalizeFamilyExecutor{}
	if clickhouseConnection != nil {
		finalize[daily.ICFinalizeFamilyName] = daily.NewICFinalizeExecutor(clickhouseConnection)
	}
	// CHAOS-5141: team_cognitive_load is a finalize-scope native family that
	// reads user_metrics_daily, itself populated earlier in the SAME run by
	// ic_finalize -- co-registration is asserted at construction:
	// team_cognitive_load registers natively ONLY when ic_finalize is ALSO
	// registered natively in this finalize map, never independently. Same
	// fail-open construction policy as every native family below -- CHAOS-3092
	// PR-A' deleted the finalize compatibility bridge, so a refusal here no
	// longer falls open to it: the family is left unregistered, and
	// FinalizeHandler.Work fails the run loudly with ErrFinalizeFamilyIncomplete
	// for every day this construction refuses. Registration is separate from
	// construction: the caller's single SetNativeFinalizeFamilies call
	// validates every name in this map against pythonRecognisedFinalizeFamilies
	// and fails loudly (not silently) on an unrecognised name -- see that
	// setter's doc comment.
	if _, icFinalizeNative := finalize[daily.ICFinalizeFamilyName]; icFinalizeNative {
		if teamCognitiveLoadExecutor, teamCognitiveLoadErr := daily.NewTeamCognitiveLoadExecutor(clickhouseConnection); teamCognitiveLoadErr == nil {
			finalize[daily.TeamCognitiveLoadFamilyName] = teamCognitiveLoadExecutor
		} else {
			logger.Error(
				"team_cognitive_load native finalize family refused; "+
					"the family has no registered executor and every "+
					"finalize run will fail loud "+
					"(ErrFinalizeFamilyIncomplete) until this is fixed. "+
					"Every other daily-metrics family is unaffected.",
				"error", teamCognitiveLoadErr,
			)
		}
	} else {
		logger.Error(
			"team_cognitive_load native finalize family refused: " +
				"ic_finalize is not registered natively in this run " +
				"(co-registration required); team_cognitive_load has no " +
				"registered executor and every finalize run will fail " +
				"loud (ErrFinalizeFamilyIncomplete) until this is fixed.",
		)
	}
	// CHAOS-5051: team_complexity is also a finalize-scope native family
	// (RUN-scoped, written exactly once per org/day from
	// run_daily_metrics_finalize), but unlike team_cognitive_load it has NO
	// co-registration dependency on ic_finalize or team_cognitive_load --
	// its only input is repo_complexity_daily, populated by the complexity
	// scan job on its own cadence, never by another finalize family in this
	// same run. So it constructs independently. Same fail-open discipline as
	// every other native family here: CHAOS-3092 PR-A' deleted the finalize
	// compatibility bridge, so a refusal leaves team_complexity unregistered
	// and every finalize run fails loud (ErrFinalizeFamilyIncomplete).
	if teamComplexityExecutor, teamComplexityErr := daily.NewTeamComplexityExecutor(clickhouseConnection); teamComplexityErr == nil {
		finalize[daily.TeamComplexityFamilyName] = teamComplexityExecutor
	} else {
		logger.Error(
			"team_complexity native finalize family refused; "+
				"the family has no registered executor and every "+
				"finalize run will fail loud "+
				"(ErrFinalizeFamilyIncomplete) until this is fixed. "+
				"Every other daily-metrics family is unaffected.",
			"error", teamComplexityErr,
		)
	}
	// CHAOS-5194: benchmarking, relocated from post_bridge (see the removed
	// registration below, and BenchmarkingFinalizeExecutor's own doc comment
	// for why). No co-registration dependency on ic_finalize above: this
	// family reads repo_metrics_daily/testops_*/dora tables written by
	// partition-scope families, never ic_finalize's user_metrics_daily output.
	if benchmarkingFinalizeExecutor, benchmarkingFinalizeErr := daily.NewBenchmarkingFinalizeExecutor(
		store, clickhouseConnection, logger,
	); benchmarkingFinalizeErr == nil {
		finalize[daily.BenchmarkingFamilyName] = benchmarkingFinalizeExecutor
	} else {
		logger.Error(
			"benchmarking finalize native executor refused; the family "+
				"has no registered executor and every finalize run will "+
				"fail loud (ErrFinalizeFamilyIncomplete) until this is "+
				"fixed. Every other daily-metrics family is unaffected.",
			"error", benchmarkingFinalizeErr,
		)
	}
	// CHAOS-5084: compounding_risk_team has no co-registration dependency --
	// unlike team_cognitive_load it reads repo_metrics_daily, written by the
	// PARTITION-scope compounding_risk/repo_user_commit families earlier in
	// this same run, not by ic_finalize -- so it registers unconditionally on
	// a healthy connection, the same policy ic_finalize itself uses above.
	if compoundingRiskTeamExecutor, compoundingRiskTeamErr := daily.NewCompoundingRiskTeamExecutor(clickhouseConnection); compoundingRiskTeamErr == nil {
		finalize[daily.CompoundingRiskTeamFamilyName] = compoundingRiskTeamExecutor
	} else {
		logger.Error(
			"compounding_risk_team native finalize family refused; the "+
				"family has no registered executor and every finalize "+
				"run will fail loud (ErrFinalizeFamilyIncomplete) until "+
				"this is fixed. Every other daily-metrics family is "+
				"unaffected.",
			"error", compoundingRiskTeamErr,
		)
	}
	if teamWellbeingExecutor, teamWellbeingErr := daily.NewTeamWellbeingExecutor(clickhouseConnection); teamWellbeingErr == nil {
		native["team_wellbeing"] = teamWellbeingExecutor
		// CHAOS-4329: per-team repo fan-out telemetry -- optional,
		// same fail-open discipline as the native family observer
		// registered by the caller once every native family is accumulated.
		if repoCountObserver, ok := observer.(jobruntime.TeamMetricsDailyRepoCountObserver); ok {
			teamWellbeingExecutor.SetRepoCountObserver(repoCountObserver)
		}
	} else {
		if nativeFamilyObserver, ok := observer.(jobruntime.DailyMetricsNativeFamilyObserver); ok {
			_ = nativeFamilyObserver.ObserveDailyMetricsNativeFamily(
				"team_wellbeing", jobruntime.DailyMetricsNativeFamilyOutcomeRefused, 0, 0,
			)
		}
		refusals = append(refusals, dailyFamilyRefusal{family: "team_wellbeing", err: teamWellbeingErr})
	}
	if repoUserCommitExecutor, repoUserCommitErr := daily.NewRepoUserCommitExecutor(clickhouseConnection); repoUserCommitErr == nil {
		native["repo_user_commit"] = repoUserCommitExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "repo_user_commit", err: repoUserCommitErr})
	}
	// CHAOS-4269/CHAOS-4295: incident carries a FIX, not just a
	// port -- the Python compatibility bridge for this family is
	// permanently zero-yield (active_incidents_query's
	// valid_from predicate has no NULL-OK guard, and
	// map_issue_incidents never sets valid_from). A refused
	// executor here means the family STAYS BROKEN for every
	// partition until the worker restarts with a healthy
	// connection, not merely "computed a little slower" the way
	// a refused team_wellbeing/repo_user_commit executor does --
	// still fail-open by the same standing policy (one family
	// degrading must never take another down), but worth this
	// note for whoever reads this log line during an incident.
	if incidentExecutor, incidentErr := daily.NewIncidentExecutor(clickhouseConnection); incidentErr == nil {
		if guardObserver, ok := observer.(jobruntime.IncidentValidFromGuardObserver); ok {
			incidentExecutor.SetValidFromGuardObserver(guardObserver)
		}
		native["incident"] = incidentExecutor
	} else {
		if nativeFamilyObserver, ok := observer.(jobruntime.DailyMetricsNativeFamilyObserver); ok {
			_ = nativeFamilyObserver.ObserveDailyMetricsNativeFamily(
				"incident", jobruntime.DailyMetricsNativeFamilyOutcomeRefused, 0, 0,
			)
		}
		refusals = append(refusals, dailyFamilyRefusal{family: "incident", err: incidentErr})
	}
	// CHAOS-4293: deploy is the daily bridge's fourth native family.
	if deployExecutor, deployErr := daily.NewDeployExecutor(clickhouseConnection); deployErr == nil {
		native["deploy"] = deployExecutor
	} else {
		if nativeFamilyObserver, ok := observer.(jobruntime.DailyMetricsNativeFamilyObserver); ok {
			_ = nativeFamilyObserver.ObserveDailyMetricsNativeFamily(
				"deploy", jobruntime.DailyMetricsNativeFamilyOutcomeRefused, 0, 0,
			)
		}
		refusals = append(refusals, dailyFamilyRefusal{family: "deploy", err: deployErr})
	}
	if cicdExecutor, cicdErr := daily.NewCICDExecutor(clickhouseConnection); cicdErr == nil {
		native["cicd"] = cicdExecutor
	} else {
		if nativeFamilyObserver, ok := observer.(jobruntime.DailyMetricsNativeFamilyObserver); ok {
			_ = nativeFamilyObserver.ObserveDailyMetricsNativeFamily(
				"cicd", jobruntime.DailyMetricsNativeFamilyOutcomeRefused, 0, 0,
			)
		}
		refusals = append(refusals, dailyFamilyRefusal{family: "cicd", err: cicdErr})
	}
	// CHAOS-4277: file_hotspots and file_risk_hotspots are
	// the third pair of families to leave the Python
	// compatibility bridge -- registered as TWO independent
	// NativeFamilyExecutors (one per families.json entry,
	// one per output table) so a construction or runtime
	// failure in one never takes the other down with it,
	// same discipline as every native family above.
	if fileHotspotsExecutor, fileHotspotsErr := daily.NewFileHotspotsExecutor(clickhouseConnection); fileHotspotsErr == nil {
		native["file_hotspots"] = fileHotspotsExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "file_hotspots", err: fileHotspotsErr})
	}
	if fileRiskHotspotsExecutor, fileRiskHotspotsErr := daily.NewFileRiskHotspotsExecutor(clickhouseConnection); fileRiskHotspotsErr == nil {
		native["file_risk_hotspots"] = fileRiskHotspotsExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "file_risk_hotspots", err: fileRiskHotspotsErr})
	}
	// CHAOS-4285: ai_governance. Same fail-open construction policy as every
	// other native family above, and registered PRE-BRIDGE like them: this
	// family reads only raw sync tables (ai_attribution_resolved,
	// git_pull_requests, ci_pipeline_runs, security_alerts,
	// ai_tool_allowlist), never another compat family's daily output, so it
	// has none of work_item_state's post_bridge ordering dependency.
	//
	// Unlike every other entry here it is ORG-scoped, not repo-scoped --
	// see AIGovernanceExecutor's doc comment for why that matches Python
	// exactly and why the deterministic event_id is what makes the
	// resulting once-per-partition rewrite idempotent.
	if aiGovernanceExecutor, aiGovernanceErr := daily.NewAIGovernanceExecutor(clickhouseConnection); aiGovernanceErr == nil {
		native["ai_governance"] = aiGovernanceExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "ai_governance", err: aiGovernanceErr})
	}
	// CHAOS-4280: ai_impact. Same fail-open construction policy, and
	// PRE-BRIDGE like the rest: it reads only raw sync tables plus the
	// incident family's own reader, never another compat family's daily
	// output. Repo-scoped, unlike ai_governance above.
	if aiImpactExecutor, aiImpactErr := daily.NewAIImpactExecutor(clickhouseConnection); aiImpactErr == nil {
		native["ai_impact"] = aiImpactExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "ai_impact", err: aiImpactErr})
	}
	// CHAOS-4286: work_graph_edges. Same fail-open construction policy, and
	// PRE-BRIDGE: it reads raw sync tables plus the shared incident
	// projection (LoadIncidentsStarted), never another compat family's daily
	// output.
	//
	// The "auto" argument is the daily job's `provider`, which discover_repos
	// falls back to when a repo's own provider column is empty or literally
	// "unknown" (job_daily.py:194). It is NOT a guess: the compatibility
	// bridge passes no provider at all, so the Python side runs on
	// discover_repos' own default, which is "auto" (job_daily.py:127).
	// Consequence worth knowing when reading rows: "auto" is a real, expected
	// value in these tables' provider columns.
	if workGraphEdgesExecutor, workGraphEdgesErr := daily.NewWorkGraphEdgesExecutor(clickhouseConnection, "auto"); workGraphEdgesErr == nil {
		native["work_graph_edges"] = workGraphEdgesExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "work_graph_edges", err: workGraphEdgesErr})
	}
	// CHAOS-4280 part B / CHAOS-4286 part B: ai_workflow. Same fail-open
	// construction policy, and PRE-BRIDGE: it reads git_pull_requests and
	// work_graph_issue_pr, never another compat family's daily output.
	// Shares the SAME "auto" job-provider convention as work_graph_edges
	// above for the identical reason (discover_repos' own default).
	if aiWorkflowExecutor, aiWorkflowErr := daily.NewAIWorkflowExecutor(clickhouseConnection, "auto"); aiWorkflowErr == nil {
		native["ai_workflow"] = aiWorkflowExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "ai_workflow", err: aiWorkflowErr})
	}
	// CHAOS-4294: testops_risk. Same fail-open construction policy as
	// every other native family above.
	if testopsRiskExecutor, testopsRiskErr := daily.NewTestopsRiskExecutor(clickhouseConnection); testopsRiskErr == nil {
		native["testops_risk"] = testopsRiskExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "testops_risk", err: testopsRiskErr})
	}
	// CHAOS-5078: work_item_attribution is now NATIVE, and it is the reason the
	// three families below can return to pre_bridge. It WRITES
	// work_item_team_attributions; work_item, work_item_estimate and
	// work_item_state READ it. families.json declares that with
	// `"after":["work_item_attribution"]` on each reader, and FamilyRunOrder
	// turns those edges into the execution order -- so the writer runs first
	// WITHIN the pre_bridge phase, which is what made the post_bridge
	// workaround unnecessary.
	//
	// Moving them back is a resilience IMPROVEMENT, not merely tidying:
	// skipFamiliesForBridge adds pre_bridge names only for families that
	// actually RAN, but post_bridge names UNCONDITIONALLY. Under post_bridge a
	// refused executor meant nobody wrote the family for that partition; under
	// pre_bridge a refusal falls back to the Python bridge again.
	if workItemAttributionExecutor, workItemAttributionErr := daily.NewWorkItemAttributionExecutor(clickhouseConnection); workItemAttributionErr == nil {
		native["work_item_attribution"] = workItemAttributionExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "work_item_attribution", err: workItemAttributionErr})
	}
	// CHAOS-4278/CHAOS-5078: work_item_state reads its team attribution from
	// work_item_team_attributions.is_primary=1 rather than recomputing the
	// 9-source cascade -- see WorkItemStateExecutor's and
	// LoadWorkItemPrimaryTeamAttributions's doc comments.
	//
	// It shipped pre_bridge, was moved to POST_BRIDGE by codex round-1 P1
	// (2026-09-01) because work_item_team_attributions was written by the
	// still-Python work_item_attribution family in the SAME partition call, so
	// a pre_bridge read saw the PREVIOUS run's rows -- and is now back in the
	// native (pre_bridge) map above, because that writer is native as of
	// CHAOS-5078 and families.json's `after` edges order it ahead of its
	// readers within the phase. The condition the old comment named
	// ("once CHAOS-4283 ports work_item_attribution to Go") is the condition
	// that has now been met.
	//
	// postBridge stays declared and returned, EMPTY of these three families
	// (benchmarking/compounding_risk below still populate it for their own,
	// different reasons). The phase itself is not removed: it is a working
	// mechanism with a real use (a native family whose correctness depends on
	// a still-Python family's same-partition write), and the next family to
	// need it should find it here rather than re-deriving it.
	//
	// CHAOS-4284: testops_pipeline / testops_test / testops_coverage.
	// Registered as THREE separate families, matching families.json, so a
	// failure in one leaves only that one on the Python bridge -- see
	// TestopsPipelineExecutor's doc comment for why they are not one
	// executor. Same fail-open construction policy as every family above.
	if testopsPipelineExecutor, testopsPipelineErr := daily.NewTestopsPipelineExecutor(clickhouseConnection); testopsPipelineErr == nil {
		native["testops_pipeline"] = testopsPipelineExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "testops_pipeline", err: testopsPipelineErr})
	}
	if testopsTestExecutor, testopsTestErr := daily.NewTestopsTestExecutor(clickhouseConnection); testopsTestErr == nil {
		native["testops_test"] = testopsTestExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "testops_test", err: testopsTestErr})
	}
	if testopsCoverageExecutor, testopsCoverageErr := daily.NewTestopsCoverageExecutor(clickhouseConnection); testopsCoverageErr == nil {
		native["testops_coverage"] = testopsCoverageExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "testops_coverage", err: testopsCoverageErr})
	}
	// CHAOS-4279: review_edges reads only RAW SYNC tables
	// (git_pull_requests, git_pull_request_reviews), written by
	// the provider sync path rather than by any daily family, so
	// it has no same-partition write-ordering dependency and
	// registers PRE-bridge like cicd -- unlike compounding_risk
	// below, whose input repo_metrics_daily is written by
	// repo_user_commit in the same partition.
	if reviewEdgesExecutor, reviewEdgesErr := daily.NewReviewEdgesExecutor(clickhouseConnection); reviewEdgesErr == nil {
		native["review_edges"] = reviewEdgesExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "review_edges", err: reviewEdgesErr})
	}

	postBridge = map[string]daily.NativeFamilyExecutor{}
	// CHAOS-4288/CHAOS-5194: benchmarking used to register HERE, post_bridge,
	// via an anchor-partition trick (one partition per org/day computed it,
	// every other partition no-op'd). That fixed the duplication class
	// (#2235/#2259) but not a race astra's F3 finding identified: the anchor
	// partition's own post_bridge phase could complete before every OTHER
	// partition for the same org/day had written its own cross-repo inputs.
	// Relocated to FINALIZE scope (see dailyNativeFamilyRegistrations' finalize
	// map below, and BenchmarkingFinalizeExecutor's own doc comment) --
	// finalize is claimed only after every partition succeeds
	// (ClaimFinalize's own barrier), which removes both the anchor mechanism
	// and the race in the same move.

	// CHAOS-4287: compounding_risk reads repo_metrics_daily, which
	// repo_user_commit writes in the SAME partition. repo_user_commit
	// is native and pre_bridge, but computeNativeFamilies walks
	// nativeFamilyNames in SORTED order and "compounding_risk" sorts
	// BEFORE "repo_user_commit" -- registering here in the native map
	// above would read the table before this partition's rows were
	// written. When repo_user_commit's own executor refuses, Python
	// writes repo_metrics_daily during the bridge call, later still.
	// post_bridge is the only phase after BOTH. Same situation and
	// same precedent as work_item_state below/above (CHAOS-4278,
	// where a pre_bridge placement reading stale data was a codex
	// round-1 P1). families.json declares "phase":"post_bridge" for
	// this family for the same reason. REPO scope only -- team-scope
	// rows come from run_daily_metrics_finalize and stay Python until
	// a finalize-side native-family hook exists; see
	// CompoundingRiskExecutor's doc comment.
	if compoundingRiskExecutor, compoundingRiskErr := daily.NewCompoundingRiskExecutor(clickhouseConnection); compoundingRiskErr == nil {
		postBridge["compounding_risk"] = compoundingRiskExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "compounding_risk", err: compoundingRiskErr})
	}
	if workItemStateExecutor, workItemStateErr := daily.NewWorkItemStateExecutor(clickhouseConnection); workItemStateErr == nil {
		native["work_item_state"] = workItemStateExecutor
		// CHAOS-4278: the team-attribution-READ guard counter --
		// see WorkItemStateMissingAttributionObserver's doc
		// comment. Same optional-observer discipline as
		// team_wellbeing's repoCountObserver above.
		if missingAttributionObserver, ok := observer.(jobruntime.WorkItemStateMissingAttributionObserver); ok {
			workItemStateExecutor.SetMissingAttributionObserver(missingAttributionObserver)
		}
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "work_item_state", err: workItemStateErr})
	}

	// CHAOS-4283: work_item and work_item_estimate read the SAME
	// work_item_team_attributions rows work_item_state reads, for the
	// same reason and with the same loader -- so they carry the same
	// "phase":"post_bridge" declaration and register here, not in the
	// native map above. Registered as TWO independent executors, one
	// per families.json entry, so a construction or runtime failure in
	// one never takes the other down (the file_hotspots/
	// file_risk_hotspots discipline, CHAOS-4277).
	//
	// FAIL-OPEN CAVEAT, inherent to post_bridge and worth restating: a
	// refusal here is NOT the same as a pre_bridge refusal. Python was
	// already told to skip these families for this partition
	// (skipFamiliesForBridge adds every post_bridge NAME
	// unconditionally), so a failure means NOBODY writes this family's
	// rows for this partition -- there is no bridge fallback. The
	// DailyMetricsNativeFamilyOutcomeRefused counter is the
	// operator-visible signal.
	if workItemExecutor, workItemErr := daily.NewWorkItemExecutor(clickhouseConnection); workItemErr == nil {
		native["work_item"] = workItemExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "work_item", err: workItemErr})
	}
	if workItemEstimateExecutor, workItemEstimateErr := daily.NewWorkItemEstimateExecutor(clickhouseConnection); workItemEstimateErr == nil {
		native["work_item_estimate"] = workItemEstimateExecutor
	} else {
		refusals = append(refusals, dailyFamilyRefusal{family: "work_item_estimate", err: workItemEstimateErr})
	}
	return native, postBridge, finalize, refusals
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
	executor remaining.PartitionExecutor,
	dependencies jobruntime.Dependencies,
	family string,
) (jobruntime.HandlerSpec, error) {
	handler, err := remaining.NewPartitionHandler[T](store, executor, family)
	if err != nil {
		return jobruntime.HandlerSpec{}, err
	}
	adapter, err := jobruntime.NewAdapter[T](registry, spec, handler, dependencies)
	if err != nil {
		return jobruntime.HandlerSpec{}, err
	}
	if addErr := river.AddWorkerSafely(workers, adapter); addErr != nil {
		return jobruntime.HandlerSpec{}, addErr
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

// complexityRefusalObserver is the narrow capability the complexity cutover
// needs to make a refusal visible, kept local so a collector without it
// degrades to log-only rather than failing the build.
type complexityRefusalObserver interface {
	ObserveComplexityRefused(reason string) error
}

// complexityRefusalReason maps a construction error onto the closed label
// set. The two are distinguished because they call for different actions: an
// unavailable ClickHouse connection (including an unreadable complexity.yaml)
// is transient and self-heals, an incompatible schema means finish or roll
// back a migration.
func complexityRefusalReason(err error) string {
	if errors.Is(err, remaining.ErrComplexitySchemaIncompatible) {
		return jobruntime.ComplexityRefusedSchemaIncompatible
	}
	return jobruntime.ComplexityRefusedUnavailable
}

// workItemAttributionRefusalObserver is the narrow capability the
// work_item_attribution backstop cutover needs to make a refusal visible,
// kept local so a collector without it degrades to log-only rather than
// failing the build.
type workItemAttributionRefusalObserver interface {
	ObserveWorkItemAttributionRefused(reason string) error
}

// workItemAttributionRefusalReason maps a construction error onto the closed
// label set. The three are distinguished because they call for different
// actions: an unavailable ClickHouse connection is transient and self-heals,
// a missing writer is a wiring bug in THIS file (never a database fault),
// and an incompatible schema means finish or roll back a migration.
func workItemAttributionRefusalReason(err error) string {
	switch {
	case errors.Is(err, remaining.ErrWorkItemAttributionUnavailable):
		return jobruntime.WorkItemAttributionRefusedUnavailable
	case errors.Is(err, remaining.ErrWorkItemAttributionWriterUnavailable):
		return jobruntime.WorkItemAttributionRefusedWriterUnavailable
	case errors.Is(err, remaining.ErrWorkItemAttributionSchemaIncompatible):
		return jobruntime.WorkItemAttributionRefusedSchemaIncompatible
	default:
		return jobruntime.WorkItemAttributionRefusedInspectFailed
	}
}

// recommendationsRefusalObserver is the narrow capability the recommendations
// cutover needs to make a refusal visible, kept local so a collector without
// it degrades to log-only rather than failing the build.
type recommendationsRefusalObserver interface {
	ObserveRecommendationsRefused(reason string) error
}

// recommendationsRefusalReason maps a construction error onto the closed label
// set. The four are distinguished because they call for different actions: an
// unavailable ClickHouse connection is transient and self-heals, a missing
// Postgres pool means the worker itself was wired without the store the
// readiness gate reads, an incompatible schema means finish or roll back a
// migration, and anything else means look at ClickHouse itself.
func recommendationsRefusalReason(err error) string {
	switch {
	case errors.Is(err, remaining.ErrRecommendationsUnavailable):
		return jobruntime.RecommendationsRefusedUnavailable
	case errors.Is(err, remaining.ErrRecommendationsPostgresUnavailable):
		return jobruntime.RecommendationsRefusedPostgresUnavailable
	case errors.Is(err, remaining.ErrRecommendationsSchemaIncompatible):
		return jobruntime.RecommendationsRefusedSchemaIncompatible
	default:
		return jobruntime.RecommendationsRefusedInspectFailed
	}
}

// membershipRefusalObserver is the narrow capability the membership-backfill
// cutover needs to make a refusal visible, kept local so a collector without
// it degrades to log-only rather than failing the build.
type membershipRefusalObserver interface {
	ObserveMembershipRefused(reason string) error
}

// membershipRefusalReason maps a construction error onto the closed label
// set. The three are distinguished because they call for different actions:
// an unavailable ClickHouse connection is transient and self-heals, a missing
// writer is a wiring bug in THIS file (never a database fault), an
// incompatible schema means finish or roll back a migration, and anything
// else means look at ClickHouse itself.
func membershipRefusalReason(err error) string {
	switch {
	case errors.Is(err, remaining.ErrMembershipUnavailable):
		return jobruntime.MembershipRefusedUnavailable
	case errors.Is(err, remaining.ErrMembershipWriterUnavailable):
		return jobruntime.MembershipRefusedWriterUnavailable
	case errors.Is(err, remaining.ErrMembershipSchemaIncompatible):
		return jobruntime.MembershipRefusedSchemaIncompatible
	default:
		return jobruntime.MembershipRefusedInspectFailed
	}
}
