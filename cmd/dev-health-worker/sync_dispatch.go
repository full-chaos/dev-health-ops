package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
)

type dailyPostSyncWriter struct {
	store     *daily.PostgresStore
	publisher *daily.PostgresPublisher
}

func (writer dailyPostSyncWriter) StartRunTx(
	ctx context.Context,
	tx pgx.Tx,
	plan syncdispatchruntime.PostSyncPlan,
	prerequisiteCompletionKey string,
) (string, error) {
	run, err := writer.store.StartRunTx(ctx, tx, daily.StartRunRequest{
		OrganizationID:            plan.OrganizationID,
		TargetDay:                 plan.TargetDay,
		Generation:                "post-sync:" + plan.SyncRunID,
		RepositoryIDs:             plan.RepositoryIDs,
		PrerequisiteCompletionKey: prerequisiteCompletionKey,
	}, writer.publisher)
	if err != nil {
		return "", err
	}
	completionKey, err := joboutbox.CompletionKey("daily_metrics_run", run.ID)
	if err != nil {
		return "", syncdispatchruntime.ErrPostSyncUnavailable
	}
	return completionKey, nil
}

type remainingPostSyncWriter struct {
	store     *remaining.PostgresStore
	publisher *remaining.PostgresPublisher
}

func (writer remainingPostSyncWriter) StartRunTx(
	ctx context.Context,
	tx pgx.Tx,
	family string,
	plan syncdispatchruntime.PostSyncPlan,
	prerequisiteCompletionKey string,
) (string, error) {
	scope, err := postSyncRemainingScope(family, plan)
	if err != nil {
		return "", err
	}
	run, err := writer.store.StartRunTx(ctx, tx, remaining.StartRunRequest{
		OrganizationID:            plan.OrganizationID,
		Family:                    family,
		Generation:                "post-sync:" + plan.SyncRunID,
		ScopeKey:                  string(scope),
		Scopes:                    []json.RawMessage{scope},
		PrerequisiteCompletionKey: prerequisiteCompletionKey,
	}, writer.publisher)
	if err != nil {
		return "", err
	}
	completionKey, err := joboutbox.CompletionKey("remaining_metric_run", run.ID)
	if err != nil {
		return "", syncdispatchruntime.ErrPostSyncUnavailable
	}
	return completionKey, nil
}

func postSyncRemainingScope(
	family string,
	plan syncdispatchruntime.PostSyncPlan,
) (json.RawMessage, error) {
	day := plan.TargetDay.UTC().Format("2006-01-02")
	switch family {
	case "complexity":
		return json.Marshal(struct {
			Version      int    `json:"version"`
			Day          string `json:"day"`
			BackfillDays int    `json:"backfill_days"`
		}{Version: remaining.ScopeVersion, Day: day, BackfillDays: 1})
	case "dora":
		backfillDays := min(90, max(1, plan.BackfillDays))
		return json.Marshal(struct {
			Version      int    `json:"version"`
			Day          string `json:"day"`
			BackfillDays int    `json:"backfill_days"`
			Sink         string `json:"sink"`
			Interval     string `json:"interval"`
		}{
			Version: remaining.ScopeVersion, Day: day, BackfillDays: backfillDays,
			Sink: "auto", Interval: "daily",
		})
	case "membership_backfill":
		return json.Marshal(struct {
			Version       int      `json:"version"`
			RepositoryIDs []string `json:"repo_ids"`
		}{
			Version: remaining.ScopeVersion, RepositoryIDs: []string{},
		})
	default:
		return nil, syncdispatchruntime.ErrPostSyncUnavailable
	}
}

type teamAutoimportPostSyncWriter struct {
	producer *joboutbox.Producer
	registry joboutbox.PolicyRegistry
}

// PublishTx stages the team-autoimport handoff in the fanout's transaction.
//
// The route is chosen from the descriptor, exactly as the daily, remaining and
// workgraph publishers do. The deferred producer path is legal ONLY while a
// kind is pinned to Celery on both its route and its rollback route; publishing
// deferred unconditionally meant every publish of this kind was rejected with
// publish_not_permitted_for_route once it was cut over to route=river, and
// because Fanout stages the whole generation in one transaction, that rejection
// discarded every other post-sync handoff with it (CHAOS-3946).
func (writer teamAutoimportPostSyncWriter) PublishTx(
	ctx context.Context,
	tx pgx.Tx,
	plan syncdispatchruntime.PostSyncPlan,
) error {
	if writer.producer == nil || writer.registry == nil || tx == nil {
		return syncdispatchruntime.ErrPostSyncUnavailable
	}
	descriptor, ok := writer.registry.Descriptor(jobcontract.KindTeamAutoimport)
	if !ok {
		return syncdispatchruntime.ErrPostSyncUnavailable
	}
	organizationID := plan.OrganizationID
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organizationID,
		CorrelationID:   "post-sync:" + plan.SyncRunID,
		IdempotencyKey:  "post-sync:" + plan.SyncRunID + ":" + jobcontract.KindTeamAutoimport,
		Domain:          jobcontract.DomainLink{Type: "sync_run", ID: plan.SyncRunID},
		Payload:         jobcontract.TeamAutoimportPayload{SyncRunID: plan.SyncRunID},
	}
	if descriptor.Executable() {
		return writer.producer.Publish(ctx, tx, jobcontract.KindTeamAutoimport, envelope)
	}
	return writer.producer.PublishDeferred(ctx, tx, jobcontract.KindTeamAutoimport, envelope)
}

var postSyncFanoutNamespace = uuid.MustParse("0713fbcf-ec5c-49dc-b7dc-18ae3de17536")

type workGraphPostSyncWriter struct{ writer *workgraph.RequestWriter }

func (writer workGraphPostSyncWriter) StartRequestTx(
	ctx context.Context,
	tx pgx.Tx,
	kind string,
	plan syncdispatchruntime.PostSyncPlan,
	prerequisiteCompletionKey string,
) (string, error) {
	var requestKind workgraph.Kind
	var consumer string
	switch kind {
	case jobcontract.KindWorkGraphBuild:
		requestKind, consumer = workgraph.KindBuild, "workgraph"
	case jobcontract.KindInvestmentMaterialize:
		requestKind, consumer = workgraph.KindMaterialize, "investment"
	default:
		return "", syncdispatchruntime.ErrPostSyncUnavailable
	}
	scope, err := postSyncWorkGraphScope(requestKind, plan)
	if err != nil {
		return "", err
	}
	generation := "post-sync:" + plan.SyncRunID
	requestID := postSyncRequestID(plan.SyncRunID, consumer)
	err = writer.writer.WriteTx(ctx, tx, workgraph.Request{
		ID:                        requestID,
		OrganizationID:            plan.OrganizationID,
		Kind:                      requestKind,
		Scope:                     scope,
		LLMConcurrency:            1,
		SpendLimitMicrounits:      0,
		CorrelationID:             generation,
		IdempotencyKey:            generation + ":" + kind,
		PrerequisiteCompletionKey: prerequisiteCompletionKey,
	})
	if err != nil {
		return "", err
	}
	completionKey, err := joboutbox.CompletionKey("work_graph_execution_request", requestID)
	if err != nil {
		return "", syncdispatchruntime.ErrPostSyncUnavailable
	}
	return completionKey, nil
}

func postSyncRequestID(syncRunID, consumer string) string {
	return uuid.NewSHA1(
		postSyncFanoutNamespace,
		[]byte(syncRunID+":"+consumer),
	).String()
}

func postSyncWorkGraphScope(
	kind workgraph.Kind,
	plan syncdispatchruntime.PostSyncPlan,
) ([]byte, error) {
	scope := map[string]any{}
	switch kind {
	case workgraph.KindBuild:
		if plan.From != nil {
			scope["from_date"] = plan.From.UTC().Format(time.RFC3339)
		}
		if plan.To != nil {
			scope["to_date"] = plan.To.UTC().Format(time.RFC3339)
		}
	case workgraph.KindMaterialize:
		if plan.From != nil {
			scope["from_date"] = plan.From.UTC().Format("2006-01-02")
		}
		if plan.To != nil {
			scope["to_date"] = plan.To.UTC().Format("2006-01-02")
		}
	default:
		return nil, syncdispatchruntime.ErrPostSyncUnavailable
	}
	return json.Marshal(scope)
}

// syncCoordinatorQueue is the queue the sync-dispatch plane declares, not an
// independent choice: the reconciler publishes dispatch_sync_run and its three
// siblings into exactly this queue, and the startup contract-version check
// resolves them from the same declaration (CHAOS-3938).
const syncCoordinatorQueue = syncdispatchcontract.RiverQueue

// buildSyncCoordinatorWorker hosts a mixed River client: four sync-dispatch
// coordinator kinds that are outside the bounded job registry (CUT-10 brings
// them in) plus one registered kind, sync.team_autoimport.
//
// The registered kind is what makes the return type a workerFamily. Before
// CUT-02 this builder returned only a lifecycle component, so the
// team-autoimport worker it constructed was invisible to startup validation --
// a second registration blind spot alongside the compiled-kind list. Capability
// must reach validation through one canonical channel no matter which client
// hosts the worker.
func buildSyncCoordinatorWorker(
	ctx context.Context,
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
	workers *river.Workers,
) (workerFamily, error) {
	if !queueSelected(cfg.Queues, syncCoordinatorQueue) {
		return workerFamily{}, nil
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil || logger == nil || registry == nil || workers == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	// reference_discovery's native ClickHouse readback verification
	// (CHAOS-4175) makes ClickHouse a hard requirement for this queue now,
	// the same way reports.go and provider_sync.go already require it for
	// theirs -- a verification step that silently skips when its dependency
	// is unconfigured is a check that fails toward "fine", the same failure
	// class this project already refuses elsewhere. Runtime unavailability
	// still degrades correctly (a query error becomes a retryable run
	// failure through the lease/backoff machinery, matching Python); this
	// check only turns "ClickHouse never configured at all" into a loud
	// startup error instead of a silent, permanent skip.
	if !cfg.ClickHouseURI.Configured() {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	clickhouseConnection, err := clickhousestore.Open(
		ctx, clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
	)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	closeClickHouse := func() { _ = clickhouseConnection.Close() }
	bridge, err := syncdispatchruntime.NewHTTPBridge(syncdispatchruntime.HTTPBridgeConfig{
		BaseURL:       strings.TrimRight(cfg.OperationalBridgeURL, "/"),
		BearerToken:   cfg.OperationalBridgeToken.Reveal(),
		Timeout:       cfg.OperationalBridgeTimeout,
		AllowInsecure: cfg.OperationalBridgeAllowInsecure,
	})
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	dailyStore, dailyStoreErr := daily.NewPostgresStore(postgresDatabase.pools.Domain)
	dailyPublisher, dailyPublisherErr := daily.NewPostgresPublisher(postgresDatabase.pools.Domain, registry)
	remainingStore, remainingStoreErr := remaining.NewPostgresStore(postgresDatabase.pools.Domain)
	remainingPublisher, remainingPublisherErr := remaining.NewPostgresPublisher(postgresDatabase.pools.Domain, registry)
	producer, producerErr := joboutbox.NewProducer(postgresDatabase.pools.Domain, registry)
	workGraphWriter, workGraphWriterErr := workgraph.NewRequestWriter(registry)
	if dailyStoreErr != nil || dailyPublisherErr != nil || remainingStoreErr != nil ||
		remainingPublisherErr != nil || producerErr != nil || workGraphWriterErr != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	postSync, err := syncdispatchruntime.NewNativePostSyncService(
		postgresDatabase.pools.Domain,
		dailyPostSyncWriter{store: dailyStore, publisher: dailyPublisher},
		remainingPostSyncWriter{store: remainingStore, publisher: remainingPublisher},
		workGraphPostSyncWriter{writer: workGraphWriter},
		teamAutoimportPostSyncWriter{producer: producer, registry: registry},
		logger,
	)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	// The zero-unit finalization counter reports directly, the same way the
	// work-graph store reports a release-lost lease directly (cmd/dev-health-worker/workgraph.go):
	// generic runtime middleware has no way to know a run planned zero units
	// or what cause finalize classified it under -- only this implementation
	// does (CHAOS-4175).
	var zeroUnitObservers []jobruntime.ZeroUnitFinalizationObserver
	if zeroUnitObserver, ok := observer.(jobruntime.ZeroUnitFinalizationObserver); ok {
		zeroUnitObservers = append(zeroUnitObservers, zeroUnitObserver)
	}
	finalizeSyncRun, err := syncdispatchruntime.NewNativeFinalizeSyncRunService(
		postgresDatabase.pools.Domain,
		logger,
		zeroUnitObservers...,
	)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	// The populate step (credential resolution + run_team_autoimport_strict)
	// stays behind the narrow, identifiers-only bridge call by design
	// (CHAOS-4175 ruling, 2026-08-24) -- everything else (claim/lease/
	// heartbeat/retry-backoff/outbox wakeups/state transitions, and now the
	// ClickHouse readback verification below) is native.
	bridgeDiscoveryExecutor, err := syncdispatchruntime.NewBridgeDiscoveryExecutor(bridge)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	readbackChecker, err := syncdispatchruntime.NewClickHouseReadbackVerifier(clickhouseConnection)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	readbackVerifier, err := syncdispatchruntime.NewReferenceReadbackVerifier(readbackChecker)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	discoveryExecutor, err := syncdispatchruntime.NewVerifiedDiscoveryExecutor(bridgeDiscoveryExecutor, readbackVerifier)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	referenceDiscovery, err := syncdispatchruntime.NewNativeReferenceDiscoveryService(
		postgresDatabase.pools.Domain,
		logger,
		discoveryExecutor,
	)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	// The budget-estimate-failure counter reports directly, the same way the
	// zero-unit finalization counter does just above: only this
	// implementation knows when its BudgetGuard estimate-bridge fetch fell
	// open, and why (codex round 2, CHAOS-4175).
	var budgetEstimateFailureObservers []jobruntime.BudgetEstimateFailureObserver
	if budgetEstimateFailureObserver, ok := observer.(jobruntime.BudgetEstimateFailureObserver); ok {
		budgetEstimateFailureObservers = append(budgetEstimateFailureObservers, budgetEstimateFailureObserver)
	}
	// dispatchSyncRun runs on the SAME domain pool/registry the producer above
	// already uses (postgresDatabase.pools.Domain, registry) -- no coordinator
	// pool needed. CHAOS-4175 ruling (see NativeDispatchSyncRunService's doc
	// comment): Dispatch's write path never reads worker_job_routes -- the
	// domain role has no grant on it -- so it needs no jobroute.Controller.
	dispatchSyncRun, err := syncdispatchruntime.NewNativeDispatchSyncRunService(
		postgresDatabase.pools.Domain,
		logger,
		bridge,
		producer,
		registry,
		budgetEstimateFailureObservers...,
	)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	if err := syncdispatchruntime.RegisterWorkers(workers, dispatchSyncRun, postSync, finalizeSyncRun, referenceDiscovery); err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	// A registered kind may only be consumed once its durable route permits
	// River execution. While sync.team_autoimport routes to Celery the worker
	// is not constructed at all, so the sync queue carries no registry
	// capability and stays out of registry queue coverage.
	autoimport, ok := registry.Descriptor(jobcontract.KindTeamAutoimport)
	if !ok {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	var handlers []jobruntime.HandlerSpec
	queues := selectedQueueBudgets(
		cfg.Queues, []string{syncCoordinatorQueue}, cfg.WorkerQueueConcurrency,
	)
	if autoimport.Executable() {
		if err := syncdispatchruntime.RegisterTeamAutoimportWorker(workers, bridge); err != nil {
			closeClickHouse()
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		handlers = []jobruntime.HandlerSpec{autoimport}
	}
	return workerFamily{
		handlers: handlers,
		queues:   queues,
		// RegisterWorkers above registered real workers for these four kinds
		// without reporting them as handler specs; rescue coverage (now applied
		// once, centrally) must still treat them as owned.
		ownedKinds: []string{
			syncdispatchcontract.KindDispatchSyncRun,
			syncdispatchcontract.KindFinalizeSyncRun,
			syncdispatchcontract.KindPostSync,
			syncdispatchcontract.KindReferenceDiscovery,
		},
		cleanups: []func() error{clickhouseConnection.Close},
	}, nil
}
