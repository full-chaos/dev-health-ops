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
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
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

type teamAutoimportPostSyncWriter struct{ producer *joboutbox.Producer }

func (writer teamAutoimportPostSyncWriter) PublishTx(
	ctx context.Context,
	tx pgx.Tx,
	plan syncdispatchruntime.PostSyncPlan,
) error {
	organizationID := plan.OrganizationID
	return writer.producer.PublishDeferred(ctx, tx, jobcontract.KindTeamAutoimport, jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organizationID,
		CorrelationID:   "post-sync:" + plan.SyncRunID,
		IdempotencyKey:  "post-sync:" + plan.SyncRunID + ":" + jobcontract.KindTeamAutoimport,
		Domain:          jobcontract.DomainLink{Type: "sync_run", ID: plan.SyncRunID},
		Payload:         jobcontract.TeamAutoimportPayload{SyncRunID: plan.SyncRunID},
	})
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

// The client type includes the driver's transaction type, but lifecycle only
// needs Start and Stop. Keep the component concrete below to avoid exposing a
// broad worker runtime interface.
type syncCoordinatorLifecycle struct {
	startStop interface {
		Start(context.Context) error
		Stop(context.Context) error
	}
}

func (component syncCoordinatorLifecycle) Name() string { return "river-sync-coordinator-worker" }
func (component syncCoordinatorLifecycle) Start(ctx context.Context) error {
	return component.startStop.Start(ctx)
}
func (component syncCoordinatorLifecycle) Shutdown(ctx context.Context) error {
	return component.startStop.Stop(ctx)
}

// syncCoordinatorQueue and its worker budget must match the deployment
// manifest entry for the sync process.
const (
	syncCoordinatorQueue        = "sync"
	syncCoordinatorQueueWorkers = 4
)

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
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	_ jobruntime.Observer,
	logger *slog.Logger,
) (workerFamily, error) {
	if cfg.Profile != "sync" {
		return workerFamily{}, nil
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil || logger == nil || registry == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	bridge, err := syncdispatchruntime.NewHTTPBridge(syncdispatchruntime.HTTPBridgeConfig{
		BaseURL:       strings.TrimRight(cfg.OperationalBridgeURL, "/"),
		BearerToken:   cfg.OperationalBridgeToken.Reveal(),
		Timeout:       cfg.OperationalBridgeTimeout,
		AllowInsecure: cfg.OperationalBridgeAllowInsecure,
	})
	if err != nil {
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
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	postSync, err := syncdispatchruntime.NewNativePostSyncService(
		postgresDatabase.pools.Domain,
		dailyPostSyncWriter{store: dailyStore, publisher: dailyPublisher},
		remainingPostSyncWriter{store: remainingStore, publisher: remainingPublisher},
		workGraphPostSyncWriter{writer: workGraphWriter},
		teamAutoimportPostSyncWriter{producer: producer},
	)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	workers := river.NewWorkers()
	if err := syncdispatchruntime.RegisterWorkers(workers, bridge, postSync); err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	// A registered kind may only be consumed once its durable route permits
	// River execution. While sync.team_autoimport routes to Celery the worker
	// is not constructed at all, so the sync queue carries no registry
	// capability and stays out of registry queue coverage.
	autoimport, ok := registry.Descriptor(jobcontract.KindTeamAutoimport)
	if !ok {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	var handlers []jobruntime.HandlerSpec
	var queues []jobruntime.QueueBudget
	if autoimport.Executable() {
		if err := syncdispatchruntime.RegisterTeamAutoimportWorker(workers, bridge); err != nil {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		handlers = []jobruntime.HandlerSpec{autoimport}
		queues = []jobruntime.QueueBudget{
			{Queue: syncCoordinatorQueue, MaxWorkers: syncCoordinatorQueueWorkers},
		}
	}
	if err := registerRescueCoverage(
		workers,
		registry,
		handlers,
		syncdispatchcontract.KindDispatchSyncRun,
		syncdispatchcontract.KindFinalizeSyncRun,
		syncdispatchcontract.KindPostSync,
		syncdispatchcontract.KindReferenceDiscovery,
	); err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	client, err := river.NewClient(riverpgxv5.New(postgresDatabase.pools.QueueControl), &river.Config{
		ID:     riverClientID(cfg, "sync-coordinator"),
		Logger: logger,
		Queues: map[string]river.QueueConfig{
			syncCoordinatorQueue: {MaxWorkers: syncCoordinatorQueueWorkers},
		},
		Schema:  cfg.RiverDatabaseSchema,
		Workers: workers,
	})
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	return workerFamily{
		component: syncCoordinatorLifecycle{startStop: client},
		handlers:  handlers,
		queues:    queues,
	}, nil
}
