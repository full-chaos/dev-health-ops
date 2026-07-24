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
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
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
) error {
	_, err := writer.store.StartRunTx(ctx, tx, daily.StartRunRequest{
		OrganizationID: plan.OrganizationID,
		TargetDay:      plan.TargetDay,
		Generation:     "post-sync:" + plan.SyncRunID,
		RepositoryIDs:  plan.RepositoryIDs,
	}, writer.publisher)
	return err
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
) error {
	scope, err := postSyncRemainingScope(family, plan)
	if err != nil {
		return err
	}
	_, err = writer.store.StartRunTx(ctx, tx, remaining.StartRunRequest{
		OrganizationID: plan.OrganizationID,
		Family:         family,
		Generation:     "post-sync:" + plan.SyncRunID,
		ScopeKey:       string(scope),
		Scopes:         []json.RawMessage{scope},
	}, writer.publisher)
	return err
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
) error {
	var requestKind workgraph.Kind
	var consumer string
	switch kind {
	case jobcontract.KindWorkGraphBuild:
		requestKind, consumer = workgraph.KindBuild, "workgraph"
	case jobcontract.KindInvestmentDispatch:
		// The current compatibility executor delegates to the legacy Python
		// dispatcher, which creates a Celery chord. Native post-sync must not
		// hide that second transport behind a River delivery.
		return syncdispatchruntime.ErrPostSyncUnavailable
	default:
		return syncdispatchruntime.ErrPostSyncUnavailable
	}
	scope, err := postSyncWorkGraphScope(requestKind, plan)
	if err != nil {
		return err
	}
	generation := "post-sync:" + plan.SyncRunID
	return writer.writer.WriteTx(ctx, tx, workgraph.Request{
		ID:                   postSyncRequestID(plan.SyncRunID, consumer),
		OrganizationID:       plan.OrganizationID,
		Kind:                 requestKind,
		Scope:                scope,
		LLMConcurrency:       1,
		SpendLimitMicrounits: 0,
		CorrelationID:        generation,
		IdempotencyKey:       generation + ":" + kind,
	})
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

func buildSyncCoordinatorWorker(
	cfg config.Config,
	database workerDatabase,
	logger *slog.Logger,
) (lifecycle.Component, error) {
	if cfg.Profile != "sync" {
		return nil, nil
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil || logger == nil {
		return nil, errWorkerDependencyUnavailable
	}
	bridge, err := syncdispatchruntime.NewHTTPBridge(syncdispatchruntime.HTTPBridgeConfig{
		BaseURL:       strings.TrimRight(cfg.OperationalBridgeURL, "/"),
		BearerToken:   cfg.OperationalBridgeToken.Reveal(),
		Timeout:       cfg.OperationalBridgeTimeout,
		AllowInsecure: cfg.OperationalBridgeAllowInsecure,
	})
	if err != nil {
		return nil, errWorkerDependencyUnavailable
	}
	registry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		return nil, errWorkerDependencyUnavailable
	}
	dailyStore, dailyStoreErr := daily.NewPostgresStore(postgresDatabase.pools.Domain)
	dailyPublisher, dailyPublisherErr := daily.NewPostgresPublisher(postgresDatabase.pools.Domain, registry)
	remainingStore, remainingStoreErr := remaining.NewPostgresStore(postgresDatabase.pools.Domain)
	remainingPublisher, remainingPublisherErr := remaining.NewPostgresPublisher(postgresDatabase.pools.Domain, registry)
	producer, producerErr := joboutbox.NewProducer(postgresDatabase.pools.Domain, registry)
	workGraphWriter, workGraphWriterErr := workgraph.NewRequestWriter(registry)
	if dailyStoreErr != nil || dailyPublisherErr != nil || remainingStoreErr != nil ||
		remainingPublisherErr != nil || producerErr != nil || workGraphWriterErr != nil {
		return nil, errWorkerDependencyUnavailable
	}
	postSync, err := syncdispatchruntime.NewNativePostSyncService(
		postgresDatabase.pools.Domain,
		dailyPostSyncWriter{store: dailyStore, publisher: dailyPublisher},
		remainingPostSyncWriter{store: remainingStore, publisher: remainingPublisher},
		workGraphPostSyncWriter{writer: workGraphWriter},
		teamAutoimportPostSyncWriter{producer: producer},
	)
	if err != nil {
		return nil, errWorkerDependencyUnavailable
	}
	workers := river.NewWorkers()
	if err := syncdispatchruntime.RegisterWorkers(workers, bridge, postSync); err != nil {
		return nil, errWorkerDependencyUnavailable
	}
	client, err := river.NewClient(riverpgxv5.New(postgresDatabase.pools.QueueControl), &river.Config{
		Logger: logger,
		Queues: map[string]river.QueueConfig{
			"sync": {MaxWorkers: 4},
		},
		Schema:  cfg.RiverDatabaseSchema,
		Workers: workers,
	})
	if err != nil {
		return nil, errWorkerDependencyUnavailable
	}
	return syncCoordinatorLifecycle{startStop: client}, nil
}
