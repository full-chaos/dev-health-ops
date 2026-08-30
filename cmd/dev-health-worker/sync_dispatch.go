package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cacheinvalidation"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	valkeygo "github.com/valkey-io/valkey-go"
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
) (string, string, error) {
	// RepositoryIDs is intentionally omitted: this run's repository set is
	// resolved later, from live ClickHouse repos.id, via the same
	// daily.RepositoryDiscoverer the scheduled fixed-schedule fan-out uses
	// (CHAOS-4263). sync_run_units.source_id is a Postgres integration_sources
	// id and was never in ClickHouse's id space.
	run, err := writer.store.StartRunTx(ctx, tx, daily.StartRunRequest{
		OrganizationID:            plan.OrganizationID,
		TargetDay:                 plan.TargetDay,
		Generation:                "post-sync:" + plan.SyncRunID,
		PrerequisiteCompletionKey: prerequisiteCompletionKey,
	}, writer.publisher)
	if err != nil {
		return "", "", err
	}
	completionKey, err := joboutbox.CompletionKey("daily_metrics_run", run.ID)
	if err != nil {
		return "", "", syncdispatchruntime.ErrPostSyncUnavailable
	}
	// Re-drive the stale window behind this sync's target day (CHAOS-4263):
	// before this, post-sync only ever re-triggered day D, so a day's
	// cicd/deploy/incident families could be computed once by an earlier
	// git/work-item-triggered run and then never recomputed once fresh
	// CI/deploy/incident data landed for it. These catch-up dispatches are
	// independent of the day-D chain above: they neither wait on
	// prerequisiteCompletionKey nor gate anything downstream of it.
	for _, day := range postSyncDailyBackfillDays(plan) {
		if _, err := writer.store.StartRunTx(ctx, tx, daily.StartRunRequest{
			OrganizationID: plan.OrganizationID,
			TargetDay:      day,
			Generation:     "post-sync:" + plan.SyncRunID,
		}, writer.publisher); err != nil {
			return "", "", err
		}
	}
	return run.ID, completionKey, nil
}

// maxPostSyncDailyBackfillDays bounds how many extra days behind a sync's
// target day the post-sync daily dispatch will re-drive (CHAOS-4263). Each
// extra day is a whole separate daily_metrics_runs pipeline (dispatch,
// per-repository partitions, finalize, and a job_daily.py execution per
// family), unlike the dora family's BackfillDays, which one job execution
// consumes as a single bounded query -- so this cap is deliberately smaller
// than dora's 90 (postSyncRemainingScope) to avoid a single sync's post-sync
// fanout bursting dozens of concurrent day-pipelines. 14 days comfortably
// covers the staleness windows observed in the CHAOS-4263 incident (up to 18
// days for deploy_metrics_daily); a gap wider than that is a historical
// backfill, which is a deliberate one-off operation, not a re-drive.
const maxPostSyncDailyBackfillDays = 14

// postSyncDailyBackfillDays returns the additional days behind plan.TargetDay
// that this sync's window covers, bounded by maxPostSyncDailyBackfillDays
// (CHAOS-4263). Day D itself is dispatched separately, unconditionally, by
// the caller; this only ever returns the days strictly before it.
func postSyncDailyBackfillDays(plan syncdispatchruntime.PostSyncPlan) []time.Time {
	extraDays := plan.BackfillDays - 1
	if extraDays > maxPostSyncDailyBackfillDays {
		extraDays = maxPostSyncDailyBackfillDays
	}
	if extraDays <= 0 {
		return nil
	}
	targetDay := plan.TargetDay.UTC()
	days := make([]time.Time, 0, extraDays)
	for offset := 1; offset <= extraDays; offset++ {
		days = append(days, targetDay.AddDate(0, 0, -offset))
	}
	return days
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

// teamRepoOwnershipPostSyncWriter stages the CHAOS-4365 item 1b
// team_repo_ownership-derivation handoff. A SIBLING of teamAutoimportPostSyncWriter
// above, not a repurposing of it: that one drives the live Python autoimport
// bridge (currently a no-op in prod -- GitHub Teams config is off); this one
// is Go-only, no provider fetch, no Python at all, and its route is river
// from day one (state=celery_removed, migration-state.json) -- there is no
// Celery implementation to defer to, unlike team-autoimport's still-live
// Celery fallback.
type teamRepoOwnershipPostSyncWriter struct {
	producer *joboutbox.Producer
	registry joboutbox.PolicyRegistry
}

// PublishTx stages the team-repo-ownership-derivation handoff in the fanout's
// transaction, same route-descriptor-driven publish/deferred choice as
// teamAutoimportPostSyncWriter.PublishTx (see its doc comment for the
// CHAOS-3946 rationale) even though this kind's descriptor is always
// Executable() today -- keeping the same shape means a future route change
// (a pause, say) degrades the same way every other bounded-registry kind
// does, rather than needing a special case here.
func (writer teamRepoOwnershipPostSyncWriter) PublishTx(
	ctx context.Context,
	tx pgx.Tx,
	plan syncdispatchruntime.PostSyncPlan,
) error {
	if writer.producer == nil || writer.registry == nil || tx == nil {
		return syncdispatchruntime.ErrPostSyncUnavailable
	}
	descriptor, ok := writer.registry.Descriptor(jobcontract.KindTeamRepoOwnershipDerivation)
	if !ok {
		return syncdispatchruntime.ErrPostSyncUnavailable
	}
	organizationID := plan.OrganizationID
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organizationID,
		CorrelationID:   "post-sync:" + plan.SyncRunID,
		IdempotencyKey:  "post-sync:" + plan.SyncRunID + ":" + jobcontract.KindTeamRepoOwnershipDerivation,
		Domain:          jobcontract.DomainLink{Type: "sync_run", ID: plan.SyncRunID},
		Payload:         jobcontract.TeamRepoOwnershipDerivationPayload{SyncRunID: plan.SyncRunID},
	}
	if descriptor.Executable() {
		return writer.producer.Publish(ctx, tx, jobcontract.KindTeamRepoOwnershipDerivation, envelope)
	}
	return writer.producer.PublishDeferred(ctx, tx, jobcontract.KindTeamRepoOwnershipDerivation, envelope)
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
	// Valkey is the second hard requirement (CHAOS-4226): the finalize's
	// post-commit hop bumps the home-dashboard cache epoch there. A family
	// built without it would finalize forever with the cache hop skipped --
	// the exact silent miss this ticket removes -- so it refuses to build,
	// the same way the ClickHouse check above does.
	if !cfg.ValkeyURI.Configured() {
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
	closeClickHouse := func() {
		valkeyClient.Close()
		_ = clickhouseConnection.Close()
	}
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
	// CHAOS-4384 (codex round 3): this is the store post-sync's own
	// StartRunTx call actually runs through -- the daily.go store built for
	// remainingSpecs' PartitionHandler never reaches StartRunTx at all, so
	// wiring the observer there instead would leave the open-day zero-row
	// counter permanently at zero for the exact trigger this ticket is about.
	if openDayZeroRowObserver, ok := observer.(remaining.OpenDayZeroRowObserver); ok {
		remainingStore.SetOpenDayZeroRowObserver(openDayZeroRowObserver)
	}
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
		teamRepoOwnershipPostSyncWriter{producer: producer, registry: registry},
		logger,
	)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	// The fanout-outcome counter reports directly, the same way the daily
	// discovery/zero-rows observers do: generic runtime middleware has no way
	// to know whether Fanout published a daily-metrics re-drive for this
	// sync's organization or found nothing daily-relevant -- only Fanout
	// itself knows (CHAOS-4263, codex adversarial-review round 2).
	if fanoutObserver, ok := observer.(jobruntime.PostSyncFanoutObserver); ok {
		postSync.SetFanoutObserver(fanoutObserver)
	}
	// The route_missing counter reports directly for the same reason (team-lead
	// ruling, 2026-08-28: "non-fatal != silent") -- only publishTeamRepoOwnershipDerivation
	// knows when its own swallowed deterministic rejection happened.
	if teamRepoOwnershipDerivationObserver, ok := observer.(jobruntime.TeamRepoOwnershipDerivationObserver); ok {
		postSync.SetTeamRepoOwnershipDerivationObserver(teamRepoOwnershipDerivationObserver)
	}
	// The zero-unit finalization counter reports directly, the same way the
	// work-graph store reports a release-lost lease directly (cmd/dev-health-worker/workgraph.go):
	// generic runtime middleware has no way to know a run planned zero units
	// or what cause finalize classified it under -- only this implementation
	// does (CHAOS-4175).
	finalizeSyncRun, err := buildFinalizeSyncRunService(
		postgresDatabase.pools.Domain, logger, observer, valkeyClient,
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
	// CHAOS-4431: a registered native provider runs its own collector and
	// skips the Python populate bridge entirely; every other provider keeps
	// going through bridgeDiscoveryExecutor exactly as before. ClickHouse
	// readback verification (readbackVerifier, just above) still wraps the
	// combined result either way. The same native-collector map and client
	// resolver feed the post-sync team-autoimport dispatch below
	// (teamAutoimportNative/teamCatalogClients), so they are built once here.
	catalogDecryptor, err := newWorkerCredentialCipher(cfg)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	nativeTeamCatalogCollectors := map[string]providersync.TeamCatalogCollector{
		"linear": providersync.LinearTeamCatalogCollector{
			Sink: providersync.LinearReferenceCatalogClickHouseEffects{
				Conn: clickhouseConnection, Lease: teamCatalogLease{},
			},
		},
		// CHAOS-4434: GitHub teams/team_memberships, Go-native. No Projects
		// surface exists for GitHub at all (auto_import_capabilities("github").
		// projects is permanently False in Python); the collector reads
		// selections.Projects but never produces a Projects/Ownership row.
		// Telemetry is generic (jobruntime.TeamCatalogObserver, wired below via
		// teamCatalogObserver) -- no per-provider Observer field needed here.
		"github": providersync.GitHubTeamCatalogCollector{
			// ResolveEmail: true mirrors PyGithub's lazy NamedUser.email
			// completion (team_membership.py's discover_members_github) --
			// without it, every membership facet set collapses to just
			// "github:<login>" and an email-based assignee can no longer
			// match team attribution (codex round 1, P2).
			Client: providersync.GitHubTeamCatalogRouteHandler{ResolveEmail: true},
			Sink:   providersync.GitHubTeamCatalogClickHouseEffects{Conn: clickhouseConnection},
		},
		// CHAOS-4432: GitLab teams/team_project_ownership/team_memberships +
		// native projects catalog (CHAOS-3380), Go-native. GroupPathResolver
		// is nil for now -- GitLab is the only provider whose group scoping
		// isn't carried on the credential itself; it resolves from this run's
		// sync_options once TeamCatalogReference.SyncOptions lands (CHAOS-4431
		// follow-up), tracked separately.
		"gitlab": providersync.GitLabTeamCatalogCollector{
			Handler: providersync.GitLabTeamCatalogRouteHandler{},
			Sink: providersync.GitLabTeamCatalogClickHouseEffects{
				Conn: clickhouseConnection, Lease: teamCatalogLease{},
			},
		},
	}
	teamCatalogClients := teamCatalogClientResolver{
		pool: postgresDatabase.pools.Domain,
		credentials: providerfoundation.CredentialResolver{
			Repository: providerfoundation.PostgresCredentialRepository{
				Pool: postgresDatabase.pools.Domain,
			},
			Decryptor: catalogDecryptor,
		},
		doer: &http.Client{
			Timeout: 45 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		retry: providerfoundation.DefaultRetryPolicy(),
	}
	teamCatalogSelections := teamCatalogSelectionsResolver{pool: postgresDatabase.pools.Domain}
	teamCatalogSources := teamCatalogSourceResolver{pool: postgresDatabase.pools.Domain}
	var teamCatalogObserver jobruntime.TeamCatalogObserver
	if typed, ok := observer.(jobruntime.TeamCatalogObserver); ok {
		teamCatalogObserver = typed
	}
	teamCatalogExecutor := &syncdispatchruntime.TeamCatalogDiscoveryExecutor{
		Native:     nativeTeamCatalogCollectors,
		Fallback:   bridgeDiscoveryExecutor,
		Clients:    teamCatalogClients,
		Selections: teamCatalogSelections,
		Sources:    teamCatalogSources,
		Observer:   teamCatalogObserver,
	}
	discoveryExecutor, err := syncdispatchruntime.NewVerifiedDiscoveryExecutor(teamCatalogExecutor, readbackVerifier)
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
	// syncCoordinatorMetrics is ONLY a vehicle for calling
	// RecordSyncRunRollupBumped on -- that method delegates to a
	// process-wide singleton (providerfoundation.SyncRunRollupBumpedMetricsSource,
	// registered once, unconditionally, in dependencies.go), not to this
	// instance's own counters. Deliberately NOT registered as this family's
	// own workerFamily.metricsSource (codex round 1, P1, CHAOS-4586): a
	// worker group running both the sync_provider and sync_coordinator
	// queues would then construct TWO providerfoundation.Metrics instances,
	// each independently declaring the SAME ~20 dev_health_provider_*
	// metric families (empty on this side, since this family never calls
	// most of those methods) -- most Prometheus parsers hard-fail the
	// WHOLE scrape on a metric name declared twice, not just that series.
	syncCoordinatorMetrics := providerfoundation.NewMetrics()
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
	// WithMetrics wires dev_health_sync_run_rollup_bumped_total's five
	// syncdispatchruntime path labels (CHAOS-4586) through to the
	// process-wide singleton -- RecordSyncRunRollupBumped delegates there
	// regardless of which *Metrics instance it is called on, so
	// syncCoordinatorMetrics never needs to be scraped itself.
	dispatchSyncRun.WithMetrics(syncCoordinatorMetrics)
	referenceDiscovery.WithMetrics(syncCoordinatorMetrics)
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
		// CHAOS-4431: a sync run whose own provider has a registered native
		// collector runs it directly (gated by CHAOS-4323 selections,
		// mirroring Python's non-strict run_team_autoimport) and never calls
		// the bridge's team-autoimport endpoint; every other provider still
		// does, unchanged. Reuses the same native-collector map and client
		// resolver the reference-discovery executor above was built with.
		teamAutoimportBridge := &teamCatalogAutoimportBridge{
			CoordinatorBridge: bridge,
			resolveProvider: func(ctx context.Context, orgID, runID string) (string, error) {
				return resolveTeamCatalogProvider(ctx, postgresDatabase.pools.Domain, orgID, runID)
			},
			native:     nativeTeamCatalogCollectors,
			clients:    teamCatalogClients,
			selections: teamCatalogSelections,
			sources:    teamCatalogSources,
			observer:   teamCatalogObserver,
		}
		if err := syncdispatchruntime.RegisterTeamAutoimportWorker(workers, teamAutoimportBridge); err != nil {
			closeClickHouse()
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		handlers = []jobruntime.HandlerSpec{autoimport}
	}
	// sync.team_repo_ownership_derivation (CHAOS-4365 item 1b) is river
	// unconditionally (state=celery_removed) -- no Executable() gate needed,
	// unlike team-autoimport above. Its worker reads ClickHouse directly
	// (clickhouseConnection, already open for reference-discovery readback
	// above), never the HTTP bridge.
	teamRepoOwnershipDerivation, ok := registry.Descriptor(jobcontract.KindTeamRepoOwnershipDerivation)
	if !ok {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	var teamRepoOwnershipDerivationObserver jobruntime.TeamRepoOwnershipDerivationObserver
	if typed, ok := observer.(jobruntime.TeamRepoOwnershipDerivationObserver); ok {
		teamRepoOwnershipDerivationObserver = typed
	}
	if err := syncdispatchruntime.RegisterTeamRepoOwnershipDerivationWorker(
		workers,
		providersync.TeamRepoOwnershipDerivationService{Conn: clickhouseConnection},
		teamRepoOwnershipDerivationObserver,
	); err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	handlers = append(handlers, teamRepoOwnershipDerivation)
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
		cleanups: []func() error{
			clickhouseConnection.Close,
			func() error { valkeyClient.Close(); return nil },
		},
		// No metricsSource here (codex round 1, P1, CHAOS-4586) -- see
		// syncCoordinatorMetrics's own doc comment above for why.
	}, nil
}

// buildFinalizeSyncRunService constructs the native finalize with BOTH its
// post-commit side channels attached: the zero-unit counter and the
// coverage-cache epoch bumper (CHAOS-4226). Split out of
// buildSyncCoordinatorWorker so a plain unit test can prove the Valkey
// client actually reaches the service (sync_dispatch_cache_invalidation_test.go)
// -- a cited constructor is not proof of capability.
func buildFinalizeSyncRunService(
	pool *pgxpool.Pool,
	logger *slog.Logger,
	observer jobruntime.Observer,
	valkeyClient valkeygo.Client,
) (*syncdispatchruntime.NativeFinalizeSyncRunService, error) {
	var zeroUnitObservers []jobruntime.ZeroUnitFinalizationObserver
	if zeroUnitObserver, ok := observer.(jobruntime.ZeroUnitFinalizationObserver); ok {
		zeroUnitObservers = append(zeroUnitObservers, zeroUnitObserver)
	}
	service, err := syncdispatchruntime.NewNativeFinalizeSyncRunService(pool, logger, zeroUnitObservers...)
	if err != nil {
		return nil, err
	}
	invalidator, err := cacheinvalidation.NewValkeyOrgCacheInvalidator(valkeyClient)
	if err != nil {
		return nil, err
	}
	if err := service.UseCoverageCacheInvalidator(invalidator); err != nil {
		return nil, err
	}
	return service, nil
}
