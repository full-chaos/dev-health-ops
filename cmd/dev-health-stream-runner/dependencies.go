package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/externalrecompute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/pagerduty"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/postureguard"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/processreadiness"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

var errStreamDependencyUnavailable = errors.New("stream-runner dependency is unavailable")

// dependencyFailure attaches a bounded reason code to the generic dependency
// sentinel, mirroring cmd/dev-health-worker/dependencies.go's dependencyFailure
// exactly. Before this, every distinct storage-construction failure -- a
// missing URI, a domain Postgres pool that would not open, ClickHouse
// refusing the connection, Valkey refusing the connection -- collapsed into
// the same bare errStreamDependencyUnavailable, so the shell logged
// "dependency_configuration_failed" with no reason and an operator could not
// tell which knob was wrong (CHAOS-3873/CHAOS-3907). The reason is always a
// compile-time constant, never interpolated input, so logging it cannot leak
// a DSN or a secret.
type dependencyFailure struct {
	reason string
}

func (failure dependencyFailure) Error() string {
	return errStreamDependencyUnavailable.Error() + ": " + failure.reason
}

func (dependencyFailure) Unwrap() error { return errStreamDependencyUnavailable }

// DependencyReason satisfies the shell's reason-code interface.
func (failure dependencyFailure) DependencyReason() string { return failure.reason }

func dependencyUnavailable(reason string) error { return dependencyFailure{reason: reason} }

// wrapStreamRunnerReadinessCheckWithLogging logs a failing check's own
// bounded error text before returning it unchanged. health.Registry never
// surfaces a CheckFunc's returned error anywhere (its own doc comment:
// "Error text is deliberately never returned by the HTTP surface"), so a
// check whose error carries real diagnostic detail -- like
// postureguard.Guard.Ready's, which names both posture-manifest digests --
// needs this to make that detail reach an operator at all. Mirrors
// cmd/dev-health-scheduler's wrapSchedulerReadinessCheckWithLogging exactly;
// not shared between the two binaries because neither imports the other's
// package for a two-line helper.
func wrapStreamRunnerReadinessCheckWithLogging(logger *slog.Logger, check string, ready health.CheckFunc) health.CheckFunc {
	return func(ctx context.Context) error {
		err := ready(ctx)
		if err != nil && logger != nil {
			logger.ErrorContext(ctx, "stream-runner readiness dependency check failed",
				"error_category", "dependency_unavailable",
				"check", check,
				"error", err.Error(),
			)
		}
		return err
	}
}

type streamHandlerKind string

const (
	internalIngestHandlerKind   streamHandlerKind = "internal-ingest"
	productTelemetryHandlerKind streamHandlerKind = "product-telemetry"
	externalIngestHandlerKind   streamHandlerKind = "external-ingest"
	pagerdutyHandlerKind        streamHandlerKind = "pagerduty"
)

type streamStorage interface {
	ClickHouseReady(context.Context) error
	DomainPostgresReady(context.Context) error
	// PostureManifestLockstep proves (CHAOS-5437) that binaryDigest -- this
	// binary's own postgres.PostureManifestDigest() -- is no older than the
	// posture manifest go-worker-migrate most recently applied.
	PostureManifestLockstep(ctx context.Context, binaryDigest string) (postgres.PostureManifestLockstepResult, error)
	ValkeyReady(context.Context) error
	Handler(streamHandlerKind, streamhandlers.ExternalIngestObserver) (streamrunner.Handler, error)
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
	pagerduty   pagerdutyWebhookSettings
	logger      *slog.Logger
}

// pagerdutyWebhookSettings is what the native PagerDuty webhook reconciler
// needs beyond the shared connections: the credential cipher for the REST
// hydration fallback, the OAuth app identity that cipher's hydrator refreshes
// with, and the Prometheus fragment its entitlement refusals land on.
//
// It replaces operationalBridgeSettings (CHAOS-4105). That struct existed to
// carry the Python worker bridge's URL, token and transport flag; there is no
// bridge and no second runtime left, so none of those fields have a reader.
type pagerdutyWebhookSettings struct {
	cipher        providerfoundation.FernetDecryptor
	oauthClientID secrets.Value
	oauthSecret   secrets.Value
	metrics       *providerfoundation.Metrics
}

// newExternalRecomputeController is the one place this binary constructs the
// external-ingest recompute controller. It exists as a named seam (rather than
// inlining externalrecompute.DefaultConfig() at the call site) so a
// composition-root test can prove the process's configured logger reaches the
// controller without needing a live ClickHouse/Postgres/Valkey connection --
// see TestExternalRecomputeControllerReceivesTheComposedLogger.
func newExternalRecomputeController(
	store externalrecompute.Store,
	dispatcher externalrecompute.CompatibilityDispatcher,
	logger *slog.Logger,
) (*externalrecompute.Controller, error) {
	cfg := externalrecompute.DefaultConfig()
	cfg.Logger = logger
	return externalrecompute.New(store, dispatcher, cfg)
}

func openProductionStreamStorage(ctx context.Context, cfg config.Config, logger *slog.Logger) (streamStorage, error) {
	if !cfg.ClickHouseURI.Configured() || !cfg.DomainDatabaseURI.Configured() || !cfg.ValkeyURI.Configured() {
		return nil, dependencyUnavailable("stream_storage_uris_unconfigured")
	}
	domainConfig := postgres.DefaultConfig(cfg.DomainDatabaseURI.Reveal())
	domainConfig.MaxConns = cfg.DomainDatabaseMaxConns
	domainPool, err := postgres.New(ctx, domainConfig)
	if err != nil {
		return nil, dependencyUnavailable("stream_domain_postgres_open_failed")
	}
	clickHouse, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(cfg.ClickHouseURI.Reveal()))
	if err != nil {
		domainPool.Close()
		return nil, dependencyUnavailable("stream_clickhouse_open_failed")
	}
	valkeyConfig := valkey.DefaultConfig(cfg.ValkeyURI.Reveal())
	valkeyConfig.ClientName = "dev-health-stream-runner-" + cfg.Profile
	valkeyClient, err := valkey.Open(ctx, valkeyConfig)
	if err != nil {
		_ = clickHouse.Close()
		domainPool.Close()
		return nil, dependencyUnavailable("stream_valkey_open_failed")
	}
	// The PagerDuty webhook reconciler decrypts the binding's own credential
	// for its REST hydration fallback. Building the cipher here rather than at
	// first use means a misconfigured key stops the process at startup instead
	// of surfacing as a single failed delivery days later.
	cipher, err := providerfoundation.NewFernetDecryptor(
		cfg.SettingsEncryptionKey, cfg.SettingsEncryptionSalt.Reveal(),
	)
	if err != nil {
		valkeyClient.Close()
		_ = clickHouse.Close()
		domainPool.Close()
		return nil, dependencyUnavailable("stream_credential_cipher_unconfigured")
	}
	return &productionStreamStorage{
		clickHouse: clickHouse, domainPool: domainPool, valkey: valkeyClient,
		domainRole: cfg.DomainDatabaseRole, riverSchema: cfg.RiverDatabaseSchema,
		logger: logger,
		pagerduty: pagerdutyWebhookSettings{
			cipher:        cipher,
			oauthClientID: cfg.PagerDutyOAuthClientID,
			oauthSecret:   cfg.PagerDutyOAuthSecret,
			metrics:       providerfoundation.NewMetrics(),
		},
	}, nil
}

func (storage *productionStreamStorage) ClickHouseReady(ctx context.Context) error {
	if storage == nil || storage.clickHouse == nil {
		return errStreamDependencyUnavailable
	}
	if err := storage.clickHouse.Ping(ctx); err != nil {
		storage.logDependencyCheckFailure(ctx, "clickhouse", err)
		return errStreamDependencyUnavailable
	}
	return nil
}

func (storage *productionStreamStorage) DomainPostgresReady(ctx context.Context) error {
	if storage == nil || storage.domainPool == nil {
		return errStreamDependencyUnavailable
	}
	if err := postgres.CheckDomainAuthorization(ctx, storage.domainPool, storage.domainRole, storage.riverSchema); err != nil {
		storage.logDependencyCheckFailure(ctx, "domain_postgres", err)
		return errStreamDependencyUnavailable
	}
	return nil
}

func (storage *productionStreamStorage) PostureManifestLockstep(
	ctx context.Context, binaryDigest string,
) (postgres.PostureManifestLockstepResult, error) {
	if storage == nil || storage.domainPool == nil {
		return postgres.PostureManifestLockstepResult{}, errStreamDependencyUnavailable
	}
	return postgres.CheckPostureManifestLockstep(ctx, storage.domainPool, binaryDigest)
}

// logDependencyCheckFailure mirrors cmd/dev-health-worker/dependencies.go's
// and cmd/dev-health-reconciler/dependencies.go's helper of the same name
// (CHAOS-5435). health.Registry never surfaces a CheckFunc's returned error
// anywhere -- before this, ClickHouseReady/DomainPostgresReady/ValkeyReady
// each collapsed their underlying failure into the bare
// errStreamDependencyUnavailable sentinel, so an operator saw only
// failed_checks=<name> with no reason anywhere. On 2026-09-07
// go-stream-ingest/-external/-pagerduty sat not_ready for 9 hours with no
// logged cause because DomainPostgresReady's CheckDomainAuthorization error
// was one of them (CHAOS-5454); ClickHouseReady's Ping and ValkeyReady's Do
// are the same shape and are fixed in the same commit, chris's standing rule
// that every touched failure path gets telemetry. err is always this
// package's own postgres.CheckDomainAuthorization result, or the
// driver/client library's own Ping/Do error -- codex r1 (CHAOS-5454) traced
// pgx/clickhouse-go's own error paths and found they can include the
// target host/address and the configured Postgres user/database or
// ClickHouse auth_db name, never a password or a full DSN/URI. That is the
// same class of address/role material CheckDomainAuthorization's own
// ErrUnavailable wrapping already accepts logging (see this package's other
// readiness paths and CHAOS-5435's precedent) -- still safe to log, just
// not the stronger "no connection-string material at all" claim.
//
// Not shared via an internal package: CHAOS-5435 gave the worker and
// reconciler each their own unexported copy of this method rather than one
// shared helper -- their sentinel types and log message text are per-binary,
// and extracting a shared helper would mean widening a currently-internal
// type or threading a check-name/logger pair through a new exported seam for
// three near-identical 10-line copies. This keeps the stream runner
// consistent with that precedent instead of introducing a third shape.
func (storage *productionStreamStorage) logDependencyCheckFailure(ctx context.Context, check string, err error) {
	if storage == nil || storage.logger == nil || err == nil {
		return
	}
	storage.logger.ErrorContext(ctx, "stream runner readiness dependency check failed",
		"error_category", "dependency_unavailable",
		"check", check,
		"error", err.Error(),
	)
}

func (storage *productionStreamStorage) ValkeyReady(ctx context.Context) error {
	if storage == nil || storage.valkey == nil {
		return errStreamDependencyUnavailable
	}
	if err := storage.valkey.Do(ctx, storage.valkey.B().Ping().Build()).Error(); err != nil {
		storage.logDependencyCheckFailure(ctx, "valkey", err)
		return errStreamDependencyUnavailable
	}
	return nil
}

func (storage *productionStreamStorage) Handler(kind streamHandlerKind, observer streamhandlers.ExternalIngestObserver) (streamrunner.Handler, error) {
	if storage == nil {
		return nil, errStreamDependencyUnavailable
	}
	if storage.clickHouse == nil {
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
		dispatcher, err := externalrecompute.NewPostgresNativeDispatcher(storage.domainPool)
		if err != nil {
			return nil, err
		}
		storage.recompute, err = newExternalRecomputeController(recomputeStore, dispatcher, storage.logger)
		if err != nil {
			return nil, err
		}
		return streamhandlers.NewExternalIngestHandler(repository, sink, storage.recompute, observer)
	case pagerdutyHandlerKind:
		// This consumer is now the ONLY consumer of pagerduty-webhooks:*.
		// CHAOS-4105 deleted the Python Celery task, the ingress .delay call
		// that fed it, and the transport flag that used to arbitrate between
		// the two runtimes; nothing else reconciles these entries or XDELs
		// them, so there is no ownership precondition left to check.
		//
		// Receipts and reconciliation are both native Go: the receipt store
		// owns the crash window, and the reconciler writes the canonical rows
		// through the same providersync effect sinks the pull route uses.
		receipts, err := pagerduty.NewPostgresReceiptStore(storage.domainPool)
		if err != nil {
			return nil, err
		}
		reconciler, err := pagerduty.NewNativeReconciler(pagerduty.NativeReconcilerConfig{
			Pool:        storage.domainPool,
			Entitlement: providersync.PostgresIncidentEntitlement{Pool: storage.domainPool},
			Receipts:    receipts,
			Sinks:       storage.pagerDutyWebhookSinks,
			Hydrator:    storage.pagerDutyIncidentHydrator,
			Metrics:     storage.pagerduty.metrics,
		})
		if err != nil {
			return nil, err
		}
		return pagerduty.NewHandler(receipts, reconciler)
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
	openStorage func(context.Context, config.Config, *slog.Logger) (streamStorage, error)
}

var productionStreamDependencySources = streamDependencySources{
	openStorage: openProductionStreamStorage,
}

func configureStreamRunnerDependenciesWithLogger(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
) ([]lifecycle.Component, error) {
	return configureStreamRunnerDependenciesWithSources(
		ctx,
		cfg,
		registry,
		productionStreamDependencySources,
		logger,
	)
}

func configureStreamRunnerDependenciesWithSources(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	sources streamDependencySources,
	logger *slog.Logger,
) ([]lifecycle.Component, error) {
	if registry == nil || sources.openStorage == nil {
		return nil, dependencyUnavailable("stream_runner_sources_unavailable")
	}
	storage, err := sources.openStorage(ctx, cfg, logger)
	if err != nil || storage == nil {
		return nil, processreadiness.RegisterUnavailable(
			registry,
			"clickhouse",
			"domain_postgres",
			"posture_manifest_lockstep",
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
	// CHAOS-5437: posture_manifest_lockstep refuses readiness the instant
	// this binary's compiled-in posture manifest is older than what
	// go-worker-migrate has applied -- see cmd/dev-health-worker's identical
	// check for the full incident this closes (three stream runners sat
	// not_ready for 9h with no crash loop and no alert).
	postureGuard := postureguard.New(
		"dev-health-stream-runner", storage.PostureManifestLockstep, postgres.PostureManifestDigest(),
	)
	if err := registry.RegisterMetrics("posture_manifest_lockstep", postureGuard); err != nil {
		return nil, err
	}
	storageChecks := []struct {
		name  string
		check health.CheckFunc
	}{
		{name: "clickhouse", check: storage.ClickHouseReady},
		{name: "domain_postgres", check: storage.DomainPostgresReady},
		// wrapStreamRunnerReadinessCheckWithLogging (codex review finding,
		// CHAOS-5437 round 1): registering postureGuard.Ready directly left
		// its formatted refusal -- naming both digests, "rebuild/redeploy
		// this image" -- unreachable, since health.Registry never surfaces a
		// CheckFunc's returned error anywhere (see cmd/dev-health-worker's
		// identical logDependencyCheckFailure doc comment). Every OTHER
		// binary's posture_manifest_lockstep check already logs; this one
		// silently did not.
		{name: "posture_manifest_lockstep", check: wrapStreamRunnerReadinessCheckWithLogging(logger, "posture_manifest_lockstep", postureGuard.Ready)},
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
			runner, err := buildStreamRunner(storage, registry, specification.kind, specification.config, logger, nil)
			if err != nil {
				if errors.Is(err, streamrunner.ErrInvalidConfig) {
					return nil, err
				}
				return nil, nil
			}
			components = append(components, runner)
		}
	case "external":
		// The external profile is the only one that ingests customer-pushed
		// records, so it is the only one with refusals and project-transition
		// writes to report. Registering the collector on the health registry
		// is what makes the counters REACHABLE rather than merely constructed:
		// without it the handler would hold a live observer whose numbers no
		// scrape ever reads.
		observer, err := newExternalIngestMetrics(registry)
		if err != nil {
			return nil, err
		}
		runner, err := buildStreamRunner(
			storage,
			registry,
			externalIngestHandlerKind,
			externalIngestRunnerConfig(replicas),
			logger,
			observer,
		)
		if err != nil {
			if errors.Is(err, streamrunner.ErrInvalidConfig) {
				return nil, err
			}
			return nil, nil
		}
		components = append(components, storage.ControlComponents()...)
		components = append(components, runner)
	case "pagerduty":
		runner, err := buildStreamRunner(
			storage,
			registry,
			pagerdutyHandlerKind,
			pagerdutyRunnerConfig(replicas),
			logger,
			nil,
		)
		if err != nil {
			if errors.Is(err, streamrunner.ErrInvalidConfig) {
				return nil, err
			}
			return nil, nil
		}
		components = append(components, runner)
	default:
		return nil, streamrunner.ErrInvalidConfig
	}
	streamConsumerConfigured = true
	closeOnError = false
	return components, nil
}

// newExternalIngestMetrics builds the external profile's Prometheus collector
// and registers it on the operator endpoint.
//
// Registration failure is returned rather than tolerated. A duplicate or
// unsafe metrics name means this fragment would be missing from every scrape,
// and a worker that starts anyway is a worker whose refusal counter reads zero
// for the most trustworthy-looking reason there is: nothing is publishing it.
func newExternalIngestMetrics(registry *health.Registry) (*jobruntime.MetricsCollector, error) {
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		return nil, err
	}
	// NOT "external_ingest": streamrunner.New registers its own runner
	// fragment under the stream config's Name, which is exactly that. A
	// duplicate name is refused by the registry, and because the external
	// profile treats a non-ErrInvalidConfig build failure as "defer
	// construction", the collision surfaced as a profile that silently built
	// no consumers at all rather than as an error anyone would read.
	if err := registry.RegisterMetrics("external_ingest_records", collector); err != nil {
		return nil, err
	}
	return collector, nil
}

func buildStreamRunner(
	storage streamStorage,
	registry *health.Registry,
	kind streamHandlerKind,
	cfg streamrunner.Config,
	logger *slog.Logger,
	observer streamhandlers.ExternalIngestObserver,
) (*streamrunner.Runner, error) {
	handler, err := storage.Handler(kind, observer)
	if err != nil {
		return nil, err
	}
	transport, err := storage.NewTransport()
	if err != nil {
		return nil, err
	}
	cfg.Logger = logger
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

// pagerdutyRunnerConfig preserves the Python webhook contract. The producer
// XADDs to "pagerduty-webhooks:{binding_id}", so discovery is a two-segment
// wildcard rather than the three-segment ingest shape.
//
// The Celery task uses max_retries=3, i.e. four total attempts before it
// dead-letters, so MaxDeliveries is 4. Its backoff is 30s/60s/120s; a stream
// runner redelivers on a fixed reclaim threshold instead of an exponential
// curve, so the budget is bounded by MaxDeliveries rather than by the schedule.
//
// ReclaimIdle must exceed pagerduty.ReceiptLease. A reclaim has to imply the
// previous holder can no longer own the receipt; if it does not, the reclaimed
// entry reads as in-flight on every delivery and is dead-lettered without a
// single reconciliation attempt. pagerdutyReclaimIdleExceedsReceiptLease pins
// the invariant so tuning either value alone fails the build's tests.
//
// Not a singleton: a binding's entries are exclusive to one consumer through
// the group PEL, and duplicate work across replicas is fenced by the Postgres
// receipt claim token. Only external-ingest needs a single lane, because its
// recompute controller carries cross-batch state.
// pagerdutyReclaimIdle is one minute past the receipt lease so an expired lease
// is always observable before the entry is redelivered.
const pagerdutyReclaimIdle = pagerduty.ReceiptLease + time.Minute

func pagerdutyRunnerConfig(replicas int) streamrunner.Config {
	return streamrunner.Config{
		Name: "pagerduty_webhooks", Patterns: []string{"pagerduty-webhooks:*"},
		ConsumerGroup: "pagerduty-webhook-consumers", ConsumerName: "go-pagerduty-webhooks",
		BatchSize: 25, DiscoveryLimit: 10_000, Block: 5 * time.Second,
		ReclaimEvery: 30 * time.Second, ReclaimIdle: pagerdutyReclaimIdle,
		MaxDeliveries: 4, ShutdownDrain: 30 * time.Second,
		ConfiguredReplicas: replicas,
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

// pagerDutyWebhookSinks builds the effect writers one webhook reconciliation
// commits through. They are the SAME types cmd/dev-health-worker constructs
// for the native pull route (provider_sync.go), with the same entitlement and
// the same ClickHouse connection -- the only difference is the lease, which
// here is the webhook's receipt claim rather than a sync-unit lease.
func (storage *productionStreamStorage) pagerDutyWebhookSinks(
	providerInstanceID string, lease providerfoundation.LeaseGuard,
) providersync.PagerDutyWebhookSinks {
	entitlement := providersync.PostgresIncidentEntitlement{Pool: storage.domainPool}
	return providersync.PagerDutyWebhookSinks{
		IncidentFamily: providersync.PagerDutyIncidentFamilyClickHouseEffects{
			Conn: storage.clickHouse, Lease: lease, ProviderInstanceID: providerInstanceID,
			Entitlement: entitlement, Metrics: storage.pagerduty.metrics,
		},
		Services: providersync.PagerDutyServicesClickHouseEffects{
			Conn: storage.clickHouse, Lease: lease, ProviderInstanceID: providerInstanceID,
			Entitlement: entitlement, Metrics: storage.pagerduty.metrics,
		},
		Users: providersync.PagerDutyUsersClickHouseEffects{
			Conn: storage.clickHouse, Lease: lease, ProviderInstanceID: providerInstanceID,
			Entitlement: entitlement, Metrics: storage.pagerduty.metrics,
		},
		Responders: providersync.PagerDutyWebhookRespondersClickHouseEffects{
			Conn: storage.clickHouse, Lease: lease, ProviderInstanceID: providerInstanceID,
			Entitlement: entitlement, Metrics: storage.pagerduty.metrics,
		},
	}
}

// pagerDutyIncidentHydrator builds the REST fallback for a webhook payload too
// sparse to normalize. It resolves the binding's own credential through the
// shared CredentialResolver, so OAuth refresh and region selection stay in one
// place rather than being reimplemented for the webhook path.
func (storage *productionStreamStorage) pagerDutyIncidentHydrator(
	graph pagerduty.LockedGraph, lease providerfoundation.LeaseGuard,
) providersync.PagerDutyIncidentHydrator {
	doer := &http.Client{
		Timeout: 45 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return pagerduty.CredentialIncidentHydrator{
		Resolver: providerfoundation.CredentialResolver{
			Repository: providerfoundation.PostgresCredentialRepository{Pool: storage.domainPool},
			Decryptor:  storage.pagerduty.cipher,
			Hydrator: providerfoundation.PagerDutyOAuthHydrator{
				Repository: providerfoundation.PostgresPagerDutyOAuthTokenRepository{
					Pool: storage.domainPool,
				},
				Cipher:          storage.pagerduty.cipher,
				Doer:            doer,
				AppClientID:     storage.pagerduty.oauthClientID,
				AppClientSecret: storage.pagerduty.oauthSecret,
			},
		},
		Doer:         doer,
		Retry:        providerfoundation.DefaultRetryPolicy(),
		Lease:        lease,
		OrgID:        graph.OrgID,
		CredentialID: graph.CredentialID,
	}
}
