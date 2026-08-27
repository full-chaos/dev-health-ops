// Package config defines the shared runtime-shell configuration contract.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// DefaultShutdownTimeout is the shutdown grace used when neither
// --shutdown-timeout nor DEV_HEALTH_SHUTDOWN_TIMEOUT is supplied. The worker
// compares against it to tell an unset timeout from one an operator chose.
const DefaultShutdownTimeout = 30 * time.Second

const (
	defaultHTTPAddress       = ":8080"
	defaultShutdownTimeout   = DefaultShutdownTimeout
	maximumShutdownTimeout   = 3 * time.Hour
	defaultHealthCheckTimout = 2 * time.Second
	defaultDomainMaxConns    = 4
	defaultQueueMaxConns     = 2
	// 2 matches the checked-in per-process coordinator_max_connections in
	// deploy/go-workers/deployment.json for every coordinator process today
	// (reconciler, scheduler, worker-operator).
	defaultCoordinatorMaxConns = 2
	defaultCompletedRetention  = 7 * 24 * time.Hour
	defaultCancelledRetention  = 30 * 24 * time.Hour
	defaultDiscardedRetention  = 30 * 24 * time.Hour
	defaultJobCleanerTimeout   = 30 * time.Second
	// defaultSyncObservationTimeout mirrors syncreconciler's own unconfigured
	// default (CHAOS-4092) so a deployment that never sets the override sees
	// no behavior change from this option's introduction.
	defaultSyncObservationTimeout = 2 * time.Second
	// DefaultSyncObservationTimeout exports the same value for callers that
	// need to tell "the reconciler's own baked-in fallback" apart from an
	// operator's genuine SYNC_OBSERVATION_TIMEOUT override (CHAOS-4239):
	// Load never leaves SyncObservationTimeout at Go's zero value for the
	// reconciler service, so a bare "!= 0" check cannot make that
	// distinction on its own. See cmd/dev-health-reconciler/dependencies.go.
	DefaultSyncObservationTimeout = defaultSyncObservationTimeout
	defaultRiverDatabaseSchema    = "river"
	defaultDomainDatabaseRole     = "devhealth_domain"
	defaultQueueDatabaseRole      = "devhealth_queue"
	// The coordinator role of the CHAOS-3033 Option B split. Provisioned
	// alongside the other two by docker/init-extra-dbs.sh (local dev) and
	// scripts/worker/provision_river_roles.sql (deployed environments).
	defaultCoordinatorDatabaseRole = "devhealth_coordinator"
	defaultStreamReplicas          = 1
	// The OpenTelemetry defaults mirror src/dev_health_ops/tracing.py exactly,
	// so a sync run crossing the Python/Go boundary is sampled consistently.
	defaultOTelServiceName   = "dev-health-ops"
	defaultOTelEnvironment   = "production"
	defaultOTelEndpoint      = "localhost:4317"
	defaultOTelSampleRate    = 0.1
	localStatusMappingPath   = "/app/config/status_mapping.yaml"
	localInvestmentAreasPath = "/app/config/investment_areas.yaml"
)

const (
	devHealthEnv = "DEV_HEALTH_ENV"
)

// QueueControlMode describes the endpoint semantics promised by the operator.
// Direct PostgreSQL and PgBouncer session pooling preserve River's required
// session semantics. Transaction pooling cannot propagate cancellation to a
// running River worker.
type QueueControlMode string

const (
	QueueControlDirect      QueueControlMode = "direct"
	QueueControlSession     QueueControlMode = "session"
	QueueControlTransaction QueueControlMode = "transaction"
)

// Spec describes the immutable configuration surface of one executable.
type Spec struct {
	Service string
	// Profile is already resolved and validated by the caller; config does not
	// own profile selection (CHAOS-3875).
	Profile       string
	RequireQueues bool
	// Queues is flag-only on purpose (CHAOS-3875): every deploy artifact passes
	// --queues, so a parallel environment path would only be a second way to
	// say the same thing.
	Queues []string
	// Overrides carries the values the operator supplied as flags, keyed by the
	// environment variable each flag shadows. Resolution is flag > env >
	// default: an override is consulted first and the environment is the
	// 12-factor fallback beneath it.
	//
	// This is precedence, not a conflict: CHAOS-4020 moves deployed
	// configuration into `command:` while host .env files still carry the old
	// variables, and a configuration that hard-failed whenever both surfaces
	// named the same setting could not be rolled out one surface at a time.
	// The shadowed environment value is reported at startup instead.
	Overrides map[string]string
	LookupEnv secrets.LookupEnv
}

// Config contains typed runtime settings. Sensitive values use secrets.Value,
// which redacts itself from formatting, slog, and JSON.
type Config struct {
	Service string
	Profile string
	Queues  []string
	// WorkerInstanceID is generated once inside the process and is the one
	// River client's attempted_by identity.
	WorkerInstanceID string
	// WorkerQueueConcurrency is populated from the selected deployment group
	// and must cover Worker Queues exactly. Worker families may use it, but they
	// must not define queue capacity in application code.
	WorkerQueueConcurrency map[string]int
	// WorkerGroup is an observability identity only. It never selects queues or
	// changes handler construction.
	WorkerGroup string
	// EnvOnlySettings names the settings that were resolved from the
	// environment even though a canonical flag exists for them. The shell warns
	// about each at startup so a deployment still configured the old way is
	// visible rather than merely working.
	EnvOnlySettings []string

	HTTPAddress string
	// UnreclaimableSweepMode is the resolved off|shadow|active choice for the
	// reconciler's unreclaimable-dispatching sweep. Flag wins over env; empty
	// means neither was supplied and the sweep uses its shadow default.
	UnreclaimableSweepMode string
	ShutdownTimeout        time.Duration
	// ShutdownTimeoutExplicit records whether ShutdownTimeout came from the
	// operator rather than the package default. The worker's drain budget is
	// ShutdownTimeout minus a finalization buffer and must cover the longest
	// selected job timeout, which the 30s default cannot do for any real queue
	// selection -- so an unset timeout is derived from the selection instead of
	// failing every default-configured worker (CHAOS-3873).
	ShutdownTimeoutExplicit bool
	HealthCheckTimeout      time.Duration
	LogLevel                slog.Level

	DomainDatabaseURI      secrets.Value
	QueueDatabaseURI       secrets.Value
	CoordinatorDatabaseURI secrets.Value
	ClickHouseURI          secrets.Value
	ValkeyURI              secrets.Value
	SettingsEncryptionKey  secrets.Value
	SettingsEncryptionSalt secrets.Value
	PagerDutyOAuthClientID secrets.Value
	PagerDutyOAuthSecret   secrets.Value

	QueueDatabaseMode           QueueControlMode
	CoordinatorDatabaseMode     QueueControlMode
	RiverDatabaseSchema         string
	DomainDatabaseRole          string
	QueueDatabaseRole           string
	CoordinatorDatabaseRole     string
	DomainTransactionPooler     bool
	DomainDatabaseMaxConns      int32
	QueueDatabaseMaxConns       int32
	CoordinatorDatabaseMaxConns int32
	CompletedJobRetention       time.Duration
	CancelledJobRetention       time.Duration
	DiscardedJobRetention       time.Duration
	RiverJobCleanerTimeout      time.Duration
	// SyncObservationTimeout bounds the sync-dispatch mutation pipeline's
	// outer per-step envelope (reconciler-only). CHAOS-4092 introduced this
	// override when exceeding it was still fatal to the whole process,
	// exactly as the hardcoded 2s was; CHAOS-4239 changed that underlying
	// behavior structurally -- syncreconciler.Loop no longer tears the
	// process down for this. Each pipeline stage now runs under its own
	// bounded budget (syncreconciler.DefaultStageBudgets), the composed
	// envelope is derived from their sum (see dependencies.go), and
	// exceeding EITHER a stage's own budget or this outer envelope degrades
	// only that tick (syncreconciler.ErrDegradedStage /
	// syncreconciler.ErrStepEnvelopeExceeded -- logged, counted, self-healing
	// on the next tick that fits) instead of exiting the process. This
	// option still lets an operator override the composed default in place
	// of a redeploy, honored even when it undercuts the composition (WARNed,
	// not rejected). It remains a liveness knob, not a correctness one:
	// syncreconciler.LoopConfig.validate bounds it to [10ms, 30s] regardless
	// of what is configured here.
	SyncObservationTimeout time.Duration
	// SyncObservationTimeoutExplicit is true only when SYNC_OBSERVATION_TIMEOUT /
	// --sync-observation-timeout was actually present in the environment/CLI
	// (CHAOS-4239). SyncObservationTimeout alone cannot tell "the operator
	// explicitly chose this value" apart from "nobody configured anything and
	// Load's own fallback (defaultSyncObservationTimeout) filled it in" --
	// both produce the identical 2s. A caller that needs that distinction
	// (cmd/dev-health-reconciler/dependencies.go, composing the mutation
	// loop's outer envelope from syncreconciler.DefaultStageBudgets instead)
	// reads this field rather than comparing the value to a sentinel, which
	// would silently ignore an operator who deliberately chose exactly the
	// package default and would need hand-updating every time that default
	// number changed.
	SyncObservationTimeoutExplicit bool
	OperationalBridgeURL           string
	OperationalBridgeToken         secrets.Value
	OperationalBridgeTimeout       time.Duration
	OperationalBridgeAllowInsecure bool
	StreamConfiguredReplicas       int

	// DailyPartitionLivenessCeilingBase/PerRepo (CHAOS-4316) bound one
	// daily_partition ComputePartition call from the Go side, as a backstop
	// behind the compatibility bridge's own progress-based watchdog
	// (worker_metrics.py _watch_progress_stall). runWithLeaseRenewal
	// otherwise renews the partition's lease forever as long as the cheap
	// PG UPDATE succeeds, independent of whether the bridge call is making
	// progress -- and the HTTP client to the bridge deliberately has no
	// Client.Timeout. The ceiling is base + per-repo*len(partition.RepoIDs),
	// the same work-size-derived shape the Python side uses (never a flat
	// wall-clock number).
	//
	// Defaults are deliberately larger than the Python watchdog's own hard
	// ceiling (120s base + 90s/repo, x3 multiplier) at every realistic
	// partition size (dailyRepositoryPartitionSize defaults to 3 repos per
	// partition) so the bridge's finer-grained, better-telemetered
	// classification always wins the race under normal conditions -- e.g.
	// at 3 repos, Python's own hard ceiling is ~19.5 minutes and this
	// backstop is ~25 minutes. This backstop exists for when that watchdog
	// itself cannot run (e.g. the bridge's event loop is wedged), not as
	// the primary mechanism. If either side's constants are retuned,
	// re-check that this one still exceeds the Python hard ceiling at the
	// deployment's actual dailyRepositoryPartitionSize.
	//
	// Ships ON by default with these constants (team-lead ruling
	// 2026-08-26): deployed compose/helm manifests do not set new env vars,
	// so an opt-in design would silently never activate in production. Set
	// DEV_HEALTH_DAILY_PARTITION_LIVENESS_CEILING_BASE explicitly to "0" to
	// disable the backstop entirely -- the only sanctioned opt-out; any
	// other value is validated against the [30s, 30m] bound like any other
	// duration setting.
	DailyPartitionLivenessCeilingBase    time.Duration
	DailyPartitionLivenessCeilingPerRepo time.Duration

	// WorkerGithubWorkItemsInvestmentConfigPath are explicit production paths
	// for the two Python-parity config engines. Production has no source-relative
	// default; a local deployment falls back to artifacts packaged at fixed
	// image paths. cmd/dev-health-worker validates both paths and rejects
	// ambient STATUS_MAPPING_PATH overrides.
	WorkerGithubWorkItemsStatusMappingPath    string
	WorkerGithubWorkItemsInvestmentConfigPath string

	// PagerDutyWebhookTransport names the single owner of the PagerDuty webhook
	// stream. The Python ingress dispatches its Celery task only while this is
	// "celery"; the Go stream runner constructs its consumer only when it is
	// "river". One switch, read by both runtimes, so the stream can never have
	// two owners reconciling and deleting the same entries.
	PagerDutyWebhookTransport string
}

// PagerDuty webhook transports. Celery is the default because it is what
// production runs today: an unset or unrecognized value must never silently
// hand ownership to the runtime that is not yet cut over.
const (
	PagerDutyTransportCelery = "celery"
	PagerDutyTransportRiver  = "river"
)

// Load reads and validates explicit process arguments plus environment-backed
// dependencies. A command argument and its environment fallback may not both
// be set.
func Load(spec Spec) (Config, error) {
	environment := spec.LookupEnv
	if environment == nil {
		environment = os.LookupEnv
	}
	if err := validateOverrides(spec.Overrides); err != nil {
		return Config{}, err
	}
	lookup := layeredLookup(spec.Overrides, environment)

	cfg := Config{Service: spec.Service}
	cfg.EnvOnlySettings = envOnlySettings(spec, environment)
	if err := validateName("service", cfg.Service); err != nil {
		return Config{}, err
	}

	cfg.HTTPAddress = envOrDefault(lookup, "DEV_HEALTH_HTTP_ADDR", defaultHTTPAddress)
	if _, _, err := net.SplitHostPort(cfg.HTTPAddress); err != nil {
		return Config{}, fmt.Errorf(
			"%s must be a host:port address", settingLabel("DEV_HEALTH_HTTP_ADDR"),
		)
	}

	var err error
	cfg.ShutdownTimeout, err = durationEnv(
		lookup,
		"DEV_HEALTH_SHUTDOWN_TIMEOUT",
		defaultShutdownTimeout,
		500*time.Millisecond,
		maximumShutdownTimeout,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.ShutdownTimeoutExplicit = settingConfigured(lookup, "DEV_HEALTH_SHUTDOWN_TIMEOUT")
	// CHAOS-4005's sweep resolves through the same layered lookup as every
	// other setting: --unreclaimable-sweep is an override keyed by
	// SYNC_UNRECLAIMABLE_SWEEP, so flag > env > default falls out of the shared
	// mechanism rather than a private branch. Empty means neither surface
	// supplied a value and ParseSweepMode applies the shadow default.
	cfg.UnreclaimableSweepMode = strings.TrimSpace(
		envOrDefault(lookup, "SYNC_UNRECLAIMABLE_SWEEP", ""),
	)
	cfg.OperationalBridgeAllowInsecure, err = boolEnv(
		lookup, "WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE", false,
	)
	if err != nil {
		return Config{}, err
	}
	// CHAOS-4054: these default to the artifacts every worker image ships
	// (docker/go-worker.Dockerfile copies both into /app/config in the runtime
	// stage). They used to default only under the local all-routes preset,
	// because outside it the work-item route was switched off and never needed
	// them. With capability always on, a worker that consumes the provider-unit
	// queue must be able to serve the work-item family, so the default has to
	// hold everywhere the binary runs — a deployment that leaves these unset is
	// not opting out of anything, it is running the image that has them.
	cfg.WorkerGithubWorkItemsStatusMappingPath = envOrDefault(
		lookup,
		"WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH",
		localStatusMappingPath,
	)
	cfg.WorkerGithubWorkItemsInvestmentConfigPath = envOrDefault(
		lookup,
		"WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH",
		localInvestmentAreasPath,
	)
	cfg.HealthCheckTimeout, err = durationEnv(
		lookup,
		"DEV_HEALTH_HEALTH_CHECK_TIMEOUT",
		defaultHealthCheckTimout,
		50*time.Millisecond,
		30*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.LogLevel, err = logLevelEnv(lookup)
	if err != nil {
		return Config{}, err
	}

	cfg.Profile = spec.Profile
	cfg.Queues, err = queueSelection(spec, lookup)
	if err != nil {
		return Config{}, err
	}
	cfg.WorkerGroup, cfg.WorkerQueueConcurrency, err = workerQueueRuntime(spec, lookup, cfg.Queues)
	if err != nil {
		return Config{}, err
	}

	secretTargets := []struct {
		name   string
		target *secrets.Value
	}{
		{name: "POSTGRES_URI", target: &cfg.DomainDatabaseURI},
		{name: "WORKER_DATABASE_URI", target: &cfg.QueueDatabaseURI},
		// Optional here on purpose: only coordinator binaries require it, and
		// they enforce that themselves through
		// postgres.RuntimeConfig.RequireCoordinator. A domain-only worker must
		// not fail to start merely because this is unset.
		{name: "COORDINATOR_DATABASE_URI", target: &cfg.CoordinatorDatabaseURI},
		{name: "CLICKHOUSE_URI", target: &cfg.ClickHouseURI},
		{name: "VALKEY_URI", target: &cfg.ValkeyURI},
		{name: "SETTINGS_ENCRYPTION_KEY", target: &cfg.SettingsEncryptionKey},
		{name: "SETTINGS_ENCRYPTION_SALT", target: &cfg.SettingsEncryptionSalt},
		{name: "PAGER_DUTY_CLIENT_ID", target: &cfg.PagerDutyOAuthClientID},
		{name: "PAGER_DUTY_SECRET", target: &cfg.PagerDutyOAuthSecret},
		{name: "WORKER_OPERATIONAL_BRIDGE_TOKEN", target: &cfg.OperationalBridgeToken},
	}
	for _, item := range secretTargets {
		value, _, resolveErr := secrets.Resolve(item.name, lookup)
		if resolveErr != nil {
			return Config{}, resolveErr
		}
		*item.target = value
	}
	cfg.OperationalBridgeURL = envOrDefault(
		lookup, "WORKER_OPERATIONAL_BRIDGE_URL", "",
	)
	cfg.OperationalBridgeTimeout, err = durationEnv(
		lookup,
		"WORKER_OPERATIONAL_BRIDGE_TIMEOUT",
		10*time.Second,
		100*time.Millisecond,
		30*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.DailyPartitionLivenessCeilingBase, err = durationEnvAllowExplicitZero(
		lookup,
		"DEV_HEALTH_DAILY_PARTITION_LIVENESS_CEILING_BASE",
		10*time.Minute,
		30*time.Second,
		30*time.Minute,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.DailyPartitionLivenessCeilingPerRepo, err = durationEnv(
		lookup,
		"DEV_HEALTH_DAILY_PARTITION_LIVENESS_CEILING_PER_REPO",
		5*time.Minute,
		1*time.Second,
		30*time.Minute,
	)
	if err != nil {
		return Config{}, err
	}

	postgresSchemes := []string{
		"postgres",
		"postgresql",
		"postgres+asyncpg",
		"postgresql+asyncpg",
		"postgresql+psycopg",
		"postgresql+psycopg2",
	}
	if err := validateURI("POSTGRES_URI", cfg.DomainDatabaseURI, postgresSchemes...); err != nil {
		return Config{}, err
	}
	if err := validateURI("WORKER_DATABASE_URI", cfg.QueueDatabaseURI, postgresSchemes...); err != nil {
		return Config{}, err
	}
	if err := validateURI("COORDINATOR_DATABASE_URI", cfg.CoordinatorDatabaseURI, postgresSchemes...); err != nil {
		return Config{}, err
	}
	if err := validateURI("CLICKHOUSE_URI", cfg.ClickHouseURI, "clickhouse", "http", "https"); err != nil {
		return Config{}, err
	}
	if err := validateURI("VALKEY_URI", cfg.ValkeyURI, "redis", "rediss", "unix"); err != nil {
		return Config{}, err
	}

	cfg.QueueDatabaseMode, err = queueControlModeEnv(lookup)
	if err != nil {
		return Config{}, err
	}
	cfg.CoordinatorDatabaseMode, err = coordinatorDatabaseModeEnv(lookup)
	if err != nil {
		return Config{}, err
	}
	cfg.PagerDutyWebhookTransport = strings.ToLower(strings.TrimSpace(envOrDefault(
		lookup, "PAGERDUTY_WEBHOOK_TRANSPORT", PagerDutyTransportCelery,
	)))
	if cfg.PagerDutyWebhookTransport != PagerDutyTransportCelery &&
		cfg.PagerDutyWebhookTransport != PagerDutyTransportRiver {
		return Config{}, fmt.Errorf(
			"PAGERDUTY_WEBHOOK_TRANSPORT must be %s or %s",
			PagerDutyTransportCelery, PagerDutyTransportRiver,
		)
	}
	cfg.RiverDatabaseSchema = envOrDefault(lookup, "RIVER_DATABASE_SCHEMA", defaultRiverDatabaseSchema)
	if err := validateIdentifier("RIVER_DATABASE_SCHEMA", cfg.RiverDatabaseSchema); err != nil {
		return Config{}, err
	}
	cfg.DomainDatabaseRole = envOrDefault(lookup, "RIVER_DOMAIN_DATABASE_ROLE", defaultDomainDatabaseRole)
	if err := validateIdentifier("RIVER_DOMAIN_DATABASE_ROLE", cfg.DomainDatabaseRole); err != nil {
		return Config{}, err
	}
	cfg.QueueDatabaseRole = envOrDefault(lookup, "RIVER_QUEUE_DATABASE_ROLE", defaultQueueDatabaseRole)
	if err := validateIdentifier("RIVER_QUEUE_DATABASE_ROLE", cfg.QueueDatabaseRole); err != nil {
		return Config{}, err
	}
	if cfg.DomainDatabaseRole == cfg.QueueDatabaseRole {
		return Config{}, fmt.Errorf("RIVER_DOMAIN_DATABASE_ROLE and RIVER_QUEUE_DATABASE_ROLE must be distinct")
	}
	cfg.CoordinatorDatabaseRole = envOrDefault(
		lookup, "RIVER_COORDINATOR_DATABASE_ROLE", defaultCoordinatorDatabaseRole,
	)
	if err := validateIdentifier("RIVER_COORDINATOR_DATABASE_ROLE", cfg.CoordinatorDatabaseRole); err != nil {
		return Config{}, err
	}
	// Checked unconditionally, even for domain-only processes: the three role
	// names are a deployment-wide invariant, and a collision configured on a
	// worker that never opens a coordinator pool is still a misconfiguration
	// that would be silently inherited by the next process to opt in.
	if cfg.CoordinatorDatabaseRole == cfg.DomainDatabaseRole ||
		cfg.CoordinatorDatabaseRole == cfg.QueueDatabaseRole {
		return Config{}, fmt.Errorf(
			"RIVER_COORDINATOR_DATABASE_ROLE must be distinct from " +
				"RIVER_DOMAIN_DATABASE_ROLE and RIVER_QUEUE_DATABASE_ROLE",
		)
	}
	cfg.DomainTransactionPooler, err = boolEnv(lookup, "PGBOUNCER_TRANSACTION_MODE", false)
	if err != nil {
		return Config{}, err
	}
	domainMaxConns, err := boundedIntEnv(
		lookup,
		"WORKER_DOMAIN_DATABASE_MAX_CONNS",
		defaultDomainMaxConns,
		1,
		16,
	)
	if err != nil {
		return Config{}, err
	}
	queueMaxConns, err := boundedIntEnv(
		lookup,
		"WORKER_DATABASE_MAX_CONNS",
		defaultQueueMaxConns,
		1,
		4,
	)
	if err != nil {
		return Config{}, err
	}
	// 1..4 mirrors deploymentcontract's CoordinatorMaxConnections bound: the
	// coordinator connection is direct and server-counted, so it shares the
	// queue-control ceiling rather than the PgBouncer-pooled domain ceiling.
	coordinatorMaxConns, err := boundedIntEnv(
		lookup,
		"WORKER_COORDINATOR_DATABASE_MAX_CONNS",
		defaultCoordinatorMaxConns,
		1,
		4,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.DomainDatabaseMaxConns = int32(domainMaxConns)
	cfg.QueueDatabaseMaxConns = int32(queueMaxConns)
	cfg.CoordinatorDatabaseMaxConns = int32(coordinatorMaxConns)

	cfg.CompletedJobRetention, err = durationEnv(
		lookup,
		"RIVER_COMPLETED_JOB_RETENTION",
		defaultCompletedRetention,
		24*time.Hour,
		365*24*time.Hour,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.CancelledJobRetention, err = durationEnv(
		lookup,
		"RIVER_CANCELLED_JOB_RETENTION",
		defaultCancelledRetention,
		24*time.Hour,
		365*24*time.Hour,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.DiscardedJobRetention, err = durationEnv(
		lookup,
		"RIVER_DISCARDED_JOB_RETENTION",
		defaultDiscardedRetention,
		24*time.Hour,
		365*24*time.Hour,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.RiverJobCleanerTimeout, err = durationEnv(
		lookup,
		"RIVER_JOB_CLEANER_TIMEOUT",
		defaultJobCleanerTimeout,
		5*time.Second,
		5*time.Minute,
	)
	if err != nil {
		return Config{}, err
	}
	// Reconciler-only (CHAOS-4092), gated on cfg.Service rather than parsed
	// unconditionally: durationEnv VALIDATES and can fail Load, unlike (say)
	// UnreclaimableSweepMode's bare envOrDefault. Every Go service reads its
	// configuration from the same shared environment/compose base (see
	// deploy/docker-compose/compose.go-workers.yml's go-worker-env-base), so
	// an unconditional parse here would let a malformed
	// SYNC_OBSERVATION_TIMEOUT meant only for the reconciler fail startup for
	// the scheduler and every worker group too. Bounds match syncreconciler's
	// own minObservationTimeout/maxObservationTimeout; LoopConfig.validate
	// re-checks them, so this is belt-and-suspenders against the two
	// constant sets drifting, not the only enforcement.
	if cfg.Service == "dev-health-reconciler" {
		// Captured directly from lookup, ahead of durationEnv, because
		// durationEnv's contract is "return the fallback when unset" -- it
		// deliberately does not distinguish an absent variable from one set
		// to the empty string, and by design returns the identical value
		// either way an operator could reach by choice or by doing nothing.
		// SyncObservationTimeoutExplicit exists specifically so a caller does not
		// have to guess which happened from the value alone (CHAOS-4239).
		cfg.SyncObservationTimeoutExplicit = settingConfigured(lookup, "SYNC_OBSERVATION_TIMEOUT")
		cfg.SyncObservationTimeout, err = durationEnv(
			lookup,
			"SYNC_OBSERVATION_TIMEOUT",
			defaultSyncObservationTimeout,
			10*time.Millisecond,
			30*time.Second,
		)
		if err != nil {
			return Config{}, err
		}
	} else {
		cfg.SyncObservationTimeout = defaultSyncObservationTimeout
	}
	cfg.StreamConfiguredReplicas, err = boundedIntEnv(
		lookup,
		"DEV_HEALTH_STREAM_REPLICAS",
		defaultStreamReplicas,
		1,
		8,
	)
	if err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// SafeAttrs is the only supported startup-config logging surface. It includes
// booleans for dependency configuration, never the corresponding DSNs.
func (c Config) SafeAttrs() []slog.Attr {
	attrs := []slog.Attr{
		slog.String("service", c.Service),
		slog.String("http_address", c.HTTPAddress),
		slog.Duration("shutdown_timeout", c.ShutdownTimeout),
		slog.Duration("health_check_timeout", c.HealthCheckTimeout),
		slog.String("log_level", c.LogLevel.String()),
		slog.Bool("domain_database_configured", c.DomainDatabaseURI.Configured()),
		slog.Bool("coordinator_database_configured", c.CoordinatorDatabaseURI.Configured()),
		slog.Bool("queue_database_configured", c.QueueDatabaseURI.Configured()),
		slog.String("queue_database_mode", string(c.QueueDatabaseMode)),
		slog.String("coordinator_database_mode", string(c.CoordinatorDatabaseMode)),
		slog.String("river_database_schema", c.RiverDatabaseSchema),
		slog.String("river_domain_database_role", c.DomainDatabaseRole),
		slog.String("river_queue_database_role", c.QueueDatabaseRole),
		slog.String("river_coordinator_database_role", c.CoordinatorDatabaseRole),
		slog.Bool("domain_transaction_pooler", c.DomainTransactionPooler),
		slog.Int("domain_database_max_connections", int(c.DomainDatabaseMaxConns)),
		slog.Int("queue_database_max_connections", int(c.QueueDatabaseMaxConns)),
		slog.Int("coordinator_database_max_connections", int(c.CoordinatorDatabaseMaxConns)),
		slog.Duration("river_completed_job_retention", c.CompletedJobRetention),
		slog.Duration("river_cancelled_job_retention", c.CancelledJobRetention),
		slog.Duration("river_discarded_job_retention", c.DiscardedJobRetention),
		slog.Duration("river_job_cleaner_timeout", c.RiverJobCleanerTimeout),
		slog.Duration("sync_observation_timeout", c.SyncObservationTimeout),
		slog.Bool("operational_bridge_allow_insecure", c.OperationalBridgeAllowInsecure),
		slog.Int("stream_configured_replicas", c.StreamConfiguredReplicas),
		slog.Bool(
			"worker_github_work_items_status_mapping_path_configured",
			c.WorkerGithubWorkItemsStatusMappingPath != "",
		),
		slog.Bool(
			"worker_github_work_items_investment_config_path_configured",
			c.WorkerGithubWorkItemsInvestmentConfigPath != "",
		),
		slog.Bool("clickhouse_configured", c.ClickHouseURI.Configured()),
		slog.Bool("valkey_configured", c.ValkeyURI.Configured()),
		slog.Bool("settings_encryption_key_configured", c.SettingsEncryptionKey.Configured()),
		slog.Bool("settings_encryption_salt_configured", c.SettingsEncryptionSalt.Configured()),
		slog.Bool("pagerduty_oauth_client_id_configured", c.PagerDutyOAuthClientID.Configured()),
		slog.Bool("pagerduty_oauth_secret_configured", c.PagerDutyOAuthSecret.Configured()),
	}
	if c.Profile != "" {
		attrs = append(attrs, slog.String("profile", c.Profile))
	}
	if len(c.Queues) > 0 {
		attrs = append(attrs, slog.String("queues", strings.Join(c.Queues, ",")))
	}
	if c.WorkerGroup != "" {
		attrs = append(attrs,
			slog.String("worker_group", c.WorkerGroup),
			slog.String("queue_workers", formatQueueConcurrency(c.WorkerQueueConcurrency)),
		)
	}
	return attrs
}

func boolEnv(lookup secrets.LookupEnv, key string, fallback bool) (bool, error) {
	raw, ok := lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", key)
	}
	return value, nil
}

func validateIdentifier(key, value string) error {
	if value == "" || len(value) > 63 {
		return fmt.Errorf("%s must be a PostgreSQL identifier", key)
	}
	for index, char := range value {
		if (char >= 'a' && char <= 'z') || char == '_' || (index > 0 && char >= '0' && char <= '9') {
			continue
		}
		return fmt.Errorf("%s must be a lowercase PostgreSQL identifier", key)
	}
	return nil
}

func queueControlModeEnv(lookup secrets.LookupEnv) (QueueControlMode, error) {
	return databaseModeEnv(lookup, "WORKER_DATABASE_MODE", QueueControlDirect)
}

func coordinatorDatabaseModeEnv(lookup secrets.LookupEnv) (QueueControlMode, error) {
	return databaseModeEnv(lookup, "COORDINATOR_DATABASE_MODE", QueueControlDirect)
}

func databaseModeEnv(lookup secrets.LookupEnv, key string, fallback QueueControlMode) (QueueControlMode, error) {
	mode := QueueControlMode(strings.ToLower(envOrDefault(lookup, key, string(fallback))))
	switch mode {
	case QueueControlDirect, QueueControlSession, QueueControlTransaction:
		return mode, nil
	default:
		return "", fmt.Errorf("%s must be direct, session, or transaction", key)
	}
}

func boundedIntEnv(
	lookup secrets.LookupEnv,
	key string,
	fallback, minimum, maximum int,
) (int, error) {
	raw, ok := lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", key, minimum, maximum)
	}
	return value, nil
}

func envOrDefault(lookup secrets.LookupEnv, key, fallback string) string {
	if value, ok := lookup(key); ok && strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

// durationEnvAllowExplicitZero behaves exactly like durationEnv, except a
// value that parses to exactly zero (e.g. "0", "0s") is accepted as a
// deliberate opt-out and returned as-is, bypassing the minimum/maximum
// bounds check. Use only for a setting whose zero value has a defined,
// intentional "disabled" meaning to the caller (CHAOS-4316: an operator
// explicitly disabling the daily-partition liveness ceiling backstop) --
// durationEnv's ordinary bounds still apply to every non-zero value.
func durationEnvAllowExplicitZero(
	lookup secrets.LookupEnv,
	key string,
	fallback, minimum, maximum time.Duration,
) (time.Duration, error) {
	if raw, ok := lookup(key); ok {
		if trimmed := strings.TrimSpace(raw); trimmed != "" {
			if parsed, err := time.ParseDuration(trimmed); err == nil && parsed == 0 {
				return 0, nil
			}
		}
	}
	return durationEnv(lookup, key, fallback, minimum, maximum)
}

func durationEnv(
	lookup secrets.LookupEnv,
	key string,
	fallback, minimum, maximum time.Duration,
) (time.Duration, error) {
	raw, ok := lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration", key)
	}
	if value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %s and %s", key, minimum, maximum)
	}
	return value, nil
}

func logLevelEnv(lookup secrets.LookupEnv) (slog.Level, error) {
	value := strings.ToLower(envOrDefault(lookup, "DEV_HEALTH_LOG_LEVEL", "info"))
	switch value {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf(
			"%s must be debug, info, warn, or error", settingLabel("DEV_HEALTH_LOG_LEVEL"),
		)
	}
}

func queueSelection(spec Spec, lookup secrets.LookupEnv) ([]string, error) {
	if !spec.RequireQueues {
		if len(spec.Queues) > 0 {
			return nil, fmt.Errorf("%s does not accept queue selection", spec.Service)
		}
		return nil, nil
	}
	if spec.Profile != "" {
		return nil, fmt.Errorf("%s cannot combine queue selection with profiles", spec.Service)
	}

	// Queue topology is flag-only on purpose (CHAOS-3875). Every deploy
	// artifact -- Compose, Swarm, raw Kubernetes, and the Helm chart -- passes
	// --queues, so the parallel DEV_HEALTH_QUEUES env path bought nothing but a
	// second way to say the same thing and a conflict branch to detect the two
	// disagreeing.
	rawValues := append([]string(nil), spec.Queues...)
	if len(rawValues) == 0 {
		return nil, errors.New("at least one worker queue is required")
	}

	seen := make(map[string]struct{})
	queues := make([]string, 0, len(rawValues))
	for _, raw := range rawValues {
		for _, item := range strings.Split(raw, ",") {
			queue := strings.TrimSpace(item)
			if !validQueueName(queue) {
				return nil, fmt.Errorf("worker queue %q is not a valid registered queue name", queue)
			}
			if _, duplicate := seen[queue]; duplicate {
				return nil, fmt.Errorf("worker queue %q is selected more than once", queue)
			}
			seen[queue] = struct{}{}
			queues = append(queues, queue)
		}
	}
	slices.Sort(queues)
	return queues, nil
}

func workerQueueRuntime(spec Spec, lookup secrets.LookupEnv, queues []string) (string, map[string]int, error) {
	group := strings.TrimSpace(envOrDefault(lookup, "DEV_HEALTH_WORKER_GROUP", ""))
	encoded := strings.TrimSpace(envOrDefault(lookup, "DEV_HEALTH_QUEUE_CONCURRENCY", ""))
	if !spec.RequireQueues {
		if group != "" || encoded != "" {
			return "", nil, fmt.Errorf("%s does not accept worker queue runtime settings", spec.Service)
		}
		return "", nil, nil
	}
	if group == "" {
		group = "worker"
	}
	groupSetting := settingLabel("DEV_HEALTH_WORKER_GROUP")
	if len(group) > 64 {
		return "", nil, fmt.Errorf("%s exceeds 64 characters", groupSetting)
	}
	if err := validateName(groupSetting, group); err != nil {
		return "", nil, err
	}
	if encoded == "" {
		return "", nil, fmt.Errorf("%s is required", settingLabel("DEV_HEALTH_QUEUE_CONCURRENCY"))
	}
	concurrency := make(map[string]int)
	for _, item := range strings.Split(encoded, ",") {
		parts := strings.Split(item, "=")
		if len(parts) != 2 {
			return "", nil, errors.New("queue concurrency must use queue=workers entries")
		}
		queue := strings.TrimSpace(parts[0])
		workers, parseErr := strconv.Atoi(strings.TrimSpace(parts[1]))
		if !validQueueName(queue) || parseErr != nil || workers < 1 || workers > 10_000 {
			return "", nil, errors.New("queue concurrency has an invalid entry")
		}
		if _, duplicate := concurrency[queue]; duplicate {
			return "", nil, fmt.Errorf("queue concurrency for %q is defined more than once", queue)
		}
		concurrency[queue] = workers
	}
	if len(concurrency) != len(queues) {
		return "", nil, errors.New("queue concurrency must cover the selected queues exactly")
	}
	for _, queue := range queues {
		if concurrency[queue] < 1 {
			return "", nil, errors.New("queue concurrency must cover the selected queues exactly")
		}
	}
	return group, concurrency, nil
}

func formatQueueConcurrency(concurrency map[string]int) string {
	queues := make([]string, 0, len(concurrency))
	for queue := range concurrency {
		queues = append(queues, queue)
	}
	slices.Sort(queues)
	values := make([]string, 0, len(queues))
	for _, queue := range queues {
		values = append(values, fmt.Sprintf("%s=%d", queue, concurrency[queue]))
	}
	return strings.Join(values, ",")
}

func validQueueName(value string) bool {
	if len(value) == 0 || len(value) > 96 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, char := range value[1:] {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') ||
			char == '.' || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}

func validateName(kind, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", kind)
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' {
			continue
		}
		return fmt.Errorf("%s must contain only lowercase letters, digits, and hyphens", kind)
	}
	return nil
}

func validateURI(key string, value secrets.Value, schemes ...string) error {
	if !value.Configured() {
		return nil
	}
	parsed, err := url.Parse(value.Reveal())
	if err != nil || parsed == nil {
		return fmt.Errorf("%s must be a valid supported URI", key)
	}
	scheme := strings.ToLower(parsed.Scheme)
	locationPresent := parsed.Host != "" || (scheme == "unix" && parsed.Path != "")
	if scheme == "" || !locationPresent || !slices.Contains(schemes, scheme) {
		return fmt.Errorf("%s must be a valid supported URI", key)
	}
	return nil
}
