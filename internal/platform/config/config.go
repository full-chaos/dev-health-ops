// Package config defines the shared runtime-shell configuration contract.
package config

import (
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

const (
	defaultHTTPAddress       = ":8080"
	defaultShutdownTimeout   = 30 * time.Second
	defaultHealthCheckTimout = 2 * time.Second
	defaultDomainMaxConns    = 4
	defaultQueueMaxConns     = 2
	// 2 matches the checked-in per-process coordinator_max_connections in
	// deploy/go-workers/profiles.json for every coordinator process today
	// (reconciler, scheduler, worker-operator).
	defaultCoordinatorMaxConns = 2
	defaultCompletedRetention  = 7 * 24 * time.Hour
	defaultCancelledRetention  = 30 * 24 * time.Hour
	defaultDiscardedRetention  = 30 * 24 * time.Hour
	defaultJobCleanerTimeout   = 30 * time.Second
	defaultRiverDatabaseSchema = "river"
	defaultDomainDatabaseRole  = "devhealth_domain"
	defaultQueueDatabaseRole   = "devhealth_queue"
	// The coordinator role of the CHAOS-3033 Option B split. Provisioned
	// alongside the other two by docker/init-extra-dbs.sh (local dev) and
	// scripts/worker/provision_river_roles.sql (deployed environments).
	defaultCoordinatorDatabaseRole = "devhealth_coordinator"
	defaultStreamReplicas          = 1
)

// QueueControlMode describes the endpoint semantics promised by the operator.
// Phase 0 proved direct PostgreSQL. Session mode remains unavailable until it
// passes the same compatibility matrix, and transaction mode cannot propagate
// cancellation to a running River worker.
type QueueControlMode string

const (
	QueueControlDirect      QueueControlMode = "direct"
	QueueControlSession     QueueControlMode = "session"
	QueueControlTransaction QueueControlMode = "transaction"
)

// Spec describes the immutable configuration surface of one executable.
type Spec struct {
	Service        string
	Profiles       []string
	DefaultProfile string
	Profile        string
	LookupEnv      secrets.LookupEnv
}

// Config contains typed runtime settings. Sensitive values use secrets.Value,
// which redacts itself from formatting, slog, and JSON.
type Config struct {
	Service            string
	Profile            string
	HTTPAddress        string
	ShutdownTimeout    time.Duration
	HealthCheckTimeout time.Duration
	LogLevel           slog.Level

	DomainDatabaseURI      secrets.Value
	QueueDatabaseURI       secrets.Value
	CoordinatorDatabaseURI secrets.Value
	ClickHouseURI          secrets.Value
	ValkeyURI              secrets.Value
	SettingsEncryptionKey  secrets.Value

	QueueDatabaseMode              QueueControlMode
	RiverDatabaseSchema            string
	DomainDatabaseRole             string
	QueueDatabaseRole              string
	CoordinatorDatabaseRole        string
	DomainTransactionPooler        bool
	DomainDatabaseMaxConns         int32
	QueueDatabaseMaxConns          int32
	CoordinatorDatabaseMaxConns    int32
	CompletedJobRetention          time.Duration
	CancelledJobRetention          time.Duration
	DiscardedJobRetention          time.Duration
	RiverJobCleanerTimeout         time.Duration
	OperationalBridgeURL           string
	OperationalBridgeToken         secrets.Value
	OperationalBridgeTimeout       time.Duration
	OperationalBridgeAllowInsecure bool
	StreamConfiguredReplicas       int

	WorkerLinearWorkItemsEnabled          bool
	WorkerJiraWorkItemsEnabled            bool
	WorkerJiraIncidentsEnabled            bool
	WorkerLaunchDarklyFeatureFlagsEnabled bool
	// WorkerGithubRepoMetadataEnabled is the (github, repo-metadata) half of
	// the two-key route gate (CHAOS-3123). The matrix marking the pair
	// route_ready is the other half; neither alone moves traffic. Its Python
	// counterpart is ProviderUnitRouteSwitches.github_repo_metadata, read from
	// the same WORKER_GITHUB_REPO_METADATA_ENABLED name, because the producer
	// and the executor must agree on the route or a dispatched unit finds no
	// handler.
	WorkerGithubRepoMetadataEnabled bool
	// WorkerGithubPRsEnabled is the (github, prs) half of the two-key route
	// gate (CHAOS-3122, following CHAOS-3123's precedent). The matrix marking
	// the pair route_ready is the other half; neither alone moves traffic.
	// Its Python counterpart is ProviderUnitRouteSwitches.github_prs, read
	// from the same WORKER_GITHUB_PRS_ENABLED name.
	WorkerGithubPRsEnabled bool
	// WorkerGithubCICDEnabled gates the isolated (github, cicd) route.
	WorkerGithubCICDEnabled bool

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

// Load reads and validates the process environment. CLI profile selection, if
// supplied by Spec.Profile, takes precedence over DEV_HEALTH_PROFILE.
func Load(spec Spec) (Config, error) {
	lookup := spec.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}

	cfg := Config{Service: spec.Service}
	if err := validateName("service", cfg.Service); err != nil {
		return Config{}, err
	}

	cfg.HTTPAddress = envOrDefault(lookup, "DEV_HEALTH_HTTP_ADDR", defaultHTTPAddress)
	if _, _, err := net.SplitHostPort(cfg.HTTPAddress); err != nil {
		return Config{}, fmt.Errorf("DEV_HEALTH_HTTP_ADDR must be a host:port address")
	}

	var err error
	cfg.ShutdownTimeout, err = durationEnv(
		lookup,
		"DEV_HEALTH_SHUTDOWN_TIMEOUT",
		defaultShutdownTimeout,
		500*time.Millisecond,
		5*time.Minute,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.OperationalBridgeAllowInsecure, err = boolEnv(
		lookup, "WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE", false,
	)
	if err != nil {
		return Config{}, err
	}
	for _, item := range []struct {
		name   string
		target *bool
	}{
		{
			name:   "WORKER_LINEAR_WORK_ITEMS_ENABLED",
			target: &cfg.WorkerLinearWorkItemsEnabled,
		},
		{
			name:   "WORKER_JIRA_WORK_ITEMS_ENABLED",
			target: &cfg.WorkerJiraWorkItemsEnabled,
		},
		{
			name:   "WORKER_JIRA_INCIDENTS_ENABLED",
			target: &cfg.WorkerJiraIncidentsEnabled,
		},
		{
			name:   "WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED",
			target: &cfg.WorkerLaunchDarklyFeatureFlagsEnabled,
		},
		{
			name:   "WORKER_GITHUB_REPO_METADATA_ENABLED",
			target: &cfg.WorkerGithubRepoMetadataEnabled,
		},
		{
			name:   "WORKER_GITHUB_PRS_ENABLED",
			target: &cfg.WorkerGithubPRsEnabled,
		},
		{
			name:   "WORKER_GITHUB_CICD_ENABLED",
			target: &cfg.WorkerGithubCICDEnabled,
		},
	} {
		*item.target, err = boolEnv(lookup, item.name, false)
		if err != nil {
			return Config{}, err
		}
	}
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

	cfg.Profile, err = profile(spec, lookup)
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
		slog.Bool("operational_bridge_allow_insecure", c.OperationalBridgeAllowInsecure),
		slog.Int("stream_configured_replicas", c.StreamConfiguredReplicas),
		slog.Bool("worker_linear_work_items_enabled", c.WorkerLinearWorkItemsEnabled),
		slog.Bool("worker_jira_work_items_enabled", c.WorkerJiraWorkItemsEnabled),
		slog.Bool("worker_jira_incidents_enabled", c.WorkerJiraIncidentsEnabled),
		slog.Bool(
			"worker_launchdarkly_feature_flags_enabled",
			c.WorkerLaunchDarklyFeatureFlagsEnabled,
		),
		slog.Bool("worker_github_repo_metadata_enabled", c.WorkerGithubRepoMetadataEnabled),
		slog.Bool("worker_github_prs_enabled", c.WorkerGithubPRsEnabled),
		slog.Bool("clickhouse_configured", c.ClickHouseURI.Configured()),
		slog.Bool("valkey_configured", c.ValkeyURI.Configured()),
		slog.Bool("settings_encryption_key_configured", c.SettingsEncryptionKey.Configured()),
	}
	if c.Profile != "" {
		attrs = append(attrs, slog.String("profile", c.Profile))
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
	mode := QueueControlMode(strings.ToLower(envOrDefault(lookup, "WORKER_DATABASE_MODE", string(QueueControlDirect))))
	switch mode {
	case QueueControlDirect, QueueControlSession, QueueControlTransaction:
		return mode, nil
	default:
		return "", fmt.Errorf("WORKER_DATABASE_MODE must be direct, session, or transaction")
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
		return 0, fmt.Errorf("DEV_HEALTH_LOG_LEVEL must be debug, info, warn, or error")
	}
}

func profile(spec Spec, lookup secrets.LookupEnv) (string, error) {
	selected := spec.Profile
	if selected == "" {
		selected = envOrDefault(lookup, "DEV_HEALTH_PROFILE", spec.DefaultProfile)
	}
	if len(spec.Profiles) == 0 {
		if selected != "" {
			return "", fmt.Errorf("%s does not accept a profile", spec.Service)
		}
		return "", nil
	}
	if !slices.Contains(spec.Profiles, selected) {
		return "", fmt.Errorf("profile must be one of %s", strings.Join(spec.Profiles, ", "))
	}
	return selected, nil
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
