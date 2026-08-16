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
	maximumShutdownTimeout   = 3 * time.Hour
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
	localStatusMappingPath         = "/app/config/status_mapping.yaml"
	localInvestmentAreasPath       = "/app/config/investment_areas.yaml"
)

const (
	providerRoutesPresetEnv = "GO_PROVIDER_ROUTES"
	devHealthEnv            = "DEV_HEALTH_ENV"
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
	Service        string
	Profiles       []string
	DefaultProfile string
	Profile        string
	LookupEnv      secrets.LookupEnv
}

// Config contains typed runtime settings. Sensitive values use secrets.Value,
// which redacts itself from formatting, slog, and JSON.
type Config struct {
	Service string
	Profile string
	// WorkerInstanceID is generated once inside the process. Every River
	// client in that process derives its attempted_by identity from it.
	WorkerInstanceID   string
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
	SettingsEncryptionSalt secrets.Value
	PagerDutyOAuthClientID secrets.Value
	PagerDutyOAuthSecret   secrets.Value

	QueueDatabaseMode              QueueControlMode
	CoordinatorDatabaseMode        QueueControlMode
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
	LocalAllProviderRoutes         bool

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
	// WorkerGitlabRepoMetadataEnabled is the independently gated native
	// (gitlab, repo-metadata) route. It defaults false and does not activate
	// traffic merely because the capability matrix is ready.
	WorkerGitlabRepoMetadataEnabled bool
	// WorkerGitlabCommitsEnabled gates the isolated (gitlab, commits) route.
	WorkerGitlabCommitsEnabled bool
	// WorkerGitlabCommitStatsEnabled gates the isolated aggregate commit-stat route.
	WorkerGitlabCommitStatsEnabled bool
	// WorkerGitlabCICDEnabled and WorkerGitlabTestsEnabled gate mutually
	// exclusive aliases for one complete GitLab TestOps writer.
	WorkerGitlabCICDEnabled  bool
	WorkerGitlabTestsEnabled bool
	// WorkerGitlabIncidentsEnabled gates the canonical operational incident route.
	WorkerGitlabIncidentsEnabled bool
	// The remaining GitLab flags gate independently completed native routes.
	// PR aliases are the exception: all three delegate to one complete PR-social
	// writer and Load rejects enabling more than one alias at a time.
	WorkerGitlabDeploymentsEnabled  bool
	WorkerGitlabFeatureFlagsEnabled bool
	WorkerGitlabFilesEnabled        bool
	WorkerGitlabBlameEnabled        bool
	WorkerGitlabPRsEnabled          bool
	WorkerGitlabPRReviewsEnabled    bool
	WorkerGitlabPRCommentsEnabled   bool
	WorkerGitlabSecurityEnabled     bool
	// WorkerGitlabWorkItemsEnabled gates the one complete five-alias GitLab
	// work-item family; sibling alias identities are not independent routes.
	WorkerGitlabWorkItemsEnabled bool
	// WorkerGithubPRsEnabled is the (github, prs) half of the two-key route
	// gate (CHAOS-3122, following CHAOS-3123's precedent). The matrix marking
	// the pair route_ready is the other half; neither alone moves traffic.
	// Its Python counterpart is ProviderUnitRouteSwitches.github_prs, read
	// from the same WORKER_GITHUB_PRS_ENABLED name.
	WorkerGithubPRsEnabled bool
	// WorkerGithubPRReviewsEnabled and WorkerGithubPRCommentsEnabled gate the
	// two remaining dataset aliases for the same complete PR-social unit.
	WorkerGithubPRReviewsEnabled  bool
	WorkerGithubPRCommentsEnabled bool
	// WorkerGithubCICDEnabled gates the isolated (github, cicd) route.
	WorkerGithubCICDEnabled bool
	// WorkerGithubCommitsEnabled gates the isolated (github, commits) route.
	WorkerGithubCommitsEnabled bool
	// WorkerGithubDeploymentsEnabled gates the isolated (github, deployments) route.
	WorkerGithubDeploymentsEnabled bool
	// WorkerGithubSecurityEnabled gates the isolated (github, security) route.
	WorkerGithubSecurityEnabled bool
	// WorkerGithubFilesEnabled gates the isolated (github, files) route.
	WorkerGithubFilesEnabled bool
	// WorkerGithubCommitStatsEnabled gates the isolated (github, commit-stats) route.
	WorkerGithubCommitStatsEnabled bool
	// WorkerGithubBlameEnabled gates the resumable (github, blame) route.
	WorkerGithubBlameEnabled bool
	// WorkerGithubTestsEnabled gates the complete six-effect (github, tests) route.
	WorkerGithubTestsEnabled bool
	// WorkerGithubWorkItemsEnabled gates the one complete five-alias GitHub
	// work-item family. The Python planner emits only canonical work-items
	// claims; sibling alias identities survive in processor flags, watermark,
	// and audit metadata rather than becoming partial writers.
	WorkerGithubWorkItemsEnabled bool
	// WorkerGithubWorkItemsStatusMappingPath and
	// WorkerGithubWorkItemsInvestmentConfigPath are explicit production paths
	// for the two Python-parity config engines. Production has no source-relative
	// default; the local-only all-routes preset selects artifacts packaged at
	// fixed image paths. When the route is enabled, cmd/dev-health-worker validates
	// both paths and rejects ambient STATUS_MAPPING_PATH overrides.
	WorkerGithubWorkItemsStatusMappingPath    string
	WorkerGithubWorkItemsInvestmentConfigPath string

	// PagerDuty route switches are default-off and independent. The incidents
	// switch owns the complete incidents family, including alert, log-entry,
	// and note alias datasets; those aliases are not separately activatable.
	WorkerPagerDutyServicesEnabled           bool
	WorkerPagerDutyBusinessServicesEnabled   bool
	WorkerPagerDutyEscalationPoliciesEnabled bool
	WorkerPagerDutySchedulesEnabled          bool
	WorkerPagerDutyOnCallsEnabled            bool
	WorkerPagerDutyUsersEnabled              bool
	WorkerPagerDutyTeamsEnabled              bool
	WorkerPagerDutyIncidentsEnabled          bool

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
		maximumShutdownTimeout,
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
	allProviderRoutes, err := localAllProviderRoutes(lookup)
	if err != nil {
		return Config{}, err
	}
	cfg.LocalAllProviderRoutes = allProviderRoutes
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
			name:   "WORKER_GITLAB_REPO_METADATA_ENABLED",
			target: &cfg.WorkerGitlabRepoMetadataEnabled,
		},
		{
			name:   "WORKER_GITLAB_COMMITS_ENABLED",
			target: &cfg.WorkerGitlabCommitsEnabled,
		},
		{
			name:   "WORKER_GITLAB_COMMIT_STATS_ENABLED",
			target: &cfg.WorkerGitlabCommitStatsEnabled,
		},
		{
			name:   "WORKER_GITLAB_CICD_ENABLED",
			target: &cfg.WorkerGitlabCICDEnabled,
		},
		{
			name:   "WORKER_GITLAB_TESTS_ENABLED",
			target: &cfg.WorkerGitlabTestsEnabled,
		},
		{
			name:   "WORKER_GITLAB_INCIDENTS_ENABLED",
			target: &cfg.WorkerGitlabIncidentsEnabled,
		},
		{
			name:   "WORKER_GITLAB_DEPLOYMENTS_ENABLED",
			target: &cfg.WorkerGitlabDeploymentsEnabled,
		},
		{
			name:   "WORKER_GITLAB_FEATURE_FLAGS_ENABLED",
			target: &cfg.WorkerGitlabFeatureFlagsEnabled,
		},
		{
			name:   "WORKER_GITLAB_FILES_ENABLED",
			target: &cfg.WorkerGitlabFilesEnabled,
		},
		{
			name:   "WORKER_GITLAB_BLAME_ENABLED",
			target: &cfg.WorkerGitlabBlameEnabled,
		},
		{
			name:   "WORKER_GITLAB_PRS_ENABLED",
			target: &cfg.WorkerGitlabPRsEnabled,
		},
		{
			name:   "WORKER_GITLAB_PR_REVIEWS_ENABLED",
			target: &cfg.WorkerGitlabPRReviewsEnabled,
		},
		{
			name:   "WORKER_GITLAB_PR_COMMENTS_ENABLED",
			target: &cfg.WorkerGitlabPRCommentsEnabled,
		},
		{
			name:   "WORKER_GITLAB_SECURITY_ENABLED",
			target: &cfg.WorkerGitlabSecurityEnabled,
		},
		{
			name:   "WORKER_GITLAB_WORK_ITEMS_ENABLED",
			target: &cfg.WorkerGitlabWorkItemsEnabled,
		},
		{
			name:   "WORKER_PAGERDUTY_SERVICES_ENABLED",
			target: &cfg.WorkerPagerDutyServicesEnabled,
		},
		{
			name:   "WORKER_PAGERDUTY_BUSINESS_SERVICES_ENABLED",
			target: &cfg.WorkerPagerDutyBusinessServicesEnabled,
		},
		{
			name:   "WORKER_PAGERDUTY_ESCALATION_POLICIES_ENABLED",
			target: &cfg.WorkerPagerDutyEscalationPoliciesEnabled,
		},
		{
			name:   "WORKER_PAGERDUTY_SCHEDULES_ENABLED",
			target: &cfg.WorkerPagerDutySchedulesEnabled,
		},
		{
			name:   "WORKER_PAGERDUTY_ON_CALLS_ENABLED",
			target: &cfg.WorkerPagerDutyOnCallsEnabled,
		},
		{
			name:   "WORKER_PAGERDUTY_USERS_ENABLED",
			target: &cfg.WorkerPagerDutyUsersEnabled,
		},
		{
			name:   "WORKER_PAGERDUTY_TEAMS_ENABLED",
			target: &cfg.WorkerPagerDutyTeamsEnabled,
		},
		{
			name:   "WORKER_PAGERDUTY_INCIDENTS_ENABLED",
			target: &cfg.WorkerPagerDutyIncidentsEnabled,
		},
		{
			name:   "WORKER_GITHUB_PRS_ENABLED",
			target: &cfg.WorkerGithubPRsEnabled,
		},
		{
			name:   "WORKER_GITHUB_PR_REVIEWS_ENABLED",
			target: &cfg.WorkerGithubPRReviewsEnabled,
		},
		{
			name:   "WORKER_GITHUB_PR_COMMENTS_ENABLED",
			target: &cfg.WorkerGithubPRCommentsEnabled,
		},
		{
			name:   "WORKER_GITHUB_CICD_ENABLED",
			target: &cfg.WorkerGithubCICDEnabled,
		},
		{
			name:   "WORKER_GITHUB_COMMITS_ENABLED",
			target: &cfg.WorkerGithubCommitsEnabled,
		},
		{
			name:   "WORKER_GITHUB_DEPLOYMENTS_ENABLED",
			target: &cfg.WorkerGithubDeploymentsEnabled,
		},
		{
			name:   "WORKER_GITHUB_SECURITY_ENABLED",
			target: &cfg.WorkerGithubSecurityEnabled,
		},
		{
			name:   "WORKER_GITHUB_FILES_ENABLED",
			target: &cfg.WorkerGithubFilesEnabled,
		},
		{
			name:   "WORKER_GITHUB_COMMIT_STATS_ENABLED",
			target: &cfg.WorkerGithubCommitStatsEnabled,
		},
		{
			name:   "WORKER_GITHUB_BLAME_ENABLED",
			target: &cfg.WorkerGithubBlameEnabled,
		},
		{
			name:   "WORKER_GITHUB_TESTS_ENABLED",
			target: &cfg.WorkerGithubTestsEnabled,
		},
		{
			name:   "WORKER_GITHUB_WORK_ITEMS_ENABLED",
			target: &cfg.WorkerGithubWorkItemsEnabled,
		},
	} {
		fallback := false
		if allProviderRoutes {
			fallback, err = providerRoutePresetDefault(lookup, item.name)
			if err != nil {
				return Config{}, err
			}
		}
		*item.target, err = boolEnv(lookup, item.name, fallback)
		if err != nil {
			return Config{}, err
		}
	}
	if cfg.WorkerGithubCICDEnabled && cfg.WorkerGithubTestsEnabled {
		return Config{}, fmt.Errorf("WORKER_GITHUB_CICD_ENABLED and WORKER_GITHUB_TESTS_ENABLED are mutually exclusive: both delegate to one complete TestOps writer")
	}
	githubPRSocialAliases := 0
	for _, enabled := range []bool{
		cfg.WorkerGithubPRsEnabled,
		cfg.WorkerGithubPRReviewsEnabled,
		cfg.WorkerGithubPRCommentsEnabled,
	} {
		if enabled {
			githubPRSocialAliases++
		}
	}
	if githubPRSocialAliases > 1 {
		return Config{}, fmt.Errorf("WORKER_GITHUB_PRS_ENABLED, WORKER_GITHUB_PR_REVIEWS_ENABLED, and WORKER_GITHUB_PR_COMMENTS_ENABLED are mutually exclusive: all delegate to one complete PR-social writer")
	}
	if cfg.WorkerGitlabCICDEnabled && cfg.WorkerGitlabTestsEnabled {
		return Config{}, fmt.Errorf("WORKER_GITLAB_CICD_ENABLED and WORKER_GITLAB_TESTS_ENABLED are mutually exclusive: both delegate to one complete TestOps writer")
	}
	gitlabPRSocialAliases := 0
	for _, enabled := range []bool{
		cfg.WorkerGitlabPRsEnabled,
		cfg.WorkerGitlabPRReviewsEnabled,
		cfg.WorkerGitlabPRCommentsEnabled,
	} {
		if enabled {
			gitlabPRSocialAliases++
		}
	}
	if gitlabPRSocialAliases > 1 {
		return Config{}, fmt.Errorf("WORKER_GITLAB_PRS_ENABLED, WORKER_GITLAB_PR_REVIEWS_ENABLED, and WORKER_GITLAB_PR_COMMENTS_ENABLED are mutually exclusive: all delegate to one complete PR-social writer")
	}
	cfg.WorkerGithubWorkItemsStatusMappingPath = envOrDefault(
		lookup,
		"WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH",
		conditionalDefault(allProviderRoutes, localStatusMappingPath),
	)
	cfg.WorkerGithubWorkItemsInvestmentConfigPath = envOrDefault(
		lookup,
		"WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH",
		conditionalDefault(allProviderRoutes, localInvestmentAreasPath),
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

func localAllProviderRoutes(lookup secrets.LookupEnv) (bool, error) {
	preset, _ := lookup(providerRoutesPresetEnv)
	preset = strings.ToLower(strings.TrimSpace(preset))
	if preset == "" {
		return false, nil
	}
	if preset != "all" {
		return false, fmt.Errorf("%s must be empty or all", providerRoutesPresetEnv)
	}
	environment, _ := lookup(devHealthEnv)
	if strings.ToLower(strings.TrimSpace(environment)) != "local" {
		return false, fmt.Errorf("%s=all requires %s=local", providerRoutesPresetEnv, devHealthEnv)
	}
	return true, nil
}

func providerRoutePresetDisabledAlias(name string) bool {
	switch name {
	case "WORKER_GITHUB_PR_REVIEWS_ENABLED",
		"WORKER_GITHUB_PR_COMMENTS_ENABLED",
		"WORKER_GITHUB_TESTS_ENABLED",
		"WORKER_GITLAB_PR_REVIEWS_ENABLED",
		"WORKER_GITLAB_PR_COMMENTS_ENABLED",
		"WORKER_GITLAB_TESTS_ENABLED":
		return true
	default:
		return false
	}
}

func providerRoutePresetDefault(lookup secrets.LookupEnv, name string) (bool, error) {
	if providerRoutePresetDisabledAlias(name) {
		return false, nil
	}
	alternatives := map[string][]string{
		"WORKER_GITHUB_PRS_ENABLED": {
			"WORKER_GITHUB_PR_REVIEWS_ENABLED",
			"WORKER_GITHUB_PR_COMMENTS_ENABLED",
		},
		"WORKER_GITHUB_CICD_ENABLED": {"WORKER_GITHUB_TESTS_ENABLED"},
		"WORKER_GITLAB_PRS_ENABLED": {
			"WORKER_GITLAB_PR_REVIEWS_ENABLED",
			"WORKER_GITLAB_PR_COMMENTS_ENABLED",
		},
		"WORKER_GITLAB_CICD_ENABLED": {"WORKER_GITLAB_TESTS_ENABLED"},
	}
	for _, alternative := range alternatives[name] {
		enabled, err := boolEnv(lookup, alternative, false)
		if err != nil {
			return false, err
		}
		if enabled {
			return false, nil
		}
	}
	return true, nil
}

func conditionalDefault(enabled bool, value string) string {
	if enabled {
		return value
	}
	return ""
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
		slog.Bool("worker_gitlab_repo_metadata_enabled", c.WorkerGitlabRepoMetadataEnabled),
		slog.Bool("worker_gitlab_commits_enabled", c.WorkerGitlabCommitsEnabled),
		slog.Bool("worker_gitlab_commit_stats_enabled", c.WorkerGitlabCommitStatsEnabled),
		slog.Bool("worker_gitlab_cicd_enabled", c.WorkerGitlabCICDEnabled),
		slog.Bool("worker_gitlab_tests_enabled", c.WorkerGitlabTestsEnabled),
		slog.Bool("worker_gitlab_incidents_enabled", c.WorkerGitlabIncidentsEnabled),
		slog.Bool("worker_gitlab_deployments_enabled", c.WorkerGitlabDeploymentsEnabled),
		slog.Bool("worker_gitlab_feature_flags_enabled", c.WorkerGitlabFeatureFlagsEnabled),
		slog.Bool("worker_gitlab_files_enabled", c.WorkerGitlabFilesEnabled),
		slog.Bool("worker_gitlab_blame_enabled", c.WorkerGitlabBlameEnabled),
		slog.Bool("worker_gitlab_prs_enabled", c.WorkerGitlabPRsEnabled),
		slog.Bool("worker_gitlab_pr_reviews_enabled", c.WorkerGitlabPRReviewsEnabled),
		slog.Bool("worker_gitlab_pr_comments_enabled", c.WorkerGitlabPRCommentsEnabled),
		slog.Bool("worker_gitlab_security_enabled", c.WorkerGitlabSecurityEnabled),
		slog.Bool("worker_gitlab_work_items_enabled", c.WorkerGitlabWorkItemsEnabled),
		slog.Bool("worker_pagerduty_services_enabled", c.WorkerPagerDutyServicesEnabled),
		slog.Bool("worker_pagerduty_business_services_enabled", c.WorkerPagerDutyBusinessServicesEnabled),
		slog.Bool("worker_pagerduty_escalation_policies_enabled", c.WorkerPagerDutyEscalationPoliciesEnabled),
		slog.Bool("worker_pagerduty_schedules_enabled", c.WorkerPagerDutySchedulesEnabled),
		slog.Bool("worker_pagerduty_on_calls_enabled", c.WorkerPagerDutyOnCallsEnabled),
		slog.Bool("worker_pagerduty_users_enabled", c.WorkerPagerDutyUsersEnabled),
		slog.Bool("worker_pagerduty_teams_enabled", c.WorkerPagerDutyTeamsEnabled),
		slog.Bool("worker_pagerduty_incidents_enabled", c.WorkerPagerDutyIncidentsEnabled),
		slog.Bool("worker_github_prs_enabled", c.WorkerGithubPRsEnabled),
		slog.Bool("worker_github_pr_reviews_enabled", c.WorkerGithubPRReviewsEnabled),
		slog.Bool("worker_github_pr_comments_enabled", c.WorkerGithubPRCommentsEnabled),
		slog.Bool("worker_github_commits_enabled", c.WorkerGithubCommitsEnabled),
		slog.Bool("worker_github_security_enabled", c.WorkerGithubSecurityEnabled),
		slog.Bool("worker_github_blame_enabled", c.WorkerGithubBlameEnabled),
		slog.Bool("worker_github_tests_enabled", c.WorkerGithubTestsEnabled),
		slog.Bool("worker_github_work_items_enabled", c.WorkerGithubWorkItemsEnabled),
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
