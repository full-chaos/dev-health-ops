package config

import (
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func TestLocalAllProviderRoutesPreset(t *testing.T) {
	t.Parallel()

	cfg, err := Load(workerSpec(map[string]string{
		"DEV_HEALTH_ENV":     "local",
		"GO_PROVIDER_ROUTES": "all",
	}))
	if err != nil {
		t.Fatal(err)
	}
	disabledAliases := map[string]bool{
		"WorkerGithubPRReviewsEnabled":  true,
		"WorkerGithubPRCommentsEnabled": true,
		"WorkerGithubTestsEnabled":      true,
		"WorkerGitlabPRReviewsEnabled":  true,
		"WorkerGitlabPRCommentsEnabled": true,
		"WorkerGitlabTestsEnabled":      true,
	}
	value := reflect.ValueOf(cfg)
	typeOfConfig := value.Type()
	for index := 0; index < value.NumField(); index++ {
		field := typeOfConfig.Field(index)
		if !strings.HasPrefix(field.Name, "Worker") ||
			!strings.HasSuffix(field.Name, "Enabled") ||
			field.Type.Kind() != reflect.Bool {
			continue
		}
		want := !disabledAliases[field.Name]
		if got := value.Field(index).Bool(); got != want {
			t.Errorf("%s=%t, want %t", field.Name, got, want)
		}
	}
	if cfg.WorkerGithubWorkItemsStatusMappingPath != "/app/config/status_mapping.yaml" ||
		cfg.WorkerGithubWorkItemsInvestmentConfigPath != "/app/config/investment_areas.yaml" {
		t.Fatalf("unexpected packaged paths: status=%q investment=%q", cfg.WorkerGithubWorkItemsStatusMappingPath, cfg.WorkerGithubWorkItemsInvestmentConfigPath)
	}
}

func TestLocalAllProviderRoutesPresetPreservesExplicitOverrides(t *testing.T) {
	t.Parallel()

	cfg, err := Load(workerSpec(map[string]string{
		"DEV_HEALTH_ENV":                               "local",
		"GO_PROVIDER_ROUTES":                           "all",
		"WORKER_GITHUB_FILES_ENABLED":                  "false",
		"WORKER_GITLAB_PR_REVIEWS_ENABLED":             "true",
		"WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH": "/override/status.yaml",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WorkerGithubFilesEnabled || !cfg.WorkerGithubCommitsEnabled {
		t.Fatalf("explicit false did not override preset: %#v", cfg.SafeAttrs())
	}
	if !cfg.WorkerGitlabPRReviewsEnabled || cfg.WorkerGitlabPRsEnabled {
		t.Fatal("explicit alias choice did not override canonical preset aliases")
	}
	if cfg.WorkerGithubWorkItemsStatusMappingPath != "/override/status.yaml" {
		t.Fatal("explicit semantic path did not override packaged preset path")
	}
}

func TestProviderRoutesPresetRejectsNonLocalAndUnknownValues(t *testing.T) {
	t.Parallel()

	for name, values := range map[string]map[string]string{
		"missing environment": {"GO_PROVIDER_ROUTES": "all"},
		"production":          {"DEV_HEALTH_ENV": "production", "GO_PROVIDER_ROUTES": "all"},
		"unknown preset":      {"DEV_HEALTH_ENV": "local", "GO_PROVIDER_ROUTES": "some"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := Load(workerSpec(values)); err == nil {
				t.Fatal("expected invalid provider route preset to fail")
			}
		})
	}
}

func lookup(values map[string]string) secrets.LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func workerSpec(values map[string]string) Spec {
	return Spec{
		Service:        "dev-health-worker",
		Profiles:       []string{"latency", "sync", "heavy", "ops"},
		DefaultProfile: "latency",
		LookupEnv:      lookup(values),
	}
}

func queueWorkerSpec(values map[string]string, queues ...string) Spec {
	return Spec{
		Service:       "dev-health-worker",
		RequireQueues: true,
		Queues:        queues,
		LookupEnv:     lookup(values),
	}
}

func TestWorkerQueueSelectionIsExplicitCanonicalAndProfileFree(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		spec Spec
		want []string
	}{
		"cli comma and repeatable": {
			spec: queueWorkerSpec(map[string]string{
				"DEV_HEALTH_WORKER_GROUP":      "api-workers",
				"DEV_HEALTH_QUEUE_CONCURRENCY": "webhooks=4,heartbeat=1,retention=2",
			}, "webhooks,heartbeat", "retention"),
			want: []string{"heartbeat", "retention", "webhooks"},
		},
		"environment": {
			spec: queueWorkerSpec(map[string]string{
				"DEV_HEALTH_QUEUES":            "webhooks, heartbeat",
				"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=3,webhooks=9",
			}),
			want: []string{"heartbeat", "webhooks"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg, err := Load(test.spec)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(cfg.Queues, test.want) || cfg.Profile != "" ||
				len(cfg.WorkerQueueConcurrency) != len(test.want) {
				t.Fatalf("queues=%v profile=%q, want queues=%v and no profile", cfg.Queues, cfg.Profile, test.want)
			}
		})
	}
}

func TestWorkerQueueSelectionRejectsInvalidOrAmbiguousInput(t *testing.T) {
	t.Parallel()

	for name, spec := range map[string]Spec{
		"missing":             queueWorkerSpec(nil),
		"duplicate":           queueWorkerSpec(nil, "heartbeat,webhooks", "heartbeat"),
		"empty item":          queueWorkerSpec(nil, "heartbeat,"),
		"invalid name":        queueWorkerSpec(nil, "Heartbeat"),
		"missing concurrency": queueWorkerSpec(nil, "heartbeat"),
		"incomplete concurrency": queueWorkerSpec(
			map[string]string{"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=1"}, "heartbeat,webhooks",
		),
		"extra concurrency": queueWorkerSpec(
			map[string]string{"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=1,webhooks=1"}, "heartbeat",
		),
		"duplicate concurrency": queueWorkerSpec(
			map[string]string{"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=1,heartbeat=2"}, "heartbeat",
		),
		"invalid concurrency": queueWorkerSpec(
			map[string]string{"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=0"}, "heartbeat",
		),
		"cli env conflict": queueWorkerSpec(
			map[string]string{"DEV_HEALTH_QUEUES": "heartbeat"}, "webhooks",
		),
		"profile compatibility": {
			Service:        "dev-health-worker",
			Profiles:       []string{"ops"},
			DefaultProfile: "ops",
			RequireQueues:  true,
			Queues:         []string{"heartbeat"},
			LookupEnv:      lookup(nil),
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := Load(spec); err == nil {
				t.Fatal("expected queue selection to fail")
			}
		})
	}
}

func TestLoadDefaultsAndTypedOverrides(t *testing.T) {
	t.Parallel()

	cfg, err := Load(workerSpec(map[string]string{
		"DEV_HEALTH_HTTP_ADDR":            "127.0.0.1:9091",
		"DEV_HEALTH_SHUTDOWN_TIMEOUT":     "17s",
		"DEV_HEALTH_HEALTH_CHECK_TIMEOUT": "750ms",
		"DEV_HEALTH_LOG_LEVEL":            "debug",
		"DEV_HEALTH_PROFILE":              "heavy",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddress != "127.0.0.1:9091" || cfg.ShutdownTimeout != 17*time.Second {
		t.Fatalf("unexpected typed config: %#v", cfg.SafeAttrs())
	}
	if cfg.HealthCheckTimeout != 750*time.Millisecond || cfg.LogLevel != slog.LevelDebug {
		t.Fatalf("unexpected health/log config: %#v", cfg.SafeAttrs())
	}
	if cfg.Profile != "heavy" {
		t.Fatalf("expected heavy profile, got %q", cfg.Profile)
	}
}

func TestShutdownTimeoutSupportsDeploymentGraceWindows(t *testing.T) {
	t.Parallel()

	for name, raw := range map[string]string{
		"sync and ops": "960s",
		"heavy":        "7260s",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg, err := Load(workerSpec(map[string]string{
				"DEV_HEALTH_SHUTDOWN_TIMEOUT": raw,
			}))
			if err != nil {
				t.Fatalf("Load() rejected deployment shutdown timeout %s: %v", raw, err)
			}
			want, err := time.ParseDuration(raw)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.ShutdownTimeout != want {
				t.Fatalf("shutdown timeout = %s, want %s", cfg.ShutdownTimeout, want)
			}
		})
	}

	if _, err := Load(workerSpec(map[string]string{
		"DEV_HEALTH_SHUTDOWN_TIMEOUT": "3h0m1s",
	})); err == nil {
		t.Fatal("expected shutdown timeout above the safety ceiling to fail")
	}
}

func TestLoadSettingsEncryptionSalt(t *testing.T) {
	t.Parallel()
	cfg, err := Load(workerSpec(map[string]string{
		"SETTINGS_ENCRYPTION_SALT": "deployment-specific-salt",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SettingsEncryptionSalt.Reveal() != "deployment-specific-salt" {
		t.Fatal("SETTINGS_ENCRYPTION_SALT was not preserved in typed config")
	}
}

func TestSafeAttrsRedactsSettingsEncryptionSalt(t *testing.T) {
	t.Parallel()
	cfg, err := Load(workerSpec(map[string]string{
		"SETTINGS_ENCRYPTION_SALT": "deployment-specific-salt",
	}))
	if err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(cfg.SafeAttrs())
	if strings.Contains(text, "deployment-specific-salt") {
		t.Fatal("safe attrs leaked SETTINGS_ENCRYPTION_SALT")
	}
	if !strings.Contains(text, "settings_encryption_salt_configured=true") {
		t.Fatal("safe attrs omitted the SETTINGS_ENCRYPTION_SALT configured marker")
	}
}

func TestCLIProfileOverridesEnvironmentAndMustBeAllowed(t *testing.T) {
	t.Parallel()

	spec := workerSpec(map[string]string{"DEV_HEALTH_PROFILE": "heavy"})
	spec.Profile = "sync"
	cfg, err := Load(spec)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Profile != "sync" {
		t.Fatalf("expected CLI profile override, got %q", cfg.Profile)
	}

	spec.Profile = "arbitrary"
	if _, err := Load(spec); err == nil {
		t.Fatal("expected invalid profile to fail")
	}
}

func TestSafeAttrsNeverContainSecretsOrDSNs(t *testing.T) {
	t.Parallel()

	secret := "postgres://worker:top-secret@database.internal/app"
	cfg, err := Load(workerSpec(map[string]string{
		"POSTGRES_URI":         secret,
		"WORKER_DATABASE_URI":  "postgres://queue:other-secret@database.internal/app",
		"CLICKHOUSE_URI":       "clickhouse://analytics:secret@ch.internal/default",
		"VALKEY_URI":           "redis://:secret@valkey.internal/1",
		"PAGER_DUTY_CLIENT_ID": "pagerduty-client-id",
		"PAGER_DUTY_SECRET":    "pagerduty-client-secret",
	}))
	if err != nil {
		t.Fatal(err)
	}

	text := fmt.Sprint(cfg.SafeAttrs())
	for _, forbidden := range []string{
		secret, "top-secret", "clickhouse://", "redis://",
		"pagerduty-client-id", "pagerduty-client-secret",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("safe attrs leaked %q: %s", forbidden, text)
		}
	}
	for _, expected := range []string{
		"domain_database_configured=true",
		"queue_database_configured=true",
		"clickhouse_configured=true",
		"valkey_configured=true",
		"pagerduty_oauth_client_id_configured=true",
		"pagerduty_oauth_secret_configured=true",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("safe attrs missing %q: %s", expected, text)
		}
	}
}

func TestQueueControlAndRetentionDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := Load(workerSpec(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.QueueDatabaseMode != QueueControlDirect {
		t.Fatalf("queue mode = %q, want direct", cfg.QueueDatabaseMode)
	}
	if cfg.CoordinatorDatabaseMode != QueueControlDirect {
		t.Fatalf("coordinator mode = %q, want direct", cfg.CoordinatorDatabaseMode)
	}
	if cfg.RiverDatabaseSchema != "river" {
		t.Fatalf("River schema = %q, want river", cfg.RiverDatabaseSchema)
	}
	if cfg.DomainDatabaseRole != "devhealth_domain" || cfg.QueueDatabaseRole != "devhealth_queue" {
		t.Fatalf("unexpected default runtime roles: domain=%q queue=%q", cfg.DomainDatabaseRole, cfg.QueueDatabaseRole)
	}
	if cfg.QueueDatabaseMaxConns != 2 || cfg.DomainDatabaseMaxConns != 4 {
		t.Fatalf("unexpected connection budget: queue=%d domain=%d", cfg.QueueDatabaseMaxConns, cfg.DomainDatabaseMaxConns)
	}
	if cfg.CompletedJobRetention != 7*24*time.Hour {
		t.Fatalf("completed retention = %s", cfg.CompletedJobRetention)
	}
	if cfg.CancelledJobRetention != 30*24*time.Hour || cfg.DiscardedJobRetention != 30*24*time.Hour {
		t.Fatalf("unexpected terminal retention: cancelled=%s discarded=%s", cfg.CancelledJobRetention, cfg.DiscardedJobRetention)
	}
	if cfg.RiverJobCleanerTimeout != 30*time.Second {
		t.Fatalf("cleaner timeout = %s", cfg.RiverJobCleanerTimeout)
	}
	if cfg.OperationalBridgeAllowInsecure {
		t.Fatal("insecure operational bridge must default off")
	}
	if cfg.StreamConfiguredReplicas != 1 {
		t.Fatalf("stream replicas = %d, want 1", cfg.StreamConfiguredReplicas)
	}
	if cfg.WorkerLinearWorkItemsEnabled || cfg.WorkerJiraWorkItemsEnabled ||
		cfg.WorkerJiraIncidentsEnabled ||
		cfg.WorkerLaunchDarklyFeatureFlagsEnabled ||
		cfg.WorkerGithubRepoMetadataEnabled || cfg.WorkerGitlabRepoMetadataEnabled ||
		cfg.WorkerGitlabCommitStatsEnabled ||
		cfg.WorkerGitlabCICDEnabled ||
		cfg.WorkerGitlabTestsEnabled ||
		cfg.WorkerGitlabIncidentsEnabled ||
		cfg.WorkerGithubPRsEnabled || cfg.WorkerGithubPRReviewsEnabled ||
		cfg.WorkerGithubPRCommentsEnabled ||
		cfg.WorkerGithubCICDEnabled || cfg.WorkerGithubCommitsEnabled ||
		cfg.WorkerGithubDeploymentsEnabled || cfg.WorkerGithubSecurityEnabled ||
		cfg.WorkerGithubFilesEnabled || cfg.WorkerGithubCommitStatsEnabled ||
		cfg.WorkerGithubBlameEnabled || cfg.WorkerGithubTestsEnabled ||
		cfg.WorkerGithubWorkItemsEnabled ||
		cfg.WorkerGithubWorkItemsStatusMappingPath != "" ||
		cfg.WorkerGithubWorkItemsInvestmentConfigPath != "" {
		t.Fatal("provider route switches must default off")
	}
}

func TestGitHubCompleteUnitAliasesAreMutuallyExclusive(t *testing.T) {
	t.Parallel()
	_, err := Load(workerSpec(map[string]string{
		"WORKER_GITHUB_CICD_ENABLED":  "true",
		"WORKER_GITHUB_TESTS_ENABLED": "true",
	}))
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("error=%v", err)
	}
}

func TestGitHubPRSocialAliasesAreMutuallyExclusive(t *testing.T) {
	t.Parallel()
	aliases := []string{
		"WORKER_GITHUB_PRS_ENABLED",
		"WORKER_GITHUB_PR_REVIEWS_ENABLED",
		"WORKER_GITHUB_PR_COMMENTS_ENABLED",
	}
	for left := 0; left < len(aliases); left++ {
		for right := left + 1; right < len(aliases); right++ {
			_, err := Load(workerSpec(map[string]string{
				aliases[left]:  "true",
				aliases[right]: "true",
			}))
			if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
				t.Fatalf("aliases=(%s,%s) error=%v", aliases[left], aliases[right], err)
			}
		}
	}
}

func TestGitHubPRSocialAliasSwitchesParseIndependently(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  string
		pick func(Config) bool
	}{
		{"prs", "WORKER_GITHUB_PRS_ENABLED", func(cfg Config) bool { return cfg.WorkerGithubPRsEnabled }},
		{"pr-reviews", "WORKER_GITHUB_PR_REVIEWS_ENABLED", func(cfg Config) bool { return cfg.WorkerGithubPRReviewsEnabled }},
		{"pr-comments", "WORKER_GITHUB_PR_COMMENTS_ENABLED", func(cfg Config) bool { return cfg.WorkerGithubPRCommentsEnabled }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := Load(workerSpec(map[string]string{test.env: "true"}))
			if err != nil {
				t.Fatal(err)
			}
			if !test.pick(cfg) {
				t.Fatalf("%s did not enable its route switch", test.env)
			}
		})
	}
}

func TestGitLabCompleteUnitAliasesAreMutuallyExclusive(t *testing.T) {
	t.Parallel()
	_, err := Load(workerSpec(map[string]string{
		"WORKER_GITLAB_CICD_ENABLED":  "true",
		"WORKER_GITLAB_TESTS_ENABLED": "true",
	}))
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("error=%v", err)
	}
}

func TestGitLabPRSocialAliasesAreMutuallyExclusive(t *testing.T) {
	t.Parallel()
	aliases := []string{
		"WORKER_GITLAB_PRS_ENABLED",
		"WORKER_GITLAB_PR_REVIEWS_ENABLED",
		"WORKER_GITLAB_PR_COMMENTS_ENABLED",
	}
	for left := 0; left < len(aliases); left++ {
		for right := left + 1; right < len(aliases); right++ {
			_, err := Load(workerSpec(map[string]string{
				aliases[left]:  "true",
				aliases[right]: "true",
			}))
			if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
				t.Fatalf("aliases=(%s,%s) error=%v", aliases[left], aliases[right], err)
			}
		}
	}
}

func TestProviderCompletionRouteSwitchesDefaultOffParseAndLogSafely(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  string
		attr string
		pick func(Config) bool
	}{
		{"linear work items", "WORKER_LINEAR_WORK_ITEMS_ENABLED", "worker_linear_work_items_enabled", func(cfg Config) bool { return cfg.WorkerLinearWorkItemsEnabled }},
		{"jira work items", "WORKER_JIRA_WORK_ITEMS_ENABLED", "worker_jira_work_items_enabled", func(cfg Config) bool { return cfg.WorkerJiraWorkItemsEnabled }},
		{"jira incidents", "WORKER_JIRA_INCIDENTS_ENABLED", "worker_jira_incidents_enabled", func(cfg Config) bool { return cfg.WorkerJiraIncidentsEnabled }},
		{"gitlab repo metadata", "WORKER_GITLAB_REPO_METADATA_ENABLED", "worker_gitlab_repo_metadata_enabled", func(cfg Config) bool { return cfg.WorkerGitlabRepoMetadataEnabled }},
		{"gitlab commits", "WORKER_GITLAB_COMMITS_ENABLED", "worker_gitlab_commits_enabled", func(cfg Config) bool { return cfg.WorkerGitlabCommitsEnabled }},
		{"gitlab commit stats", "WORKER_GITLAB_COMMIT_STATS_ENABLED", "worker_gitlab_commit_stats_enabled", func(cfg Config) bool { return cfg.WorkerGitlabCommitStatsEnabled }},
		{"gitlab cicd", "WORKER_GITLAB_CICD_ENABLED", "worker_gitlab_cicd_enabled", func(cfg Config) bool { return cfg.WorkerGitlabCICDEnabled }},
		{"gitlab tests", "WORKER_GITLAB_TESTS_ENABLED", "worker_gitlab_tests_enabled", func(cfg Config) bool { return cfg.WorkerGitlabTestsEnabled }},
		{"gitlab incidents", "WORKER_GITLAB_INCIDENTS_ENABLED", "worker_gitlab_incidents_enabled", func(cfg Config) bool { return cfg.WorkerGitlabIncidentsEnabled }},
		{"gitlab deployments", "WORKER_GITLAB_DEPLOYMENTS_ENABLED", "worker_gitlab_deployments_enabled", func(cfg Config) bool { return cfg.WorkerGitlabDeploymentsEnabled }},
		{"gitlab feature flags", "WORKER_GITLAB_FEATURE_FLAGS_ENABLED", "worker_gitlab_feature_flags_enabled", func(cfg Config) bool { return cfg.WorkerGitlabFeatureFlagsEnabled }},
		{"gitlab files", "WORKER_GITLAB_FILES_ENABLED", "worker_gitlab_files_enabled", func(cfg Config) bool { return cfg.WorkerGitlabFilesEnabled }},
		{"gitlab blame", "WORKER_GITLAB_BLAME_ENABLED", "worker_gitlab_blame_enabled", func(cfg Config) bool { return cfg.WorkerGitlabBlameEnabled }},
		{"gitlab prs", "WORKER_GITLAB_PRS_ENABLED", "worker_gitlab_prs_enabled", func(cfg Config) bool { return cfg.WorkerGitlabPRsEnabled }},
		{"gitlab pr reviews", "WORKER_GITLAB_PR_REVIEWS_ENABLED", "worker_gitlab_pr_reviews_enabled", func(cfg Config) bool { return cfg.WorkerGitlabPRReviewsEnabled }},
		{"gitlab pr comments", "WORKER_GITLAB_PR_COMMENTS_ENABLED", "worker_gitlab_pr_comments_enabled", func(cfg Config) bool { return cfg.WorkerGitlabPRCommentsEnabled }},
		{"gitlab security", "WORKER_GITLAB_SECURITY_ENABLED", "worker_gitlab_security_enabled", func(cfg Config) bool { return cfg.WorkerGitlabSecurityEnabled }},
		{"gitlab work items", "WORKER_GITLAB_WORK_ITEMS_ENABLED", "worker_gitlab_work_items_enabled", func(cfg Config) bool { return cfg.WorkerGitlabWorkItemsEnabled }},
		{"pagerduty services", "WORKER_PAGERDUTY_SERVICES_ENABLED", "worker_pagerduty_services_enabled", func(cfg Config) bool { return cfg.WorkerPagerDutyServicesEnabled }},
		{"pagerduty business services", "WORKER_PAGERDUTY_BUSINESS_SERVICES_ENABLED", "worker_pagerduty_business_services_enabled", func(cfg Config) bool { return cfg.WorkerPagerDutyBusinessServicesEnabled }},
		{"pagerduty escalation policies", "WORKER_PAGERDUTY_ESCALATION_POLICIES_ENABLED", "worker_pagerduty_escalation_policies_enabled", func(cfg Config) bool { return cfg.WorkerPagerDutyEscalationPoliciesEnabled }},
		{"pagerduty schedules", "WORKER_PAGERDUTY_SCHEDULES_ENABLED", "worker_pagerduty_schedules_enabled", func(cfg Config) bool { return cfg.WorkerPagerDutySchedulesEnabled }},
		{"pagerduty on calls", "WORKER_PAGERDUTY_ON_CALLS_ENABLED", "worker_pagerduty_on_calls_enabled", func(cfg Config) bool { return cfg.WorkerPagerDutyOnCallsEnabled }},
		{"pagerduty users", "WORKER_PAGERDUTY_USERS_ENABLED", "worker_pagerduty_users_enabled", func(cfg Config) bool { return cfg.WorkerPagerDutyUsersEnabled }},
		{"pagerduty teams", "WORKER_PAGERDUTY_TEAMS_ENABLED", "worker_pagerduty_teams_enabled", func(cfg Config) bool { return cfg.WorkerPagerDutyTeamsEnabled }},
		{"pagerduty incidents", "WORKER_PAGERDUTY_INCIDENTS_ENABLED", "worker_pagerduty_incidents_enabled", func(cfg Config) bool { return cfg.WorkerPagerDutyIncidentsEnabled }},
	}

	defaults, err := Load(workerSpec(nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		if test.pick(defaults) {
			t.Fatalf("%s must default off", test.env)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := Load(workerSpec(map[string]string{test.env: "true"}))
			if err != nil {
				t.Fatal(err)
			}
			if !test.pick(cfg) {
				t.Fatalf("%s did not enable its route switch", test.env)
			}
			if attrs := fmt.Sprint(cfg.SafeAttrs()); !strings.Contains(attrs, test.attr+"=true") {
				t.Fatalf("safe attrs missing %s=true: %s", test.attr, attrs)
			}
		})
	}
}

func TestQueueControlAndRetentionOverridesAreBounded(t *testing.T) {
	t.Parallel()

	cfg, err := Load(workerSpec(map[string]string{
		"WORKER_DATABASE_MODE":                            "transaction",
		"PGBOUNCER_TRANSACTION_MODE":                      "true",
		"RIVER_DATABASE_SCHEMA":                           "worker_queue",
		"RIVER_DOMAIN_DATABASE_ROLE":                      "worker_domain",
		"RIVER_QUEUE_DATABASE_ROLE":                       "worker_queue",
		"WORKER_DATABASE_MAX_CONNS":                       "4",
		"WORKER_DOMAIN_DATABASE_MAX_CONNS":                "12",
		"RIVER_COMPLETED_JOB_RETENTION":                   "48h",
		"RIVER_CANCELLED_JOB_RETENTION":                   "240h",
		"RIVER_DISCARDED_JOB_RETENTION":                   "336h",
		"RIVER_JOB_CLEANER_TIMEOUT":                       "45s",
		"WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE":        "true",
		"DEV_HEALTH_STREAM_REPLICAS":                      "3",
		"WORKER_LINEAR_WORK_ITEMS_ENABLED":                "true",
		"WORKER_JIRA_WORK_ITEMS_ENABLED":                  "true",
		"WORKER_JIRA_INCIDENTS_ENABLED":                   "true",
		"WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED":       "true",
		"WORKER_GITHUB_REPO_METADATA_ENABLED":             "true",
		"WORKER_GITLAB_REPO_METADATA_ENABLED":             "true",
		"WORKER_GITLAB_COMMITS_ENABLED":                   "true",
		"WORKER_GITLAB_COMMIT_STATS_ENABLED":              "true",
		"WORKER_GITLAB_CICD_ENABLED":                      "true",
		"WORKER_GITLAB_INCIDENTS_ENABLED":                 "true",
		"WORKER_GITHUB_PRS_ENABLED":                       "true",
		"WORKER_GITHUB_PR_REVIEWS_ENABLED":                "false",
		"WORKER_GITHUB_PR_COMMENTS_ENABLED":               "false",
		"WORKER_GITHUB_CICD_ENABLED":                      "false",
		"WORKER_GITHUB_COMMITS_ENABLED":                   "true",
		"WORKER_GITHUB_DEPLOYMENTS_ENABLED":               "true",
		"WORKER_GITHUB_SECURITY_ENABLED":                  "true",
		"WORKER_GITHUB_FILES_ENABLED":                     "true",
		"WORKER_GITHUB_COMMIT_STATS_ENABLED":              "true",
		"WORKER_GITHUB_BLAME_ENABLED":                     "true",
		"WORKER_GITHUB_TESTS_ENABLED":                     "true",
		"WORKER_GITHUB_WORK_ITEMS_ENABLED":                "true",
		"WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH":    "/config/status.yaml",
		"WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH": "/config/investment.yaml",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.QueueDatabaseMode != QueueControlTransaction || cfg.QueueDatabaseMaxConns != 4 || cfg.DomainDatabaseMaxConns != 12 {
		t.Fatalf("unexpected queue settings: %#v", cfg.SafeAttrs())
	}
	if cfg.RiverDatabaseSchema != "worker_queue" {
		t.Fatalf("River schema = %q", cfg.RiverDatabaseSchema)
	}
	if cfg.DomainDatabaseRole != "worker_domain" || cfg.QueueDatabaseRole != "worker_queue" {
		t.Fatalf("runtime roles = domain=%q queue=%q", cfg.DomainDatabaseRole, cfg.QueueDatabaseRole)
	}
	if !cfg.DomainTransactionPooler {
		t.Fatal("expected domain transaction-pooler mode")
	}
	if cfg.CompletedJobRetention != 48*time.Hour || cfg.RiverJobCleanerTimeout != 45*time.Second {
		t.Fatalf("unexpected retention settings: %#v", cfg.SafeAttrs())
	}
	if !cfg.OperationalBridgeAllowInsecure {
		t.Fatal("expected explicit insecure operational bridge opt-in")
	}
	if cfg.StreamConfiguredReplicas != 3 {
		t.Fatalf("stream replicas = %d, want 3", cfg.StreamConfiguredReplicas)
	}
	if !cfg.WorkerLinearWorkItemsEnabled || !cfg.WorkerJiraWorkItemsEnabled ||
		!cfg.WorkerJiraIncidentsEnabled ||
		!cfg.WorkerLaunchDarklyFeatureFlagsEnabled ||
		!cfg.WorkerGithubRepoMetadataEnabled || !cfg.WorkerGitlabRepoMetadataEnabled ||
		!cfg.WorkerGitlabCommitsEnabled ||
		!cfg.WorkerGitlabCommitStatsEnabled ||
		!cfg.WorkerGitlabCICDEnabled ||
		cfg.WorkerGitlabTestsEnabled ||
		!cfg.WorkerGitlabIncidentsEnabled ||
		!cfg.WorkerGithubPRsEnabled || cfg.WorkerGithubPRReviewsEnabled ||
		cfg.WorkerGithubPRCommentsEnabled ||
		cfg.WorkerGithubCICDEnabled || !cfg.WorkerGithubCommitsEnabled ||
		!cfg.WorkerGithubDeploymentsEnabled || !cfg.WorkerGithubSecurityEnabled ||
		!cfg.WorkerGithubFilesEnabled || !cfg.WorkerGithubCommitStatsEnabled ||
		!cfg.WorkerGithubBlameEnabled || !cfg.WorkerGithubTestsEnabled ||
		!cfg.WorkerGithubWorkItemsEnabled ||
		cfg.WorkerGithubWorkItemsStatusMappingPath != "/config/status.yaml" ||
		cfg.WorkerGithubWorkItemsInvestmentConfigPath != "/config/investment.yaml" {
		t.Fatal("expected independent provider route opt-ins")
	}

	for key, value := range map[string]string{
		"WORKER_DATABASE_MODE":                         "arbitrary",
		"WORKER_DATABASE_MAX_CONNS":                    "5",
		"WORKER_DOMAIN_DATABASE_MAX_CONNS":             "0",
		"RIVER_COMPLETED_JOB_RETENTION":                "23h",
		"RIVER_JOB_CLEANER_TIMEOUT":                    "4s",
		"RIVER_DATABASE_SCHEMA":                        "River-Bad",
		"RIVER_DOMAIN_DATABASE_ROLE":                   "Domain-Bad",
		"RIVER_QUEUE_DATABASE_ROLE":                    "Queue-Bad",
		"PGBOUNCER_TRANSACTION_MODE":                   "sometimes",
		"WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE":     "sometimes",
		"DEV_HEALTH_STREAM_REPLICAS":                   "9",
		"WORKER_LINEAR_WORK_ITEMS_ENABLED":             "sometimes",
		"WORKER_JIRA_WORK_ITEMS_ENABLED":               "sometimes",
		"WORKER_JIRA_INCIDENTS_ENABLED":                "sometimes",
		"WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED":    "sometimes",
		"WORKER_GITHUB_REPO_METADATA_ENABLED":          "sometimes",
		"WORKER_GITLAB_REPO_METADATA_ENABLED":          "sometimes",
		"WORKER_GITLAB_COMMITS_ENABLED":                "sometimes",
		"WORKER_GITLAB_COMMIT_STATS_ENABLED":           "sometimes",
		"WORKER_GITLAB_CICD_ENABLED":                   "sometimes",
		"WORKER_GITLAB_TESTS_ENABLED":                  "sometimes",
		"WORKER_GITLAB_INCIDENTS_ENABLED":              "sometimes",
		"WORKER_GITLAB_DEPLOYMENTS_ENABLED":            "sometimes",
		"WORKER_GITLAB_FEATURE_FLAGS_ENABLED":          "sometimes",
		"WORKER_GITLAB_FILES_ENABLED":                  "sometimes",
		"WORKER_GITLAB_BLAME_ENABLED":                  "sometimes",
		"WORKER_GITLAB_PRS_ENABLED":                    "sometimes",
		"WORKER_GITLAB_PR_REVIEWS_ENABLED":             "sometimes",
		"WORKER_GITLAB_PR_COMMENTS_ENABLED":            "sometimes",
		"WORKER_GITLAB_SECURITY_ENABLED":               "sometimes",
		"WORKER_GITLAB_WORK_ITEMS_ENABLED":             "sometimes",
		"WORKER_PAGERDUTY_SERVICES_ENABLED":            "sometimes",
		"WORKER_PAGERDUTY_BUSINESS_SERVICES_ENABLED":   "sometimes",
		"WORKER_PAGERDUTY_ESCALATION_POLICIES_ENABLED": "sometimes",
		"WORKER_PAGERDUTY_SCHEDULES_ENABLED":           "sometimes",
		"WORKER_PAGERDUTY_ON_CALLS_ENABLED":            "sometimes",
		"WORKER_PAGERDUTY_USERS_ENABLED":               "sometimes",
		"WORKER_PAGERDUTY_TEAMS_ENABLED":               "sometimes",
		"WORKER_PAGERDUTY_INCIDENTS_ENABLED":           "sometimes",
		"WORKER_GITHUB_PRS_ENABLED":                    "sometimes",
		"WORKER_GITHUB_PR_REVIEWS_ENABLED":             "sometimes",
		"WORKER_GITHUB_PR_COMMENTS_ENABLED":            "sometimes",
		"WORKER_GITHUB_COMMITS_ENABLED":                "sometimes",
		"WORKER_GITHUB_COMMIT_STATS_ENABLED":           "sometimes",
		"WORKER_GITHUB_BLAME_ENABLED":                  "sometimes",
		"WORKER_GITHUB_WORK_ITEMS_ENABLED":             "sometimes",
	} {
		if _, err := Load(workerSpec(map[string]string{key: value})); err == nil {
			t.Fatalf("expected %s=%q to fail", key, value)
		}
	}
	if _, err := Load(workerSpec(map[string]string{
		"RIVER_DOMAIN_DATABASE_ROLE": "same_role",
		"RIVER_QUEUE_DATABASE_ROLE":  "same_role",
	})); err == nil {
		t.Fatal("expected shared runtime roles to fail")
	}
}

func TestValidationErrorsDoNotEchoInvalidValues(t *testing.T) {
	t.Parallel()

	secret := "postgres://user:do-not-print@"
	_, err := Load(workerSpec(map[string]string{"POSTGRES_URI": secret}))
	if err == nil {
		t.Fatal("expected invalid URI")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "do-not-print") {
		t.Fatalf("error leaked invalid secret: %v", err)
	}
}
