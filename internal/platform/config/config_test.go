package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func lookup(values map[string]string) secrets.LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// workerSpec is the profile-free worker surface. The worker's named profiles
// (latency/sync/heavy/ops) were retired when queue topology became explicit,
// and profile resolution itself now lives in internal/platform/shell
// (CHAOS-3875), so config.Spec carries only an already-resolved value.
func workerSpec(values map[string]string) Spec {
	return Spec{
		Service:   "dev-health-worker",
		LookupEnv: lookup(values),
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
		"single comma-joined flag": {
			spec: queueWorkerSpec(map[string]string{
				"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=3,webhooks=9",
			}, "webhooks, heartbeat"),
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
		"queue env is not a configuration path": queueWorkerSpec(
			map[string]string{
				"DEV_HEALTH_QUEUES":            "heartbeat",
				"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=1",
			},
		),
		"profile compatibility": {
			Service:       "dev-health-worker",
			Profile:       "ops",
			RequireQueues: true,
			Queues:        []string{"heartbeat"},
			LookupEnv:     lookup(nil),
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

// TestFlagOverridesTakePrecedenceOverEnvironment pins the resolution order
// CHAOS-4020 introduced. These three settings previously made a Load that named
// both surfaces a hard error. That could not survive the migration this ticket
// performs: deployed configuration moves into `command:` while host .env files
// still carry the same variables, so a conflict rule would have failed every
// worker at the moment the flags were added. The environment is a fallback
// beneath the flag, and the shadowed setting is reported rather than fatal.
func TestFlagOverridesTakePrecedenceOverEnvironment(t *testing.T) {
	t.Parallel()

	cfg, err := Load(Spec{
		Service:       "dev-health-worker",
		RequireQueues: true,
		Queues:        []string{"heartbeat"},
		Overrides: map[string]string{
			"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=1",
			"DEV_HEALTH_WORKER_GROUP":      "command-group",
			"DEV_HEALTH_SHUTDOWN_TIMEOUT":  "30s",
		},
		LookupEnv: lookup(map[string]string{
			"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=2",
			"DEV_HEALTH_WORKER_GROUP":      "environment-group",
			"DEV_HEALTH_SHUTDOWN_TIMEOUT":  "60s",
		}),
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.WorkerGroup != "command-group" {
		t.Fatalf("worker group = %q, want the flag value", cfg.WorkerGroup)
	}
	if cfg.WorkerQueueConcurrency["heartbeat"] != 1 {
		t.Fatalf("heartbeat concurrency = %d, want the flag value 1", cfg.WorkerQueueConcurrency["heartbeat"])
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Fatalf("shutdown timeout = %s, want the flag value 30s", cfg.ShutdownTimeout)
	}
	// Every setting came from a flag, so none is reported as environment-only.
	if len(cfg.EnvOnlySettings) != 0 {
		t.Fatalf("EnvOnlySettings = %v, want empty when every setting was a flag", cfg.EnvOnlySettings)
	}
}

// TestEnvironmentFallbackIsReportedNotSilent proves the other half: a setting
// that has a flag but was supplied through the environment still works and is
// named, so a deployment configured the old way is visible at startup instead
// of merely functioning.
func TestEnvironmentFallbackIsReportedNotSilent(t *testing.T) {
	t.Parallel()

	cfg, err := Load(Spec{
		Service:       "dev-health-worker",
		RequireQueues: true,
		Queues:        []string{"heartbeat"},
		Overrides:     map[string]string{"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=1"},
		LookupEnv: lookup(map[string]string{
			"DEV_HEALTH_WORKER_GROUP": "environment-group",
			"DEV_HEALTH_LOG_LEVEL":    "debug",
		}),
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.WorkerGroup != "environment-group" {
		t.Fatalf("worker group = %q, want the environment fallback", cfg.WorkerGroup)
	}
	want := []string{"log-level", "worker-group"}
	if !slices.Equal(cfg.EnvOnlySettings, want) {
		t.Fatalf("EnvOnlySettings = %v, want %v", cfg.EnvOnlySettings, want)
	}
}

// TestOverridesRejectCredentials pins the rule that keeps DSNs and tokens off
// the command line, where `ps`, `docker inspect`, and `docker compose config`
// would all expose them.
func TestOverridesRejectCredentials(t *testing.T) {
	t.Parallel()

	for name, override := range map[string]map[string]string{
		"credential": {"POSTGRES_URI": "postgres://user:pw@host/db"},
		"undeclared": {"NOT_A_SETTING": "value"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			spec := workerSpec(nil)
			spec.Overrides = override
			if _, err := Load(spec); err == nil {
				t.Fatal("expected the override to be rejected")
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

func TestProfileIsCarriedVerbatimAndNeverResolvedHere(t *testing.T) {
	t.Parallel()

	// Selection, the DEV_HEALTH_PROFILE fallback, and membership checking are
	// the shell's job (CHAOS-3875). Load must neither read the environment for
	// a profile nor second-guess the value it is handed -- a service that
	// declares no profiles is the only thing it still rejects.
	spec := workerSpec(map[string]string{"DEV_HEALTH_PROFILE": "heavy"})
	cfg, err := Load(spec)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Profile != "" {
		t.Fatalf("Load resolved a profile from the environment: %q", cfg.Profile)
	}

	spec = workerSpec(nil)
	spec.Profile = "arbitrary"
	cfg, err = Load(spec)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Profile != "arbitrary" {
		t.Fatalf("profile = %q, want the caller's resolved value verbatim", cfg.Profile)
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

// TestSafeAttrsReportFormAndNameOfEachResolvedDSN is round 4's (2026-09-11)
// observability requirement (chris's ruling): a successful resolution used
// to leave only "*_database_configured=true" observable -- which of the
// two DSN forms actually won, and which database an operator's config
// reaches, was invisible at Info, so silently selecting the wrong
// (but reachable) database was a regression no one could see without
// reading the DSN itself. Neither new field is a credential.
func TestSafeAttrsReportFormAndNameOfEachResolvedDSN(t *testing.T) {
	t.Parallel()

	cfg, err := Load(workerSpec(map[string]string{
		"POSTGRES_URI":                 "postgresql://app:app@db.internal:5432/appdb",
		"DEV_HEALTH_PG_QUEUE_HOST":     "queue.internal",
		"DEV_HEALTH_PG_QUEUE_USER":     "app",
		"DEV_HEALTH_PG_QUEUE_PASSWORD": "app",
		"DEV_HEALTH_PG_DB":             "queuedb",
	}))
	if err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(cfg.SafeAttrs())
	for _, want := range []string{
		"domain_database_form=uri",
		"queue_database_form=components", "queue_database_name=queuedb",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("safe attrs missing %q: %s", want, text)
		}
	}
	// Round 6 ruling: the pre-built-URI form NEVER has a Name -- chris's
	// "no parsing of a URI for telemetry" rule -- so domain_database_name
	// must be entirely absent, not an empty string.
	if strings.Contains(text, "domain_database_name") {
		t.Fatalf("safe attrs emitted a Name for the URI form, which must never be parsed for telemetry: %s", text)
	}
	// Coordinator and ClickHouse were never configured in this Spec --
	// neither field is emitted for a DSN that never resolved.
	for _, absent := range []string{"coordinator_database_form", "coordinator_database_name", "clickhouse_form", "clickhouse_name"} {
		if strings.Contains(text, absent) {
			t.Fatalf("safe attrs emitted %q for an unconfigured DSN: %s", absent, text)
		}
	}
}

// TestComponentDatabaseNameNeverParsesAURI is round 6's (2026-09-11)
// ruling, replacing round 5's parse-and-sanitize approach entirely: no
// telemetry field is ever derived by parsing a DSN. ComponentDatabaseName
// reads the separate, non-secret DBKey env value directly -- proven here
// by handing it a lookup where the ONLY sensible value comes from that
// key, including a case where a raw MIGRATION_DATABASE_URI-shaped
// pre-built value is ALSO present in the same environment (component
// resolution never even looks at it for this purpose).
func TestComponentDatabaseNameNeverParsesAURI(t *testing.T) {
	t.Parallel()

	t.Run("reads the DBKey value directly", func(t *testing.T) {
		t.Parallel()
		got := ComponentDatabaseName(lookup(map[string]string{
			"DEV_HEALTH_PG_DOMAIN_DB": "appdb",
		}), ComponentSpec{DBKey: "DEV_HEALTH_PG_DOMAIN_DB", DefaultDB: "postgres"})
		if got != "appdb" {
			t.Fatalf("got %q, want %q", got, "appdb")
		}
	})

	t.Run("falls back to DefaultDB when unset", func(t *testing.T) {
		t.Parallel()
		got := ComponentDatabaseName(lookup(nil), ComponentSpec{DBKey: "DEV_HEALTH_PG_DOMAIN_DB", DefaultDB: "postgres"})
		if got != "postgres" {
			t.Fatalf("got %q, want %q", got, "postgres")
		}
	})

	t.Run("never influenced by an unrelated raw URI in the same environment", func(t *testing.T) {
		t.Parallel()
		got := ComponentDatabaseName(lookup(map[string]string{
			"DEV_HEALTH_PG_DOMAIN_DB": "appdb",
			"POSTGRES_URI":            "postgresql://user:pass@word/extra@host:5432/realdb",
		}), ComponentSpec{DBKey: "DEV_HEALTH_PG_DOMAIN_DB", DefaultDB: "postgres"})
		if got != "appdb" {
			t.Fatalf("got %q, want %q -- must never be influenced by an unrelated raw URI", got, "appdb")
		}
	})
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
	// CHAOS-4054: these default to the artifacts every worker image ships,
	// because a process that serves the provider-unit queue must be able to
	// serve the work-item family and no switch can say otherwise any more.
	if cfg.WorkerGithubWorkItemsStatusMappingPath != "/app/config/status_mapping.yaml" ||
		cfg.WorkerGithubWorkItemsInvestmentConfigPath != "/app/config/investment_areas.yaml" {
		t.Fatalf("work-item runtime artifact paths = %q, %q; want the packaged image defaults",
			cfg.WorkerGithubWorkItemsStatusMappingPath,
			cfg.WorkerGithubWorkItemsInvestmentConfigPath)
	}
	// CHAOS-4291: same packaged-image-default reasoning as the work-item
	// paths above.
	if cfg.WorkerRemainingComplexityConfigPath != "/app/config/complexity.yaml" {
		t.Fatalf("complexity config path = %q; want the packaged image default",
			cfg.WorkerRemainingComplexityConfigPath)
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
		"WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH":    "/config/status.yaml",
		"WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH": "/config/investment.yaml",
		"WORKER_REMAINING_COMPLEXITY_CONFIG_PATH":         "/config/complexity.yaml",
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
	if cfg.WorkerGithubWorkItemsStatusMappingPath != "/config/status.yaml" ||
		cfg.WorkerGithubWorkItemsInvestmentConfigPath != "/config/investment.yaml" {
		t.Fatal("expected the explicit work-item runtime artifact paths")
	}
	if cfg.WorkerRemainingComplexityConfigPath != "/config/complexity.yaml" {
		t.Fatalf("complexity config path = %q; want the explicit override",
			cfg.WorkerRemainingComplexityConfigPath)
	}

	for key, value := range map[string]string{
		"WORKER_DATABASE_MODE":                     "arbitrary",
		"WORKER_DATABASE_MAX_CONNS":                "5",
		"WORKER_DOMAIN_DATABASE_MAX_CONNS":         "0",
		"RIVER_COMPLETED_JOB_RETENTION":            "23h",
		"RIVER_JOB_CLEANER_TIMEOUT":                "4s",
		"RIVER_DATABASE_SCHEMA":                    "River-Bad",
		"RIVER_DOMAIN_DATABASE_ROLE":               "Domain-Bad",
		"RIVER_QUEUE_DATABASE_ROLE":                "Queue-Bad",
		"PGBOUNCER_TRANSACTION_MODE":               "sometimes",
		"WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE": "sometimes",
		"DEV_HEALTH_STREAM_REPLICAS":               "9",
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

// CHAOS-4005 / CHAOS-4020: the flag is canonical and the environment is only a
// fallback. A deployment that still carries the env var must not override an
// operator who passed the flag explicitly.
func TestUnreclaimableSweepFlagBeatsEnvironment(t *testing.T) {
	for _, testCase := range []struct {
		name string
		flag string
		env  map[string]string
		want string
	}{
		{"flag only", "active", nil, "active"},
		{"env only", "", map[string]string{"SYNC_UNRECLAIMABLE_SWEEP": "active"}, "active"},
		{"flag wins", "off", map[string]string{"SYNC_UNRECLAIMABLE_SWEEP": "active"}, "off"},
		{"neither", "", nil, ""},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			spec := Spec{
				Service:   "dev-health-reconciler",
				LookupEnv: lookup(testCase.env),
			}
			// The flag reaches Load as an override keyed by the variable it
			// shadows, exactly as the shell layer supplies it.
			if testCase.flag != "" {
				spec.Overrides = map[string]string{
					"SYNC_UNRECLAIMABLE_SWEEP": testCase.flag,
				}
			}
			cfg, err := Load(spec)
			if err != nil {
				t.Fatalf("load config: %v", err)
			}
			if cfg.UnreclaimableSweepMode != testCase.want {
				t.Fatalf("UnreclaimableSweepMode = %q, want %q",
					cfg.UnreclaimableSweepMode, testCase.want)
			}
		})
	}
}

// TestSyncObservationTimeoutDefaultAndOverride pins CHAOS-4092's new
// reconciler-only knob: it defaults to syncreconciler's unconfigured 2s
// (so the option's introduction changes nothing for a deployment that never
// sets it), an in-bounds override reaches Config verbatim, and an
// out-of-bounds value is rejected the same way every other durationEnv
// setting is -- Load fails rather than silently clamping.
func TestSyncObservationTimeoutDefaultAndOverride(t *testing.T) {
	t.Parallel()

	reconcilerSpec := func(values map[string]string) Spec {
		return Spec{Service: "dev-health-reconciler", LookupEnv: lookup(values)}
	}

	t.Run("defaults to 2s when unset", func(t *testing.T) {
		t.Parallel()
		cfg, err := Load(reconcilerSpec(nil))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.SyncObservationTimeout != 2*time.Second {
			t.Fatalf("SyncObservationTimeout = %s, want 2s", cfg.SyncObservationTimeout)
		}
	})

	t.Run("env override reaches Config", func(t *testing.T) {
		t.Parallel()
		cfg, err := Load(reconcilerSpec(map[string]string{"SYNC_OBSERVATION_TIMEOUT": "9s"}))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.SyncObservationTimeout != 9*time.Second {
			t.Fatalf("SyncObservationTimeout = %s, want 9s", cfg.SyncObservationTimeout)
		}
	})

	t.Run("flag beats env, matching every other layered setting", func(t *testing.T) {
		t.Parallel()
		spec := reconcilerSpec(map[string]string{"SYNC_OBSERVATION_TIMEOUT": "9s"})
		spec.Overrides = map[string]string{"SYNC_OBSERVATION_TIMEOUT": "1s"}
		cfg, err := Load(spec)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.SyncObservationTimeout != 1*time.Second {
			t.Fatalf("SyncObservationTimeout = %s, want the flag's 1s", cfg.SyncObservationTimeout)
		}
	})

	for _, out := range []string{"5ms", "31s", "not-a-duration"} {
		out := out
		t.Run("rejects out-of-bounds "+out, func(t *testing.T) {
			t.Parallel()
			if _, err := Load(reconcilerSpec(map[string]string{"SYNC_OBSERVATION_TIMEOUT": out})); err == nil {
				t.Fatalf("expected SYNC_OBSERVATION_TIMEOUT=%q to fail", out)
			}
		})
	}

	// CHAOS-4092 / Codex adversarial review (round 1): the reconciler-only
	// option must not be parsed for every service. Every Go binary reads
	// from the same shared compose environment
	// (deploy/docker-compose/compose.go-workers.yml's go-worker-env-base),
	// so a value scoped to the reconciler's own deployment must not be able
	// to fail Load for the worker or scheduler -- a malformed value meant
	// only for the reconciler must not be able to break every OTHER Go
	// service's startup.
	for _, service := range []string{"dev-health-worker", "dev-health-scheduler"} {
		service := service
		t.Run("out-of-bounds value is inert for "+service, func(t *testing.T) {
			t.Parallel()
			for _, out := range []string{"5ms", "31s", "not-a-duration"} {
				spec := Spec{
					Service:   service,
					LookupEnv: lookup(map[string]string{"SYNC_OBSERVATION_TIMEOUT": out}),
				}
				cfg, err := Load(spec)
				if err != nil {
					t.Fatalf("SYNC_OBSERVATION_TIMEOUT=%q failed Load for %s: %v", out, service, err)
				}
				if cfg.SyncObservationTimeout != 2*time.Second {
					t.Fatalf(
						"SyncObservationTimeout = %s for %s with SYNC_OBSERVATION_TIMEOUT=%q, want the 2s default ignored",
						cfg.SyncObservationTimeout, service, out,
					)
				}
			}
		})
	}
}

// TestBlankValuesAreTreatedAsUnsetOnBothSurfaces pins the edge the removed
// conflict branches used to arbitrate.
//
// ShutdownTimeoutExplicit feeds cmd/dev-health-worker's drain-budget decision:
// when it is false and the grace is at the package default, the worker derives
// its budget from the queue selection instead of trusting 30s. A blank value
// that read as "explicitly set" would silently hand a real worker a 30s drain
// budget that no real queue selection can satisfy, so blank must mean unset on
// BOTH surfaces, exactly as it did before.
func TestBlankValuesAreTreatedAsUnsetOnBothSurfaces(t *testing.T) {
	t.Parallel()

	for name, spec := range map[string]Spec{
		"blank environment": {
			Service:       "dev-health-worker",
			RequireQueues: true,
			Queues:        []string{"heartbeat"},
			Overrides:     map[string]string{"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=1"},
			LookupEnv: lookup(map[string]string{
				"DEV_HEALTH_SHUTDOWN_TIMEOUT": "   ",
			}),
		},
		"blank flag": {
			Service:       "dev-health-worker",
			RequireQueues: true,
			Queues:        []string{"heartbeat"},
			Overrides: map[string]string{
				"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=1",
				"DEV_HEALTH_SHUTDOWN_TIMEOUT":  "",
			},
			LookupEnv: lookup(nil),
		},
		"absent entirely": {
			Service:       "dev-health-worker",
			RequireQueues: true,
			Queues:        []string{"heartbeat"},
			Overrides:     map[string]string{"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=1"},
			LookupEnv:     lookup(nil),
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg, err := Load(spec)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if cfg.ShutdownTimeoutExplicit {
				t.Error("a blank or absent value must not read as an operator choice")
			}
			if cfg.ShutdownTimeout != DefaultShutdownTimeout {
				t.Fatalf("shutdown timeout = %s, want the package default", cfg.ShutdownTimeout)
			}
		})
	}
}

// TestBlankWorkerGroupFallsBackToTheDefaultName keeps the other blank-value
// path honest: an empty group on either surface is the default identity, not a
// validation failure and not an empty label.
func TestBlankWorkerGroupFallsBackToTheDefaultName(t *testing.T) {
	t.Parallel()

	cfg, err := Load(Spec{
		Service:       "dev-health-worker",
		RequireQueues: true,
		Queues:        []string{"heartbeat"},
		Overrides: map[string]string{
			"DEV_HEALTH_QUEUE_CONCURRENCY": "heartbeat=1",
			"DEV_HEALTH_WORKER_GROUP":      "   ",
		},
		LookupEnv: lookup(map[string]string{"DEV_HEALTH_WORKER_GROUP": "  "}),
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.WorkerGroup != "worker" {
		t.Fatalf("worker group = %q, want the default identity", cfg.WorkerGroup)
	}
}

// TestNonQueueServicesStillRejectQueueRuntimeSettings keeps the guard the
// layered lookup could have quietly weakened: a reconciler or scheduler that
// inherits a worker's environment must still refuse queue runtime settings
// rather than silently ignoring them.
func TestNonQueueServicesStillRejectQueueRuntimeSettings(t *testing.T) {
	t.Parallel()

	for name, values := range map[string]map[string]string{
		"worker group":      {"DEV_HEALTH_WORKER_GROUP": "inherited"},
		"queue concurrency": {"DEV_HEALTH_QUEUE_CONCURRENCY": "sync=2"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := Load(workerSpec(values)); err == nil {
				t.Fatal("a non-queue service must reject queue runtime settings")
			}
		})
	}
}

// TestQueueSelectionAcceptsDottedSubpathNames is a forward-compatibility guard
// for CHAOS-4027.
//
// That ticket introduces finer-grained River queues following a dotted
// convention, so an operator can shard by hand: -Q sync.github for a provider,
// -Q sync.github.heavy for one cost class. The CLI needs no change to support
// it, because -Q takes an arbitrary queue list and validation is a charset
// check plus the runtime's selected-equals-constructed rule -- NOT a
// restriction on queue-name shape.
//
// This test exists so that stays true. Tightening queue validation into a
// pattern that assumes flat names would break CHAOS-4027 silently, at the
// moment those queues first appear rather than in the change that caused it.
func TestQueueSelectionAcceptsDottedSubpathNames(t *testing.T) {
	t.Parallel()

	queues := []string{"sync.github", "sync.github.heavy", "sync_provider"}
	cfg, err := Load(Spec{
		Service:       "dev-health-worker",
		RequireQueues: true,
		Queues:        []string{strings.Join(queues, ",")},
		Overrides: map[string]string{
			"DEV_HEALTH_QUEUE_CONCURRENCY": "sync.github=1,sync.github.heavy=2,sync_provider=1",
		},
		LookupEnv: lookup(nil),
	})
	if err != nil {
		t.Fatalf("dotted subpath queue names must be accepted: %v", err)
	}
	want := []string{"sync.github", "sync.github.heavy", "sync_provider"}
	if !slices.Equal(cfg.Queues, want) {
		t.Fatalf("queues = %v, want %v", cfg.Queues, want)
	}
	if cfg.WorkerQueueConcurrency["sync.github.heavy"] != 2 {
		t.Fatalf("per-queue concurrency must key on the dotted name: %v", cfg.WorkerQueueConcurrency)
	}
}

// TestNoRouteEnablementSurfaceExists is the config half of CHAOS-4054's
// acceptance: the route enablement plane is gone, not merely defaulted off.
// Every WORKER_*_ENABLED name is unrecognised, so setting one cannot change
// this process's behaviour, cannot fail startup, and cannot appear in the
// startup evidence an operator reads.
func TestNoRouteEnablementSurfaceExists(t *testing.T) {
	t.Parallel()

	routeSwitchNames := []string{
		"WORKER_GITHUB_PRS_ENABLED",
		"WORKER_GITHUB_PR_REVIEWS_ENABLED",
		"WORKER_GITHUB_PR_COMMENTS_ENABLED",
		"WORKER_GITHUB_CICD_ENABLED",
		"WORKER_GITHUB_TESTS_ENABLED",
		"WORKER_GITHUB_WORK_ITEMS_ENABLED",
		"WORKER_GITLAB_CICD_ENABLED",
		"WORKER_GITLAB_TESTS_ENABLED",
		"WORKER_LINEAR_WORK_ITEMS_ENABLED",
		"WORKER_PAGERDUTY_INCIDENTS_ENABLED",
	}

	// None of them is a recognised environment name any more.
	known := make(map[string]bool, len(EnvNames()))
	for _, name := range EnvNames() {
		known[name] = true
	}
	for _, name := range routeSwitchNames {
		if known[name] {
			t.Fatalf("%s is still a recognised environment name", name)
		}
	}

	// Setting the historically mutually-exclusive pair together used to fail
	// startup. It is now simply ignored: exclusivity lives in the execution
	// registry as "the alias is not a plannable identity", not as two booleans
	// nobody may set at once.
	environment := make(map[string]string, len(routeSwitchNames))
	for _, name := range routeSwitchNames {
		environment[name] = "true"
	}
	withSwitches, err := Load(workerSpec(environment))
	if err != nil {
		t.Fatalf("route switch names must be inert, not rejected: %v", err)
	}
	baseline, err := Load(workerSpec(nil))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(withSwitches.SafeAttrs()) != fmt.Sprint(baseline.SafeAttrs()) {
		t.Fatal("route switch names changed the resolved configuration")
	}
	if strings.Contains(fmt.Sprint(withSwitches.SafeAttrs()), "_enabled=") {
		t.Fatalf("startup evidence still reports a route switch: %s",
			fmt.Sprint(withSwitches.SafeAttrs()))
	}
}

// TestComponentKeysMatchesSpecFieldsMinusTheSharedDBException is round-3's
// (2026-09-11) structural fix, pinned directly: componentKeys() must be
// derived from ComponentSpec's own fields, never drift from them again the
// way the round-2 hand-picked slice did (it silently omitted DBKey for
// every spec and both fields' `_FILE` variants entirely).
func TestComponentKeysMatchesSpecFieldsMinusTheSharedDBException(t *testing.T) {
	t.Parallel()

	for name, spec := range map[string]ComponentSpec{
		"domain":      DomainDatabaseSpec,
		"queue":       QueueDatabaseSpec,
		"coordinator": CoordinatorDatabaseSpec,
		"clickhouse":  ClickHouseSpec,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			want := []string{spec.HostKey, spec.PortKey, spec.UserKey, spec.UserKey + "_FILE", spec.PasswordKey, spec.PasswordKey + "_FILE"}
			if !spec.DBKeyShared {
				want = append(want, spec.DBKey)
			}
			got := spec.componentKeys()
			gotSet := make(map[string]bool, len(got))
			for _, k := range got {
				gotSet[k] = true
			}
			wantSet := make(map[string]bool, len(want))
			for _, k := range want {
				wantSet[k] = true
			}
			if len(got) != len(gotSet) {
				t.Fatalf("componentKeys() returned a duplicate: %v", got)
			}
			for k := range wantSet {
				if !gotSet[k] {
					t.Fatalf("componentKeys() missing %q, got %v", k, got)
				}
			}
			for k := range gotSet {
				if !wantSet[k] {
					t.Fatalf("componentKeys() has unexpected %q, got %v want %v", k, got, want)
				}
			}
		})
	}

	// The shared exception itself: DomainDatabaseSpec.DBKey must be absent
	// from its own componentKeys().
	for _, key := range DomainDatabaseSpec.componentKeys() {
		if key == DomainDatabaseSpec.DBKey {
			t.Fatalf("DomainDatabaseSpec.componentKeys() must exclude the shared DBKey %q, got %v",
				DomainDatabaseSpec.DBKey, DomainDatabaseSpec.componentKeys())
		}
	}
	// ClickHouse's DB key is NOT shared and must be present.
	found := false
	for _, key := range ClickHouseSpec.componentKeys() {
		if key == ClickHouseSpec.DBKey {
			found = true
		}
	}
	if !found {
		t.Fatalf("ClickHouseSpec.componentKeys() must include its own (unshared) DBKey %q, got %v",
			ClickHouseSpec.DBKey, ClickHouseSpec.componentKeys())
	}
}

// TestResolveDSNFromComponentsInputDomain pins CHAOS-5560's per-field input
// domain: the whole point of the component form is that url.UserPassword
// percent-encodes whatever it is given, so none of the reserved-character
// classes below should behave any differently from the plain-ASCII case --
// that IS the fix. Executed against the real function, one cell per row.
func TestResolveDSNFromComponentsInputDomain(t *testing.T) {
	t.Parallel()

	base := map[string]string{
		"TEST_HOST": "db.internal",
		"TEST_PORT": "5432",
		"TEST_USER": "app",
		"TEST_PASS": "app",
		"TEST_DB":   "appdb",
	}
	spec := ComponentSpec{
		HostKey: "TEST_HOST", PortKey: "TEST_PORT", DefaultPort: "5432",
		UserKey: "TEST_USER", PasswordKey: "TEST_PASS",
		DBKey: "TEST_DB", DefaultDB: "postgres", Scheme: "postgresql",
	}

	cases := []struct {
		name      string
		mutate    func(map[string]string)
		wantUsed  bool
		wantErr   bool
		wantHost  string // "" = don't check
		wantUser  string
		checkUser bool
		wantDB    string // "" = don't check
	}{
		{name: "absent host", mutate: func(m map[string]string) { delete(m, "TEST_HOST") }, wantUsed: false},
		// Round-4 (2026-09-11) finding (Trap #140: never TrimSpace an
		// identifier): a whitespace-only host used to be treated as
		// "absent" (trimmed to empty). It is non-empty raw input -- the
		// component form now activates and explicitly refuses it, rather
		// than silently treating it the same as no host at all.
		{name: "whitespace-only host -- activates and is explicitly refused", mutate: func(m map[string]string) { m["TEST_HOST"] = "   " }, wantUsed: true, wantErr: true},
		{name: "canonical", wantUsed: true, wantHost: "db.internal"},
		{
			name: "password with # (RFC3986 reserved, fragment)", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_PASS"] = "p#ss" },
		},
		{
			name: "password with @ (RFC3986 reserved, userinfo delimiter)", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_PASS"] = "p@ss" },
		},
		{
			name: "password with : (RFC3986 reserved, userinfo separator)", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_PASS"] = "p:ss" },
		},
		{
			name: "password with / (RFC3986 reserved, path delimiter)", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_PASS"] = "p/ss" },
		},
		{
			name: "password with ? (RFC3986 reserved, query delimiter)", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_PASS"] = "p?ss" },
		},
		{
			name: "password with space", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_PASS"] = "p ss" },
		},
		{
			name: "password with unicode", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_PASS"] = "päßword" },
		},
		{
			name: "password absent, user present (canonical: no auth)", wantUsed: true,
			mutate: func(m map[string]string) { delete(m, "TEST_PASS") },
		},
		{
			name: "password present, user absent -- refused", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { delete(m, "TEST_USER") },
		},
		{
			name: "user and password both absent (canonical: no auth at all)", wantUsed: true,
			mutate: func(m map[string]string) { delete(m, "TEST_USER"); delete(m, "TEST_PASS") },
		},
		{
			name: "port absent -- falls to defaultPort", wantUsed: true,
			mutate: func(m map[string]string) { delete(m, "TEST_PORT") },
		},
		{
			// envOrDefault's established, codebase-wide convention: an
			// explicitly empty value is treated the same as absent, not as
			// "blank and therefore invalid" -- falls to defaultPort, same
			// as the "port absent" cell above.
			name: "port empty string -- same as absent, falls to defaultPort", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_PORT"] = "" },
		},
		{
			name: "port non-numeric -- refused by the round-trip check", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_PORT"] = "notaport" },
		},
		{
			name: "port with leading whitespace -- explicitly refused, not silently trimmed", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_PORT"] = " 5432" },
		},
		// Round-5 (2026-09-11) finding: envOrDefault's own presence check
		// trimmed for emptiness, so a whitespace-only PORT/DB was treated
		// as absent and silently fell to the default -- NEVER reaching the
		// whitespace-refusal guard above at all. rawOrDefault fixes this;
		// these cells pin it.
		{
			name: "port whitespace-only -- explicitly refused, not silently defaulted", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_PORT"] = "   " },
		},
		{
			name: "db name whitespace-only -- explicitly refused, not silently defaulted", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "   " },
		},
		{
			name: "user with trailing whitespace -- explicitly refused, not silently trimmed", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_USER"] = "app " },
		},
		{
			name: "db name with / -- refused explicitly (would inject a path segment)", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "app/db" },
		},
		{
			name: "db name with # -- refused explicitly (would inject a fragment)", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "app#db" },
		},
		// Round-4 (2026-09-11) findings (Trap #140): leading/trailing
		// whitespace on an identifier used to be silently trimmed, which
		// proved (against a real live PostgreSQL, in the reviewer's own
		// reproduction) to connect to a DIFFERENT existing database than
		// the one actually configured. It is now explicitly refused;
		// meaningful INNER whitespace (not at either edge) is preserved
		// exactly and connects correctly.
		{
			name: "db name with leading whitespace -- explicitly refused, not silently trimmed", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = " appdb" },
		},
		{
			name: "db name with trailing whitespace -- explicitly refused, not silently trimmed", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "appdb " },
		},
		{
			name: "db name with inner whitespace -- preserved exactly, not an edge case", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "app db" }, wantDB: "app db",
		},
		{
			name: "db name empty -- falls to defaultDB", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "" },
		},
		{
			name: "IPv6 host, bracketed by net.JoinHostPort", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_HOST"] = "::1" },
		},
		{
			name:     "host containing a path-injection attempt -- refused by the round-trip check",
			wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_HOST"] = "db.internal/evil" },
		},
		{
			name:     "host containing an authority-injection attempt -- refused by the round-trip check",
			wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_HOST"] = "db.internal@evil.example" },
		},
		{
			name:     "duplicate: host set twice via the same key (idempotent, last value wins by map semantics)",
			wantUsed: true,
			mutate:   func(m map[string]string) { m["TEST_HOST"] = "db.internal" },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := make(map[string]string, len(base))
			for k, v := range base {
				values[k] = v
			}
			if tc.mutate != nil {
				tc.mutate(values)
			}
			built, used, err := ResolveDSNFromComponents(lookup(values), spec)
			if used != tc.wantUsed {
				t.Fatalf("used = %v, want %v (err=%v)", used, tc.wantUsed, err)
			}
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got built=%q", built.Reveal())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.wantUsed {
				return
			}
			if !built.Configured() {
				t.Fatal("used=true but built value is not configured")
			}
			parsed, parseErr := url.Parse(built.Reveal())
			if parseErr != nil {
				t.Fatalf("assembled URI does not itself parse: %v (%q)", parseErr, built.Reveal())
			}
			if tc.wantHost != "" && parsed.Hostname() != tc.wantHost {
				t.Fatalf("hostname = %q, want %q", parsed.Hostname(), tc.wantHost)
			}
			if tc.wantDB != "" && parsed.Path != "/"+tc.wantDB {
				t.Fatalf("db path = %q, want %q -- the identifier must be preserved exactly", parsed.Path, "/"+tc.wantDB)
			}
			// The password (whatever reserved characters it contains) must
			// round-trip byte-for-byte through Password() -- this is the
			// actual acceptance criterion for CHAOS-5560, not merely "it
			// parses."
			wantPassword, havePassword := values["TEST_PASS"]
			gotPassword, gotHasPassword := parsed.User.Password()
			if havePassword && wantPassword != "" {
				if !gotHasPassword || gotPassword != wantPassword {
					t.Fatalf("password round-trip failed: got %q (present=%v), want %q",
						gotPassword, gotHasPassword, wantPassword)
				}
			}
		})
	}
}

// TestLoadPrefersComponentFormAndSurvivesAReservedCharacterPassword is the
// end-to-end proof: a raw '#' in a password breaks the pre-built-URI form
// (CHAOS-5560's whole motivation, reproduced first) but the SAME password
// through the component form loads cleanly and is byte-for-byte recoverable.
func TestLoadPrefersComponentFormAndSurvivesAReservedCharacterPassword(t *testing.T) {
	t.Parallel()

	reservedPassword := "s#cr#t@pass/word"

	// Red: reproduce the pre-built-URI failure this ticket exists to fix.
	brokenURI := "postgresql://app:" + reservedPassword + "@db.internal:5432/appdb"
	_, err := Load(workerSpec(map[string]string{
		"POSTGRES_URI":        brokenURI,
		"WORKER_DATABASE_URI": "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err == nil {
		t.Fatal("expected the pre-built URI form to reject an unescaped reserved character")
	}

	// Green: the same password through the component form.
	cfg, err := Load(workerSpec(map[string]string{
		"DEV_HEALTH_PG_DOMAIN_HOST":     "db.internal",
		"DEV_HEALTH_PG_DOMAIN_USER":     "app",
		"DEV_HEALTH_PG_DOMAIN_PASSWORD": reservedPassword,
		"DEV_HEALTH_PG_DB":              "appdb",
		"WORKER_DATABASE_URI":           "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err != nil {
		t.Fatalf("component form should accept the reserved-character password: %v", err)
	}
	parsed, err := url.Parse(cfg.DomainDatabaseURI.Reveal())
	if err != nil {
		t.Fatalf("assembled DomainDatabaseURI does not parse: %v", err)
	}
	got, ok := parsed.User.Password()
	if !ok || got != reservedPassword {
		t.Fatalf("password round-trip failed: got %q (present=%v), want %q", got, ok, reservedPassword)
	}
	if parsed.Hostname() != "db.internal" || parsed.Port() != "5432" || parsed.Path != "/appdb" {
		t.Fatalf("assembled URI has wrong host/port/path: %s", cfg.DomainDatabaseURI.Reveal())
	}

	// The pre-built form for a DIFFERENT connection (queue) is untouched --
	// component and pre-built forms coexist per-connection, never merged.
	if cfg.QueueDatabaseURI.Reveal() != "postgresql://app:app@db.internal:5432/appdb" {
		t.Fatalf("queue DSN should be unaffected by the domain connection's component override: %q",
			cfg.QueueDatabaseURI.Reveal())
	}
}

// TestURIAndComponentFormsAreMutuallyExclusivePerDSN is round-1's
// (2026-09-11) design ruling: NEITHER form may silently win over the other
// (both directions were tried and both left the losing form's value sitting
// in the environment with no way to tell a deliberate override from a stale
// leftover). Setting both a DSN's pre-built key (or its `_FILE` variant) and
// its component HOST var is refused outright, naming both keys in one
// message, checked before the raw key's own KEY/KEY_FILE rule.
func TestURIAndComponentFormsAreMutuallyExclusivePerDSN(t *testing.T) {
	t.Parallel()

	// URI + URI_FILE, no host var: the pre-existing KEY/KEY_FILE rule alone
	// is still enforced, unchanged.
	_, err := Load(workerSpec(map[string]string{
		"POSTGRES_URI":        "postgresql://old:old@old.invalid:5432/old",
		"POSTGRES_URI_FILE":   "/does/not/matter",
		"WORKER_DATABASE_URI": "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err == nil || !strings.Contains(err.Error(), "POSTGRES_URI and POSTGRES_URI_FILE are mutually exclusive") {
		t.Fatalf("expected the pre-existing KEY/KEY_FILE error when no host var is set, got: %v", err)
	}

	// URI + component HOST, both set: refused, naming both keys -- this is
	// round-1's exact reproduction (an operator set POSTGRES_DOMAIN_HOST to
	// move to components while a stale POSTGRES_URI/_FILE pair still sat in
	// the environment).
	_, err = Load(workerSpec(map[string]string{
		"POSTGRES_URI":                  "postgresql://old:old@old.invalid:5432/old",
		"DEV_HEALTH_PG_DOMAIN_HOST":     "db.internal",
		"DEV_HEALTH_PG_DOMAIN_USER":     "app",
		"DEV_HEALTH_PG_DOMAIN_PASSWORD": "app",
		"WORKER_DATABASE_URI":           "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err == nil ||
		!strings.Contains(err.Error(), "POSTGRES_URI") ||
		!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_HOST") ||
		!strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected a mutual-exclusivity error naming both POSTGRES_URI and DEV_HEALTH_PG_DOMAIN_HOST, got: %v", err)
	}

	// URI_FILE (not the direct key) + component HOST: also refused -- the
	// `_FILE` variant counts as "the raw form is present" too.
	_, err = Load(workerSpec(map[string]string{
		"POSTGRES_URI_FILE":         "/does/not/matter",
		"DEV_HEALTH_PG_DOMAIN_HOST": "db.internal",
		"DEV_HEALTH_PG_DOMAIN_USER": "app",
		"WORKER_DATABASE_URI":       "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err == nil || !strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_HOST") {
		t.Fatalf("expected POSTGRES_URI_FILE + a set HOST var to be refused too, got: %v", err)
	}

	// Components only: succeeds, exactly as the reserved-character-password
	// test above already proves end to end.
	cfg, err := Load(workerSpec(map[string]string{
		"DEV_HEALTH_PG_DOMAIN_HOST":     "db.internal",
		"DEV_HEALTH_PG_DOMAIN_USER":     "app",
		"DEV_HEALTH_PG_DOMAIN_PASSWORD": "app",
		"DEV_HEALTH_PG_DB":              "appdb",
		"WORKER_DATABASE_URI":           "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err != nil {
		t.Fatalf("components-only should succeed, got: %v", err)
	}
	if cfg.DomainDatabaseURI.Reveal() != "postgresql://app:app@db.internal:5432/appdb" {
		t.Fatalf("expected the component-built DSN, got %q", cfg.DomainDatabaseURI.Reveal())
	}

	// Neither URI nor HOST set for a required DSN: today's unchanged
	// "required" behavior (POSTGRES_URI has no default and Load's caller
	// reports it missing further up the stack; here just confirm no
	// mutual-exclusivity error is raised when neither form is present).
	cfg2, err := Load(workerSpec(map[string]string{
		"WORKER_DATABASE_URI": "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err != nil {
		t.Fatalf("neither form set should not itself be an error at Load() level, got: %v", err)
	}
	if cfg2.DomainDatabaseURI.Configured() {
		t.Fatalf("expected DomainDatabaseURI to be unconfigured when neither form is set, got %q", cfg2.DomainDatabaseURI.Reveal())
	}

	// Round-2 (2026-09-11) finding: the exclusivity check only inspected
	// HOST. A non-host component (here, USER) set alongside a raw URI was
	// silently ignored -- ResolveDSNFromComponents never activates without
	// HOST, so the raw URI won with no error and no indication the stray
	// USER var was sitting unused.
	_, err = Load(workerSpec(map[string]string{
		"POSTGRES_URI":              "postgresql://raw:raw@raw.invalid:5432/rawdb",
		"DEV_HEALTH_PG_DOMAIN_USER": "app",
		"WORKER_DATABASE_URI":       "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err == nil ||
		!strings.Contains(err.Error(), "POSTGRES_URI") ||
		!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_USER") ||
		!strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected a non-host component set alongside a raw URI to be refused, got: %v", err)
	}

	// Round-2 (2026-09-11) finding, second half: a non-host component set
	// with HOST absent AND no raw URI either used to silently report
	// "not configured" -- the same as if nothing had been set at all, with
	// no sign the operator had already started configuring components.
	_, err = Load(workerSpec(map[string]string{
		"DEV_HEALTH_PG_DOMAIN_USER": "app",
		"WORKER_DATABASE_URI":       "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err == nil ||
		!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_USER") ||
		!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_HOST") {
		t.Fatalf("expected a partial component set (no HOST, no raw URI) to name the missing HOST key, got: %v", err)
	}
}

// TestNonSharedDBKeyAndFileVariantsAreDetected is round 3's (2026-09-11) two
// findings, reproduced then fixed: (1) ClickHouse's DBKey is NOT shared the
// way the three Postgres specs' DBKey is, so it must still be swept by the
// exclusivity/missing-key detection; (2) USER_FILE/PASSWORD_FILE are real,
// supported activation paths (ResolveDSNFromComponents resolves both
// through secrets.Resolve) that the detection sweep must also cover.
func TestNonSharedDBKeyAndFileVariantsAreDetected(t *testing.T) {
	t.Parallel()

	t.Run("ClickHouse DB alone (no host, no raw) names the missing host key", func(t *testing.T) {
		t.Parallel()
		_, err := Load(workerSpec(map[string]string{
			"DEV_HEALTH_CH_DB":    "analytics",
			"WORKER_DATABASE_URI": "postgresql://app:app@db.internal:5432/appdb",
		}))
		if err == nil ||
			!strings.Contains(err.Error(), "DEV_HEALTH_CH_DB") ||
			!strings.Contains(err.Error(), "DEV_HEALTH_CH_HOST") {
			t.Fatalf("expected the missing-host error naming DEV_HEALTH_CH_DB, got: %v", err)
		}
	})

	t.Run("ClickHouse DB plus a raw CLICKHOUSE_URI is refused", func(t *testing.T) {
		t.Parallel()
		_, err := Load(workerSpec(map[string]string{
			"CLICKHOUSE_URI":      "clickhouse://old:old@old.invalid:9000/old",
			"DEV_HEALTH_CH_DB":    "analytics",
			"WORKER_DATABASE_URI": "postgresql://app:app@db.internal:5432/appdb",
		}))
		if err == nil ||
			!strings.Contains(err.Error(), "CLICKHOUSE_URI") ||
			!strings.Contains(err.Error(), "DEV_HEALTH_CH_DB") ||
			!strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("expected a mutual-exclusivity error naming DEV_HEALTH_CH_DB, got: %v", err)
		}
	})

	t.Run("PASSWORD_FILE alone (no host, no raw) names the missing host key", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		passwordFile := dir + "/password"
		if err := os.WriteFile(passwordFile, []byte("s3cret\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(workerSpec(map[string]string{
			"DEV_HEALTH_PG_DOMAIN_PASSWORD_FILE": passwordFile,
			"WORKER_DATABASE_URI":                "postgresql://app:app@db.internal:5432/appdb",
		}))
		if err == nil ||
			!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_PASSWORD_FILE") ||
			!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_HOST") {
			t.Fatalf("expected the missing-host error naming DEV_HEALTH_PG_DOMAIN_PASSWORD_FILE, got: %v", err)
		}
	})

	t.Run("USER_FILE plus a raw POSTGRES_URI is refused", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		userFile := dir + "/user"
		if err := os.WriteFile(userFile, []byte("app\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(workerSpec(map[string]string{
			"POSTGRES_URI":                   "postgresql://old:old@old.invalid:5432/old",
			"DEV_HEALTH_PG_DOMAIN_USER_FILE": userFile,
			"WORKER_DATABASE_URI":            "postgresql://app:app@db.internal:5432/appdb",
		}))
		if err == nil ||
			!strings.Contains(err.Error(), "POSTGRES_URI") ||
			!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_USER_FILE") ||
			!strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("expected a mutual-exclusivity error naming DEV_HEALTH_PG_DOMAIN_USER_FILE, got: %v", err)
		}
	})
}

// TestWhitespaceOnlyPasswordIsDetectedAndAuthenticates is round 4's
// (2026-09-11) third finding, reproduced then fixed (Trap #140: never
// TrimSpace a secret): presence detection used to TrimSpace a password
// before checking it for emptiness, while secrets.Resolve (which actually
// builds the connection) deliberately never trims a secret's meaningful
// content -- so a password consisting only of whitespace was Configured()
// to secrets.Resolve (and DID authenticate -- the reviewer proved this
// against a real live PostgreSQL) but invisible to the exclusivity check,
// letting it silently coexist with a raw URI unflagged.
func TestWhitespaceOnlyPasswordIsDetectedAndAuthenticates(t *testing.T) {
	t.Parallel()

	// Presence: the round-4 defect, reproduced then fixed. A whitespace-
	// only password alongside a raw URI must be refused, not silently
	// ignored.
	_, err := Load(workerSpec(map[string]string{
		"POSTGRES_URI":                  "postgresql://old:old@old.invalid:5432/old",
		"DEV_HEALTH_PG_DOMAIN_HOST":     "db.internal",
		"DEV_HEALTH_PG_DOMAIN_USER":     "app",
		"DEV_HEALTH_PG_DOMAIN_PASSWORD": "   ",
		"WORKER_DATABASE_URI":           "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err == nil ||
		!strings.Contains(err.Error(), "POSTGRES_URI") ||
		!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_PASSWORD") ||
		!strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected a mutual-exclusivity error naming DEV_HEALTH_PG_DOMAIN_PASSWORD, got: %v", err)
	}

	// The whitespace-only password is still preserved exactly (never
	// trimmed) when the component form is used on its own -- byte-for-byte
	// round-trip through url.User.Password(), the same acceptance
	// criterion CHAOS-5560 uses for every other reserved-character case.
	cfg, err := Load(workerSpec(map[string]string{
		"DEV_HEALTH_PG_DOMAIN_HOST":     "db.internal",
		"DEV_HEALTH_PG_DOMAIN_USER":     "app",
		"DEV_HEALTH_PG_DOMAIN_PASSWORD": "   ",
		"WORKER_DATABASE_URI":           "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err != nil {
		t.Fatalf("component form with a whitespace-only password should succeed, got: %v", err)
	}
	parsed, parseErr := url.Parse(cfg.DomainDatabaseURI.Reveal())
	if parseErr != nil {
		t.Fatalf("assembled URI does not parse: %v", parseErr)
	}
	got, ok := parsed.User.Password()
	if !ok || got != "   " {
		t.Fatalf("whitespace-only password round-trip failed: got %q (present=%v), want %q", got, ok, "   ")
	}
}

// TestWhitespaceOnlyFileSourcedCredentialsAreDetected is round 6's explicit
// cell for round 4/5's whitespace-detection fix: a whitespace-only
// PASSWORD or USER delivered via its `_FILE` form (a mounted secret file
// containing only spaces, no trailing newline) must be detected exactly
// the same way as one supplied directly -- secrets.Resolve applies the
// identical Configured() predicate regardless of source.
func TestWhitespaceOnlyFileSourcedCredentialsAreDetected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	passwordFile := dir + "/password"
	if err := os.WriteFile(passwordFile, []byte("   "), 0o600); err != nil {
		t.Fatal(err)
	}
	userFile := dir + "/user"
	if err := os.WriteFile(userFile, []byte("   "), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("whitespace-only PASSWORD_FILE alongside a raw URI is refused", func(t *testing.T) {
		t.Parallel()
		_, err := Load(workerSpec(map[string]string{
			"POSTGRES_URI":                       "postgresql://old:old@old.invalid:5432/old",
			"DEV_HEALTH_PG_DOMAIN_HOST":          "db.internal",
			"DEV_HEALTH_PG_DOMAIN_USER":          "app",
			"DEV_HEALTH_PG_DOMAIN_PASSWORD_FILE": passwordFile,
			"WORKER_DATABASE_URI":                "postgresql://app:app@db.internal:5432/appdb",
		}))
		if err == nil ||
			!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_PASSWORD_FILE") ||
			!strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("expected a mutual-exclusivity error naming DEV_HEALTH_PG_DOMAIN_PASSWORD_FILE, got: %v", err)
		}
	})

	t.Run("whitespace-only PASSWORD_FILE preserved exactly via the component form", func(t *testing.T) {
		t.Parallel()
		cfg, err := Load(workerSpec(map[string]string{
			"DEV_HEALTH_PG_DOMAIN_HOST":          "db.internal",
			"DEV_HEALTH_PG_DOMAIN_USER":          "app",
			"DEV_HEALTH_PG_DOMAIN_PASSWORD_FILE": passwordFile,
			"WORKER_DATABASE_URI":                "postgresql://app:app@db.internal:5432/appdb",
		}))
		if err != nil {
			t.Fatalf("expected success, got: %v", err)
		}
		parsed, parseErr := url.Parse(cfg.DomainDatabaseURI.Reveal())
		if parseErr != nil {
			t.Fatalf("assembled URI does not parse: %v", parseErr)
		}
		got, ok := parsed.User.Password()
		if !ok || got != "   " {
			t.Fatalf("whitespace-only PASSWORD_FILE round-trip failed: got %q (present=%v)", got, ok)
		}
	})

	t.Run("whitespace-only USER_FILE alongside a raw URI is refused", func(t *testing.T) {
		t.Parallel()
		_, err := Load(workerSpec(map[string]string{
			"POSTGRES_URI":                   "postgresql://old:old@old.invalid:5432/old",
			"DEV_HEALTH_PG_DOMAIN_HOST":      "db.internal",
			"DEV_HEALTH_PG_DOMAIN_USER_FILE": userFile,
			"WORKER_DATABASE_URI":            "postgresql://app:app@db.internal:5432/appdb",
		}))
		if err == nil ||
			!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_USER_FILE") ||
			!strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("expected a mutual-exclusivity error naming DEV_HEALTH_PG_DOMAIN_USER_FILE, got: %v", err)
		}
	})
}

// TestFileReadFailureNeverEchoesTheConfiguredPath is round 4's (2026-09-11)
// first finding, reproduced then fixed at the root
// (internal/platform/secrets/source.go): secrets.Resolve's KEY_FILE
// read-failure error used to wrap the underlying os.PathError verbatim,
// and Go's os.PathError.Error() embeds the exact path it tried to open --
// an operator who misconfigures KEY_FILE to a raw credential string (a
// full DSN, say) instead of an actual path had that entire string,
// password included, echoed back by any caller that printed the error.
func TestFileReadFailureNeverEchoesTheConfiguredPath(t *testing.T) {
	t.Parallel()

	syntheticSecret := "s3cr3t-p@ssw0rd-should-never-appear"
	misconfiguredPath := "postgresql://svc:" + syntheticSecret + "@internal.example:5432/db"

	_, err := Load(workerSpec(map[string]string{
		"DEV_HEALTH_PG_DOMAIN_HOST":      "db.internal",
		"DEV_HEALTH_PG_DOMAIN_USER_FILE": misconfiguredPath,
		"WORKER_DATABASE_URI":            "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err == nil {
		t.Fatal("expected a file-read failure")
	}
	if strings.Contains(err.Error(), syntheticSecret) {
		t.Fatalf("the configured (credential-bearing) path leaked into the error, got: %v", err)
	}
	if strings.Contains(err.Error(), misconfiguredPath) {
		t.Fatalf("the entire misconfigured path leaked into the error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_USER_FILE") {
		t.Fatalf("expected the error to name the key, got: %v", err)
	}
}
