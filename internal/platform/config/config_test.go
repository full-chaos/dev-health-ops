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
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/jackc/pgx/v5/pgconn"
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

// TestSafeAttrsReportFormAndNameOfEachResolvedDSN pins the observability
// requirement: a successful resolution must not leave only
// "*_database_configured=true" observable -- which of the
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
	// The pre-built-URI form NEVER has a Name -- telemetry never parses
	// a URI -- so domain_database_name must be entirely absent, not an
	// empty string.
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

// TestComponentDatabaseIdentityReportsWhatWillBeConnectedTo pins the rule
// that the database this package publishes at Info is the one the DRIVER
// will actually open, not the string the operator requested. The two can
// only diverge through a lossy assembly, which the resolver refuses --
// reporting the parsed value anyway means a regression in that refusal
// surfaces in telemetry instead of hiding behind it. A pre-built URI is
// still never parsed for this purpose; those callers report form="uri"
// with no name at all.
func TestComponentDatabaseIdentityReportsWhatWillBeConnectedTo(t *testing.T) {
	t.Parallel()

	t.Run("reports the identifier the driver parses back", func(t *testing.T) {
		t.Parallel()
		built, used, err := ResolveDSNFromComponents(lookup(map[string]string{
			"DEV_HEALTH_PG_DOMAIN_HOST": "db.internal",
			"DEV_HEALTH_PG_DB":          "app#db",
		}), DomainDatabaseSpec)
		if !used || err != nil {
			t.Fatalf("used=%v err=%v", used, err)
		}
		if got := ComponentDatabaseIdentity(DomainDatabaseSpec.Scheme, built); got != "app#db" {
			t.Fatalf("got %q, want %q", got, "app#db")
		}
	})

	t.Run("reports what the driver parses, not what was asked for", func(t *testing.T) {
		t.Parallel()
		// The resolver refuses this shape, so it cannot arrive through
		// Load. Handed the DSN directly, telemetry must still report the
		// database the driver will OPEN ("appdb"), never the "/appdb"
		// that was asked for -- otherwise a regression in that refusal
		// would hide behind an honest-looking log line.
		lossy := assembleWithNetURL("postgresql", "db.internal", "5432", "app", "pw", "/appdb")
		if got := ComponentDatabaseIdentity("postgresql", secrets.NewValue(lossy)); got != "appdb" {
			t.Fatalf("got %q, want the parsed identity %q", got, "appdb")
		}
	})

	t.Run("reports nothing for an unconfigured value", func(t *testing.T) {
		t.Parallel()
		if got := ComponentDatabaseIdentity(DomainDatabaseSpec.Scheme, secrets.Value{}); got != "" {
			t.Fatalf("got %q, want empty", got)
		}
	})

	t.Run("reports nothing rather than guessing when the driver cannot parse", func(t *testing.T) {
		t.Parallel()
		if got := ComponentDatabaseIdentity("postgresql", secrets.NewValue("postgresql://u:p@[[::1]]:5432/db")); got != "" {
			t.Fatalf("got %q, want empty for a DSN the driver rejects", got)
		}
	})

	t.Run("never derived from an unrelated raw URI in the same environment", func(t *testing.T) {
		t.Parallel()
		built, used, err := ResolveDSNFromComponents(lookup(map[string]string{
			"DEV_HEALTH_PG_DOMAIN_HOST": "db.internal",
			"DEV_HEALTH_PG_DB":          "realdb",
			"POSTGRES_URI":              "postgresql://user:pass@word/extra@host:5432/decoydb",
		}), DomainDatabaseSpec)
		if !used || err != nil {
			t.Fatalf("used=%v err=%v", used, err)
		}
		if got := ComponentDatabaseIdentity(DomainDatabaseSpec.Scheme, built); got != "realdb" {
			t.Fatalf("got %q, want %q -- must never be influenced by an unrelated raw URI", got, "realdb")
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

// TestComponentKeysMatchesSpecFieldsMinusTheSharedDBException pins the
// structural requirement directly: componentKeys() must be derived from
// ComponentSpec's own fields, never a hand-picked slice that could drift
// from them (silently omitting DBKey for a spec, or a field's `_FILE`
// variant entirely).
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

// componentDrivers are the two real driver/spec pairs every acceptance
// cell below is executed against.
var componentDrivers = []struct {
	name string
	spec ComponentSpec
}{
	{"PostgreSQL", DomainDatabaseSpec},
	{"ClickHouse", ClickHouseSpec},
}

// assembleWithNetURL builds a DSN the way the resolver is required to
// build one -- stdlib net/url and nothing else. It is deliberately a
// second, independent implementation: an accepted cell asserts the
// resolver's output is byte-identical to this, so any hand-written
// encoder, escape or grammar reintroduced into the resolver shows up as a
// byte difference rather than having to be spotted by eye.
func assembleWithNetURL(scheme, host, port, user, password, db string) string {
	target := &url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, port),
		Path:   "/" + db,
	}
	if user != "" {
		target.User = url.UserPassword(user, password)
	}
	return target.String()
}

// driverView is what the REAL driver makes of a DSN: the identity it will
// connect with, and how many endpoints it resolved to. pgconn always
// carries at least one Fallbacks entry for a single-host DSN (its own
// sslmode negotiation retry against the same endpoint), so only a
// fallback naming a different host or port counts as another endpoint.
type driverView struct {
	host, port, user, password, database string
	endpoints                            int
	err                                  error
}

// usable reports the oracle's verdict for a component set: the driver
// accepted the DSN, resolved it to exactly one endpoint, AND reads every
// field back as the value that was configured. Acceptance alone is not
// the contract -- a database named "/app" is accepted and then parsed as
// "app", which is a connection to a different database that nothing
// reports.
func (v driverView) usable(host, port, user, password, database string) bool {
	return v.err == nil && v.endpoints == 1 &&
		v.host == host && v.port == port &&
		v.user == user && v.password == password && v.database == database
}

func driverParse(t *testing.T, dsn string) driverView {
	t.Helper()
	if strings.HasPrefix(dsn, "clickhouse://") {
		options, err := clickhouse.ParseDSN(dsn)
		if err != nil {
			return driverView{err: err}
		}
		view := driverView{
			user:      options.Auth.Username,
			password:  options.Auth.Password,
			database:  options.Auth.Database,
			endpoints: len(options.Addr),
		}
		if view.endpoints == 1 {
			host, port, splitErr := net.SplitHostPort(options.Addr[0])
			if splitErr != nil {
				return driverView{err: splitErr}
			}
			view.host, view.port = host, port
		}
		return view
	}
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return driverView{err: err}
	}
	view := driverView{
		host:      cfg.Host,
		port:      strconv.FormatUint(uint64(cfg.Port), 10),
		user:      cfg.User,
		password:  cfg.Password,
		database:  cfg.Database,
		endpoints: 1,
	}
	for _, fallback := range cfg.Fallbacks {
		if fallback.Host != cfg.Host || fallback.Port != cfg.Port {
			view.endpoints++
		}
	}
	return view
}

func rawOrFallback(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

// TestComponentAcceptanceIsExactlyTheDriversOwnParser executes the whole
// acceptance contract as one property, one cell at a time: a component
// set is accepted if and only if the driver it feeds accepts the DSN
// assembled from it AND resolves that DSN to exactly one endpoint.
//
// No cell carries a verdict of its own. Each is compared against the
// driver's live answer for the same inputs, so the table cannot drift
// from the parser it stands in front of, and any character rule
// reintroduced into the resolver breaks the equivalence on the first
// value the driver accepts and the rule does not.
//
// The host values below are every shape a hand-written grammar had
// refused: a multi-host list, a leading and a trailing comma, a space, a
// bare port stuffed into HOST, an absolute hostname's trailing dot, empty
// and doubled labels, a lone dot, a percent, an at, a slash, a hash, a
// question mark, an underscore, a non-ASCII label, a name past 253 bytes,
// an operator-bracketed IPv6 and IPv4 literal, whitespace-only, and a
// leading space.
func TestComponentAcceptanceIsExactlyTheDriversOwnParser(t *testing.T) {
	t.Parallel()

	const (
		user     = "app"
		password = "s3cr3t#pw"
		database = "appdb"
	)
	hosts := []string{
		"db.internal",
		"127.0.0.1",
		"::1",
		"[::1]",
		"::ffff:192.0.2.1",
		"[::ffff:192.0.2.1]",
		"[127.0.0.1]",
		",127.0.0.1",
		"127.0.0.1,",
		"127.0.0.1,evil.invalid",
		"127.0.0.1 evil.invalid",
		"127.0.0.1:5432",
		"db.internal.",
		".db.internal",
		"db.internal..",
		"db..internal",
		".",
		"db%2einternal",
		"user@db.internal",
		"db.internal/x",
		"db.internal#x",
		"db.internal?x",
		"db_internal",
		"db\u00e9.internal",
		strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." +
			strings.Repeat("c", 63) + "." + strings.Repeat("d", 62),
		"   ",
		" db.internal",
	}

	for _, driver := range componentDrivers {
		for _, host := range hosts {
			t.Run(driver.name+"/"+host, func(t *testing.T) {
				t.Parallel()
				spec := driver.spec
				built, used, err := ResolveDSNFromComponents(lookup(map[string]string{
					spec.HostKey:     host,
					spec.UserKey:     user,
					spec.PasswordKey: password,
					spec.DBKey:       database,
				}), spec)
				if !used {
					t.Fatal("expected used=true once HOST is set")
				}

				oracle := assembleWithNetURL(spec.Scheme, host, spec.DefaultPort, user, password, database)
				view := driverParse(t, oracle)
				usable := view.usable(host, spec.DefaultPort, user, password, database)

				switch {
				case usable && err != nil:
					t.Fatalf("the %s driver reads host %q back unchanged as exactly one endpoint, but the resolver refused it: %v",
						spec.Scheme, host, err)
				case !usable && err == nil:
					t.Fatalf("the %s driver does not use host %q as configured (endpoints=%d host=%q err=%v), but the resolver accepted it",
						spec.Scheme, host, view.endpoints, view.host, view.err)
				}

				if err != nil {
					// A refusal names the setting responsible and carries
					// neither the credential nor its encoded form. Where
					// the driver rejected the whole DSN the message names
					// every setting that fed it; where one component did
					// not survive the round trip it names just that one,
					// which is the more useful answer and is why this
					// asserts the relevant key rather than all of them.
					if !strings.Contains(err.Error(), spec.HostKey) {
						t.Fatalf("refusal for host %q does not name %s: %v", host, spec.HostKey, err)
					}
					for _, leak := range []string{password, url.QueryEscape(password), url.PathEscape(password)} {
						if strings.Contains(err.Error(), leak) {
							t.Fatalf("refusal for host %q leaked the credential (%q): %v", host, leak, err)
						}
					}
					return
				}
				if built.Reveal() != oracle {
					t.Fatalf("host %q: resolver built %q, want the pure net/url assembly %q",
						host, built.Reveal(), oracle)
				}
			})
		}
	}
}

// TestComponentPortAcceptanceIsExactlyTheDriversOwnParser is the same
// property for PORT, which likewise has no grammar of its own any more:
// the two drivers genuinely disagree about a port outside 1-65535, and
// each connection is held to its own driver's answer rather than to a
// range check that would have to pick one of them.
func TestComponentPortAcceptanceIsExactlyTheDriversOwnParser(t *testing.T) {
	t.Parallel()

	const (
		host     = "db.internal"
		user     = "app"
		password = "s3cr3t#pw"
		database = "appdb"
	)
	ports := []string{"5432", "1", "65535", "0", "65536", "99999", "5432,5433", "5432x", "+5432", "", " 5432", "5432 ", "   ", "0443", "05432"}

	for _, driver := range componentDrivers {
		for _, port := range ports {
			t.Run(driver.name+"/"+port, func(t *testing.T) {
				t.Parallel()
				spec := driver.spec
				built, used, err := ResolveDSNFromComponents(lookup(map[string]string{
					spec.HostKey:     host,
					spec.PortKey:     port,
					spec.UserKey:     user,
					spec.PasswordKey: password,
					spec.DBKey:       database,
				}), spec)
				if !used {
					t.Fatal("expected used=true once HOST is set")
				}

				// An empty PORT is absent, not blank: it falls to the
				// spec default, exactly as the resolver's own
				// rawOrDefault does.
				effective := rawOrFallback(port, spec.DefaultPort)
				oracle := assembleWithNetURL(spec.Scheme, host, effective, user, password, database)
				view := driverParse(t, oracle)
				usable := view.usable(host, effective, user, password, database)

				switch {
				case usable && err != nil:
					t.Fatalf("the %s driver reads port %q back unchanged, but the resolver refused it: %v", spec.Scheme, port, err)
				case !usable && err == nil:
					t.Fatalf("the %s driver does not use port %q as configured (endpoints=%d port=%q err=%v), but the resolver accepted it",
						spec.Scheme, port, view.endpoints, view.port, view.err)
				}
				if err != nil {
					if !strings.Contains(err.Error(), spec.PortKey) {
						t.Fatalf("refusal for port %q does not name %s: %v", port, spec.PortKey, err)
					}
					return
				}
				if built.Reveal() != oracle {
					t.Fatalf("port %q: resolver built %q, want the pure net/url assembly %q", port, built.Reveal(), oracle)
				}
			})
		}
	}
}

// TestABrokenDriverEnvironmentIsReportedWithoutExcusingTheComponents pins
// what a failed control parse does and does not prove. pgconn reads the
// ambient PG* environment while parsing, so a broken PGSSLROOTCERT or
// PGCONNECT_TIMEOUT makes it refuse a DSN assembled from perfectly good
// components; an operator told only about the component settings goes to
// the wrong file, which is why the environment is named at all.
//
// But a failed control says only that the environment is broken. It says
// nothing about the components, so it must never excuse them: with a bad
// timeout and a bad port set together, an earlier wording named the port
// in its own error text and declared it innocent in the same sentence.
func TestABrokenDriverEnvironmentIsReportedWithoutExcusingTheComponents(t *testing.T) {
	const password = "s3cr3t#pw"
	good := map[string]string{
		"DEV_HEALTH_PG_DOMAIN_HOST":     "db.internal",
		"DEV_HEALTH_PG_DOMAIN_USER":     "app",
		"DEV_HEALTH_PG_DOMAIN_PASSWORD": password,
		"DEV_HEALTH_PG_DB":              "appdb",
	}
	badPort := map[string]string{}
	for k, v := range good {
		badPort[k] = v
	}
	badPort["DEV_HEALTH_PG_DOMAIN_PORT"] = "bad-port"

	// Green first: with nothing ambient set, the good set resolves and the
	// bad port is refused naming the port and nothing else.
	if _, _, err := ResolveDSNFromComponents(lookup(good), DomainDatabaseSpec); err != nil {
		t.Fatalf("control: expected the good component set to resolve, got: %v", err)
	}
	_, _, err := ResolveDSNFromComponents(lookup(badPort), DomainDatabaseSpec)
	if err == nil || !strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_PORT") {
		t.Fatalf("expected a plain component refusal naming the port, got: %v", err)
	}
	if strings.Contains(err.Error(), "environment") {
		t.Fatalf("a healthy environment must not be mentioned at all: %v", err)
	}

	for _, ambient := range []struct{ key, value string }{
		{"PGSSLROOTCERT", "/nonexistent/ca.pem"},
		{"PGCONNECT_TIMEOUT", "notanumber"},
	} {
		for _, components := range []struct {
			name     string
			env      map[string]string
			mustName string
		}{
			{"components are fine", good, "DEV_HEALTH_PG_DOMAIN_HOST"},
			{"components are also broken", badPort, "DEV_HEALTH_PG_DOMAIN_PORT"},
		} {
			t.Run(ambient.key+"/"+components.name, func(t *testing.T) {
				t.Setenv(ambient.key, ambient.value)

				_, used, err := ResolveDSNFromComponents(lookup(components.env), DomainDatabaseSpec)
				if !used {
					t.Fatal("expected used=true once HOST is set")
				}
				if err == nil {
					t.Fatalf("expected a refusal while %s is broken", ambient.key)
				}
				if !strings.Contains(err.Error(), "the environment is broken too") {
					t.Fatalf("the refusal does not report the broken environment: %v", err)
				}
				if strings.Contains(err.Error(), "not the cause") {
					t.Fatalf("a failed control must never excuse the component settings: %v", err)
				}
				if !strings.Contains(err.Error(), components.mustName) {
					t.Fatalf("the refusal does not name %s: %v", components.mustName, err)
				}
				for _, leak := range []string{password, url.QueryEscape(password)} {
					if strings.Contains(err.Error(), leak) {
						t.Fatalf("the refusal leaked the credential: %v", err)
					}
				}
			})
		}
	}
}

// TestAnUnregisteredSchemeIsRefusedLoudly pins the one branch no spec in
// this package reaches: ComponentSpec is exported, so a caller can name a
// scheme with no driver parser behind it, and silently skipping validation
// for it would be the worst possible answer.
func TestAnUnregisteredSchemeIsRefusedLoudly(t *testing.T) {
	t.Parallel()

	spec := ComponentSpec{
		HostKey: "OTHER_HOST", PortKey: "OTHER_PORT", DefaultPort: "1",
		UserKey: "OTHER_USER", PasswordKey: "OTHER_PASSWORD",
		DBKey: "OTHER_DB", DefaultDB: "other", Scheme: "mysql",
	}
	_, used, err := ResolveDSNFromComponents(lookup(map[string]string{"OTHER_HOST": "db.internal"}), spec)
	if !used {
		t.Fatal("expected used=true once HOST is set")
	}
	if err == nil || !strings.Contains(err.Error(), "no driver parser is registered") {
		t.Fatalf("expected a loud refusal for an unsupported scheme, got: %v", err)
	}
	if !strings.Contains(err.Error(), "OTHER_HOST") {
		t.Fatalf("the refusal does not name the setting that activated the form: %v", err)
	}
}

// reservedCharacters and componentPositions are the axis a
// character-only table misses. A reserved character behaves differently
// depending on WHERE in the value it sits: a database named "/app"
// assembles into a URL path of "//app", which pgconn reads back as "app"
// -- accepted, one endpoint, and a live connection to a different
// existing database. The same slash in the middle of the name survives
// untouched, which is exactly why a table that only probed the middle
// position reported the component form as safe.
var (
	reservedCharacters = []struct{ name, char string }{
		{"slash", "/"}, {"hash", "#"}, {"question", "?"}, {"space", " "},
		{"percent", "%"}, {"at", "@"}, {"colon", ":"}, {"dot", "."},
		{"backslash", `\`}, {"ampersand", "&"}, {"equals", "="}, {"plus", "+"},
	}
	componentPositions = []struct{ name, format string }{
		{"leading", "%sbase"}, {"middle", "ba%sse"}, {"trailing", "base%s"}, {"only", "%s"},
	}
)

// TestEveryComponentSurvivesAssemblyOrIsRefused is the whole acceptance
// contract on the axis that matters: character x POSITION x component x
// driver, with every verdict derived from the driver's own answer rather
// than written down. A component set is accepted if and only if the
// driver reads every field back as the value that was configured; a value
// the assembly cannot carry losslessly is refused naming that setting,
// never accepted and quietly changed.
func TestEveryComponentSurvivesAssemblyOrIsRefused(t *testing.T) {
	t.Parallel()

	for _, driver := range componentDrivers {
		for _, component := range []string{"user", "password", "database"} {
			for _, char := range reservedCharacters {
				for _, position := range componentPositions {
					t.Run(driver.name+"/"+component+"/"+char.name+"/"+position.name, func(t *testing.T) {
						t.Parallel()
						spec := driver.spec
						value := fmt.Sprintf(position.format, char.char)

						host, port := "db.internal", spec.DefaultPort
						user, password, database := "app", "pw", "appdb"
						switch component {
						case "user":
							user = value
						case "password":
							password = value
						case "database":
							database = value
						}

						built, used, err := ResolveDSNFromComponents(lookup(map[string]string{
							spec.HostKey:     host,
							spec.PortKey:     port,
							spec.UserKey:     user,
							spec.PasswordKey: password,
							spec.DBKey:       database,
						}), spec)
						if !used {
							t.Fatal("expected used=true once HOST is set")
						}

						view := driverParse(t, assembleWithNetURL(spec.Scheme, host, port, user, password, database))
						usable := view.usable(host, port, user, password, database)

						switch {
						case usable && err != nil:
							t.Fatalf("%s=%q survives the round trip through the %s driver, but the resolver refused it: %v",
								component, value, spec.Scheme, err)
						case !usable && err == nil:
							t.Fatalf("%s=%q does NOT survive the round trip (the %s driver reads user=%q password-match=%v database=%q), but the resolver accepted it as %q",
								component, value, spec.Scheme, view.user, view.password == password, view.database, built.Reveal())
						}
						if err != nil {
							key := map[string]string{
								"user":     spec.UserKey,
								"password": spec.PasswordKey,
								"database": spec.DBKey,
							}[component]
							if !strings.Contains(err.Error(), key) {
								t.Fatalf("the refusal does not name %s, the setting responsible: %v", key, err)
							}
							if strings.Contains(err.Error(), password) {
								t.Fatalf("the refusal leaked the credential: %v", err)
							}
							return
						}
						// Accepted: the telemetry identity must be the
						// parsed one, which is now necessarily the
						// configured one.
						if got := ComponentDatabaseIdentity(spec.Scheme, built); got != database {
							t.Fatalf("telemetry reports database %q, but the driver will connect to %q", got, database)
						}
					})
				}
			}
		}
	}
}

// TestALeadingSlashDatabaseIsRefusedRatherThanSilentlyRewritten is the
// finding itself, pinned on its own so a regression names it. pgconn
// reads "//app" as the database "app": accepted, one endpoint, and a
// successful connection to a DIFFERENT existing database while telemetry
// reported the requested name.
func TestALeadingSlashDatabaseIsRefusedRatherThanSilentlyRewritten(t *testing.T) {
	t.Parallel()

	for _, database := range []string{"/appdb", "/"} {
		t.Run(database, func(t *testing.T) {
			t.Parallel()

			// Red: the driver accepts the assembled DSN and resolves it
			// to one endpoint. Nothing about acceptance catches this.
			assembled := assembleWithNetURL("postgresql", "db.internal", "5432", "app", "pw", database)
			view := driverParse(t, assembled)
			if view.err != nil || view.endpoints != 1 {
				t.Fatalf("test setup: expected the driver to accept %q as one endpoint, got err=%v endpoints=%d",
					assembled, view.err, view.endpoints)
			}
			if view.database == database {
				t.Fatalf("test setup: expected pgconn to rewrite %q, got %q", database, view.database)
			}

			_, used, err := ResolveDSNFromComponents(lookup(map[string]string{
				"DEV_HEALTH_PG_DOMAIN_HOST": "db.internal",
				"DEV_HEALTH_PG_DB":          database,
			}), DomainDatabaseSpec)
			if !used {
				t.Fatal("expected used=true once HOST is set")
			}
			if err == nil {
				t.Fatalf("database %q is rewritten to %q by the driver and must be refused, not accepted", database, view.database)
			}
			if !strings.Contains(err.Error(), "DEV_HEALTH_PG_DB") {
				t.Fatalf("the refusal does not name DEV_HEALTH_PG_DB: %v", err)
			}
		})
	}
}

// TestMultiHostComponentsAreRefusedOnTheParsedConfigNotTheString pins
// WHERE the single-endpoint rule lives. A comma-bearing host is not a
// malformed DSN: both drivers parse it without complaint, and only the
// parsed result -- pgconn's Fallbacks, clickhouse-go's Addr -- shows the
// second endpoint. A check on the raw string or on net/url's own
// round-trip sees nothing, which is how a second host reached a live
// driver before.
func TestMultiHostComponentsAreRefusedOnTheParsedConfigNotTheString(t *testing.T) {
	t.Parallel()

	for _, driver := range componentDrivers {
		t.Run(driver.name, func(t *testing.T) {
			t.Parallel()
			spec := driver.spec
			const multi = "db.internal,evil.invalid"

			// Red first: the assembled DSN parses cleanly. Nothing about
			// the string itself is wrong.
			assembled := assembleWithNetURL(spec.Scheme, multi, spec.DefaultPort, "app", "app", "appdb")
			if _, parseErr := url.Parse(assembled); parseErr != nil {
				t.Fatalf("test setup: the assembled DSN does not parse: %v", parseErr)
			}
			view := driverParse(t, assembled)
			if view.err != nil {
				t.Fatalf("test setup: the %s driver rejected the DSN outright (%v) -- this cell must prove the PARSED config is what catches it", spec.Scheme, view.err)
			}
			if view.endpoints < 2 {
				t.Fatalf("test setup: the %s driver resolved %d endpoint(s), want at least 2", spec.Scheme, view.endpoints)
			}

			_, used, err := ResolveDSNFromComponents(lookup(map[string]string{
				spec.HostKey:     multi,
				spec.UserKey:     "app",
				spec.PasswordKey: "app",
				spec.DBKey:       "appdb",
			}), spec)
			if !used {
				t.Fatal("expected used=true once HOST is set")
			}
			if err == nil || !strings.Contains(err.Error(), "more than one endpoint") {
				t.Fatalf("expected a single-endpoint refusal naming the component settings, got: %v", err)
			}
			if !strings.Contains(err.Error(), spec.HostKey) {
				t.Fatalf("refusal does not name %s: %v", spec.HostKey, err)
			}
		})
	}
}

// TestAbsoluteHostnameTrailingDotIsPreservedVerbatim proves the trailing
// dot reaches the real driver exactly as configured, never stripped --
// not merely that ResolveDSNFromComponents no longer errors on it. Host
// identifiers are never altered
// anywhere in this resolver (the same discipline as every
// other identifier field).
func TestAbsoluteHostnameTrailingDotIsPreservedVerbatim(t *testing.T) {
	t.Parallel()

	value, used, err := ResolveDSNFromComponents(lookup(map[string]string{
		"DEV_HEALTH_PG_DOMAIN_HOST": "db.internal.",
	}), DomainDatabaseSpec)
	if !used || err != nil {
		t.Fatalf("used=%v err=%v", used, err)
	}
	cfg, parseErr := pgconn.ParseConfig(value.Reveal())
	if parseErr != nil {
		t.Fatalf("real pgx driver could not parse the assembled DSN: %v", parseErr)
	}
	if cfg.Host != "db.internal." {
		t.Fatalf("trailing dot was not preserved verbatim: driver saw host=%q, want %q", cfg.Host, "db.internal.")
	}
}

// TestHostnameLengthIsNotThisPackagesRuleToEnforce pins that the classic
// 253-byte DNS limit is the resolver's business no longer: the drivers
// accept a longer name, so the resolver does too and hands it on
// byte-for-byte. A length check here refused a valid maximum-length
// hostname the moment it was written in absolute form, which is the
// whole reason no length rule survives.
func TestHostnameLengthIsNotThisPackagesRuleToEnforce(t *testing.T) {
	t.Parallel()

	// 63.63.63.61 + 3 dots = 253 bytes, the classic maximum; the
	// absolute form adds a dot, and the third value is a byte past it.
	maxHostname := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." +
		strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	if len(maxHostname) != 253 {
		t.Fatalf("test setup: maxHostname is %d bytes, want 253", len(maxHostname))
	}

	for _, host := range []string{maxHostname, maxHostname + ".", maxHostname + "e"} {
		value, used, err := ResolveDSNFromComponents(lookup(map[string]string{
			"DEV_HEALTH_PG_DOMAIN_HOST": host,
		}), DomainDatabaseSpec)
		if !used || err != nil {
			t.Fatalf("host of %d bytes: used=%v err=%v", len(host), used, err)
		}
		cfg, parseErr := pgconn.ParseConfig(value.Reveal())
		if parseErr != nil {
			t.Fatalf("host of %d bytes: real pgx driver could not parse the assembled DSN: %v", len(host), parseErr)
		}
		if cfg.Host != host {
			t.Fatalf("host was not preserved verbatim: driver saw %q, want %q", cfg.Host, host)
		}
	}
}

// TestDatabaseNameReservedCharactersReachTheRealDriverExactly pins that a
// component is never refused on character grounds when the assembler can
// encode it: '/', '?', '#', a space, and '%' are all legitimate
// PostgreSQL/ClickHouse database identifier characters -- this resolver
// must never refuse them with a denylist it never needed, since every
// OTHER identifier in it is preserved exactly with no character denylist
// at all. Executed through the real drivers' own DSN parsers here, and
// LIVE against real PostgreSQL and ClickHouse containers in this
// package's integration suite (config_integration_test.go), where both
// drivers connect to each of these five database names and
// current_database()/currentDatabase() match exactly.
func TestDatabaseNameReservedCharactersReachTheRealDriverExactly(t *testing.T) {
	t.Parallel()

	for _, db := range []string{"app/db", "app?db", "app#db", "app db", "app%db"} {
		t.Run(db+" (PostgreSQL)", func(t *testing.T) {
			t.Parallel()
			value, used, err := ResolveDSNFromComponents(lookup(map[string]string{
				"DEV_HEALTH_PG_DOMAIN_HOST": "db.internal",
				"DEV_HEALTH_PG_DB":          db,
			}), DomainDatabaseSpec)
			if !used || err != nil {
				t.Fatalf("db %q: used=%v err=%v", db, used, err)
			}
			cfg, parseErr := pgconn.ParseConfig(value.Reveal())
			if parseErr != nil {
				t.Fatalf("db %q: real pgx driver could not parse the assembled DSN: %v", db, parseErr)
			}
			if cfg.Database != db {
				t.Fatalf("db %q: real pgx driver saw database=%q, want the exact identifier", db, cfg.Database)
			}
		})
		t.Run(db+" (ClickHouse)", func(t *testing.T) {
			t.Parallel()
			value, used, err := ResolveDSNFromComponents(lookup(map[string]string{
				"DEV_HEALTH_CH_HOST": "db.internal",
				"DEV_HEALTH_CH_DB":   db,
			}), ClickHouseSpec)
			if !used || err != nil {
				t.Fatalf("db %q: used=%v err=%v", db, used, err)
			}
			options, parseErr := clickhouse.ParseDSN(value.Reveal())
			if parseErr != nil {
				t.Fatalf("db %q: real clickhouse-go driver could not parse the assembled DSN: %v", db, parseErr)
			}
			if options.Auth.Database != db {
				t.Fatalf("db %q: real clickhouse-go driver saw database=%q, want the exact identifier", db, options.Auth.Database)
			}
		})
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
		// Never TrimSpace an identifier: a whitespace-only host must not
		// be treated as "absent" (trimmed to empty). It is non-empty raw
		// input, so the component form activates and the driver -- which
		// cannot escape a space into a host -- refuses it, rather than
		// this silently treating it the same as no host at all.
		{name: "whitespace-only host -- activates and the driver refuses it", mutate: func(m map[string]string) { m["TEST_HOST"] = "   " }, wantUsed: true, wantErr: true},
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
			name: "port non-numeric -- refused by the driver", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_PORT"] = "notaport" },
		},
		{
			name: "port with leading whitespace -- passed through untrimmed and refused by the driver", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_PORT"] = " 5432" },
		},
		// envOrDefault's own presence check trims for emptiness, so a
		// whitespace-only PORT/DB would be treated as absent and
		// silently fall to the default, substituting a value the
		// operator never wrote. rawOrDefault passes it through instead,
		// and what happens next is the driver's call: a space cannot be
		// escaped into a port, but it is a perfectly ordinary byte in a
		// database name or a user name.
		{
			name: "port whitespace-only -- passed through undefaulted and refused by the driver", wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_PORT"] = "   " },
		},
		{
			name: "db name whitespace-only -- passed through undefaulted and preserved exactly", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "   " }, wantDB: "   ",
		},
		{
			name: "user with trailing whitespace -- preserved exactly, not trimmed", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_USER"] = "app " }, wantUser: "app ", checkUser: true,
		},
		// '/', '?', and '#' are all valid
		// PostgreSQL/ClickHouse database identifier characters (a
		// double-quoted CREATE DATABASE name may contain any of them) --
		// refusing them violated the same exact-preservation contract
		// every other identifier in this resolver gets. url.URL's own
		// path encoder percent-escapes '?'/'#' when assembling the DSN
		// and both driver parsers decode them back to the literal
		// character; '/' is a legitimate literal byte within one path
		// segment and neither driver splits the database name on it.
		{
			name: "db name with / -- preserved exactly, not refused", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "app/db" }, wantDB: "app/db",
		},
		{
			name: "db name with ? -- preserved exactly, not refused", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "app?db" }, wantDB: "app?db",
		},
		{
			name: "db name with # -- preserved exactly, not refused", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "app#db" }, wantDB: "app#db",
		},
		{
			name: "db name with % -- preserved exactly, not refused", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "app%db" }, wantDB: "app%db",
		},
		// Leading/trailing whitespace on an identifier must never be
		// silently trimmed: doing so connects (proven against a real
		// live PostgreSQL) to a DIFFERENT existing database than the one
		// actually configured. Both drivers accept such a name, so it is
		// carried through percent-encoded and preserved exactly, edge
		// whitespace and inner whitespace alike.
		{
			name: "db name with leading whitespace -- preserved exactly, not trimmed", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = " appdb" }, wantDB: " appdb",
		},
		{
			name: "db name with trailing whitespace -- preserved exactly, not trimmed", wantUsed: true,
			mutate: func(m map[string]string) { m["TEST_DB"] = "appdb " }, wantDB: "appdb ",
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
			name:     "host containing a path-injection attempt -- refused by the driver",
			wantUsed: true, wantErr: true,
			mutate: func(m map[string]string) { m["TEST_HOST"] = "db.internal/evil" },
		},
		{
			name:     "host containing an authority-injection attempt -- refused by the driver",
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
			if tc.checkUser && parsed.User.Username() != tc.wantUser {
				t.Fatalf("user = %q, want %q -- the identifier must be preserved exactly", parsed.User.Username(), tc.wantUser)
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

// TestURIAndComponentFormsAreMutuallyExclusivePerDSN pins the design rule
// that NEITHER form may silently win over the other
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

	// URI + component HOST, both set: refused, naming both keys -- e.g.
	// an operator set POSTGRES_DOMAIN_HOST to move to components while a
	// stale POSTGRES_URI/_FILE pair still sat in the environment.
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

	// The exclusivity check must not inspect only HOST: a non-host
	// component (here, USER) set alongside a raw URI must not be
	// silently ignored -- ResolveDSNFromComponents never activates without
	// HOST, so the raw URI would otherwise win with no error and no
	// indication the stray USER var was sitting unused.
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

	// A non-host component set with HOST absent AND no raw URI either
	// must not silently report "not configured" -- the same as if
	// nothing had been set at all, with no sign the operator had
	// already started configuring components.
	_, err = Load(workerSpec(map[string]string{
		"DEV_HEALTH_PG_DOMAIN_USER": "app",
		"WORKER_DATABASE_URI":       "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err == nil ||
		!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_USER") ||
		!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_HOST") {
		t.Fatalf("expected a partial component set (no HOST, no raw URI) to name the missing HOST key, got: %v", err)
	}

	// An empty-string raw URI alongside a full, valid component set must
	// not be refused as "mutually exclusive" -- the exclusivity check
	// must use secrets.Resolve's own "configured" predicate, never bare
	// env-var PRESENCE (a direct value of exactly "" is NOT
	// configured, per secrets.Resolve's `direct != ""` rule). An operator
	// who leaves POSTGRES_URI="" set (e.g. a compose file's unset-interpolation
	// default) while fully configuring components must succeed on components,
	// not be refused for a raw value that contributes nothing.
	cfg3, err := Load(workerSpec(map[string]string{
		"POSTGRES_URI":                  "",
		"DEV_HEALTH_PG_DOMAIN_HOST":     "db.internal",
		"DEV_HEALTH_PG_DOMAIN_USER":     "app",
		"DEV_HEALTH_PG_DOMAIN_PASSWORD": "app",
		"DEV_HEALTH_PG_DB":              "appdb",
		"WORKER_DATABASE_URI":           "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err != nil {
		t.Fatalf("an empty-string raw URI alongside valid components should succeed on components, got: %v", err)
	}
	if cfg3.DomainDatabaseURI.Reveal() != "postgresql://app:app@db.internal:5432/appdb" {
		t.Fatalf("expected the component-built DSN, got %q", cfg3.DomainDatabaseURI.Reveal())
	}

	// Companion cell: a WHITESPACE-ONLY raw URI is, unlike "", still
	// "configured" under secrets.Resolve's own rule (it only special-cases
	// exactly ""; it never trims). ResolveDSN's exclusivity check must keep
	// treating it as the raw form being set -- components alongside it is
	// still refused, for the same reason a real (non-blank) raw value would
	// be: the raw form has no whitespace refusal of its own, only components
	// do.
	_, err = Load(workerSpec(map[string]string{
		"POSTGRES_URI":                  "   ",
		"DEV_HEALTH_PG_DOMAIN_HOST":     "db.internal",
		"DEV_HEALTH_PG_DOMAIN_USER":     "app",
		"DEV_HEALTH_PG_DOMAIN_PASSWORD": "app",
		"DEV_HEALTH_PG_DB":              "appdb",
		"WORKER_DATABASE_URI":           "postgresql://app:app@db.internal:5432/appdb",
	}))
	if err == nil ||
		!strings.Contains(err.Error(), "POSTGRES_URI") ||
		!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_HOST") ||
		!strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected a whitespace-only raw URI alongside components to still be refused as mutually exclusive, got: %v", err)
	}
}

// TestNonSharedDBKeyAndFileVariantsAreDetected pins two requirements:
// (1) ClickHouse's DBKey is NOT shared the
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

// TestWhitespaceOnlyPasswordIsDetectedAndAuthenticates pins that presence
// detection must never TrimSpace a secret: a password consisting only of
// whitespace is Configured() to secrets.Resolve (which actually builds
// the connection, and deliberately never trims a secret's meaningful
// content) and DOES authenticate (proven against a real live PostgreSQL)
// -- if presence detection trimmed it before checking emptiness, it
// would be invisible to the exclusivity check, letting it silently
// coexist with a raw URI unflagged.
func TestWhitespaceOnlyPasswordIsDetectedAndAuthenticates(t *testing.T) {
	t.Parallel()

	// Presence: a whitespace-only password alongside a raw URI must be
	// refused, not silently ignored.
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

// TestWhitespaceOnlyFileSourcedCredentialsAreDetected is the explicit
// cell for the whitespace-detection rule: a whitespace-only
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

// TestFileReadFailureNeverEchoesTheConfiguredPath pins the fix at the root
// (internal/platform/secrets/source.go): secrets.Resolve's KEY_FILE
// read-failure error must never wrap the underlying os.PathError
// verbatim, since Go's os.PathError.Error() embeds the exact path it tried to open --
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
