package config

import (
	"fmt"
	"log/slog"
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
