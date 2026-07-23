package config

import (
	"fmt"
	"log/slog"
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

func workerSpec(values map[string]string) Spec {
	return Spec{
		Service:        "dev-health-worker",
		Profiles:       []string{"latency", "sync", "heavy", "ops"},
		DefaultProfile: "latency",
		LookupEnv:      lookup(values),
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
		"POSTGRES_URI":        secret,
		"WORKER_DATABASE_URI": "postgres://queue:other-secret@database.internal/app",
		"CLICKHOUSE_URI":      "clickhouse://analytics:secret@ch.internal/default",
		"VALKEY_URI":          "redis://:secret@valkey.internal/1",
	}))
	if err != nil {
		t.Fatal(err)
	}

	text := fmt.Sprint(cfg.SafeAttrs())
	for _, forbidden := range []string{secret, "top-secret", "clickhouse://", "redis://"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("safe attrs leaked %q: %s", forbidden, text)
		}
	}
	for _, expected := range []string{
		"domain_database_configured=true",
		"queue_database_configured=true",
		"clickhouse_configured=true",
		"valkey_configured=true",
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
}

func TestQueueControlAndRetentionOverridesAreBounded(t *testing.T) {
	t.Parallel()

	cfg, err := Load(workerSpec(map[string]string{
		"WORKER_DATABASE_MODE":                     "transaction",
		"PGBOUNCER_TRANSACTION_MODE":               "true",
		"RIVER_DATABASE_SCHEMA":                    "worker_queue",
		"RIVER_DOMAIN_DATABASE_ROLE":               "worker_domain",
		"RIVER_QUEUE_DATABASE_ROLE":                "worker_queue",
		"WORKER_DATABASE_MAX_CONNS":                "4",
		"WORKER_DOMAIN_DATABASE_MAX_CONNS":         "12",
		"RIVER_COMPLETED_JOB_RETENTION":            "48h",
		"RIVER_CANCELLED_JOB_RETENTION":            "240h",
		"RIVER_DISCARDED_JOB_RETENTION":            "336h",
		"RIVER_JOB_CLEANER_TIMEOUT":                "45s",
		"WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE": "true",
		"DEV_HEALTH_STREAM_REPLICAS":               "3",
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
