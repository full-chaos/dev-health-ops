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
		"POSTGRES_URI":   secret,
		"CLICKHOUSE_URI": "clickhouse://analytics:secret@ch.internal/default",
		"VALKEY_URI":     "redis://:secret@valkey.internal/1",
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
		"clickhouse_configured=true",
		"valkey_configured=true",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("safe attrs missing %q: %s", expected, text)
		}
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
