// Package config defines the shared runtime-shell configuration contract.
package config

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	defaultHTTPAddress       = ":8080"
	defaultShutdownTimeout   = 30 * time.Second
	defaultHealthCheckTimout = 2 * time.Second
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

	DomainDatabaseURI secrets.Value
	ClickHouseURI     secrets.Value
	ValkeyURI         secrets.Value
}

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
		{name: "CLICKHOUSE_URI", target: &cfg.ClickHouseURI},
		{name: "VALKEY_URI", target: &cfg.ValkeyURI},
	}
	for _, item := range secretTargets {
		value, _, resolveErr := secrets.Resolve(item.name, lookup)
		if resolveErr != nil {
			return Config{}, resolveErr
		}
		*item.target = value
	}

	if err := validateURI("POSTGRES_URI", cfg.DomainDatabaseURI, "postgres", "postgresql"); err != nil {
		return Config{}, err
	}
	if err := validateURI("CLICKHOUSE_URI", cfg.ClickHouseURI, "clickhouse", "http", "https"); err != nil {
		return Config{}, err
	}
	if err := validateURI("VALKEY_URI", cfg.ValkeyURI, "redis", "rediss", "unix"); err != nil {
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
		slog.Bool("clickhouse_configured", c.ClickHouseURI.Configured()),
		slog.Bool("valkey_configured", c.ValkeyURI.Configured()),
	}
	if c.Profile != "" {
		attrs = append(attrs, slog.String("profile", c.Profile))
	}
	return attrs
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
