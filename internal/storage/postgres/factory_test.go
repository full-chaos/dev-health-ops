package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestConfigRejectsInvalidBounds(t *testing.T) {
	t.Parallel()

	tests := []Config{
		{},
		{URI: "postgres://localhost/database", MaxConns: -1, ConnectTimeout: time.Second, MaxConnLifetime: time.Second, MaxConnIdleTime: time.Second, HealthCheckPeriod: time.Second},
		{URI: "postgres://localhost/database", MinConns: 2, MaxConns: 1, ConnectTimeout: time.Second, MaxConnLifetime: time.Second, MaxConnIdleTime: time.Second, HealthCheckPeriod: time.Second},
		{URI: "://not-a-dsn", MaxConns: 1, ConnectTimeout: time.Second, MaxConnLifetime: time.Second, MaxConnIdleTime: time.Second, HealthCheckPeriod: time.Second},
	}
	for _, config := range tests {
		if err := config.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
		}
	}
}

func TestSafeSurfaceRedactsURI(t *testing.T) {
	t.Parallel()

	const secret = "never-log-this-password"
	config := DefaultConfig("postgres://worker:" + secret + "@database.internal/production")
	surface := fmt.Sprint(config.SafeAttributes(), config.Validate())
	if strings.Contains(surface, secret) || strings.Contains(surface, config.URI) {
		t.Fatalf("safe surface exposed PostgreSQL connection material: %s", surface)
	}
}

func TestDefaultConfigDoesNotPinIdleSessionPoolBackends(t *testing.T) {
	t.Parallel()

	config := DefaultConfig("postgres://queue:secret@pgbouncer-river-queue:6433/app")
	if config.MinConns != 0 {
		t.Fatalf("MinConns = %d, want 0 so idle clients release session-pool backends", config.MinConns)
	}
	if config.MaxConnIdleTime <= 0 {
		t.Fatalf("MaxConnIdleTime = %s, want bounded idle release", config.MaxConnIdleTime)
	}
}

func TestOpenReturnsSanitizedUnavailableError(t *testing.T) {
	t.Parallel()

	const secret = "never-log-this-password"
	config := DefaultConfig("postgres://worker:" + secret + "@127.0.0.1:1/production")
	config.ConnectTimeout = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := Open(ctx, config)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Open() error = %v, want ErrUnavailable", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), config.URI) {
		t.Fatalf("Open() exposed PostgreSQL connection material: %v", err)
	}
}

func TestDocumentedSQLAlchemySchemesNormalizeForPgx(t *testing.T) {
	t.Parallel()

	for _, uri := range []string{
		"postgresql+asyncpg://domain_role:secret@postgres.internal/app?sslmode=require",
		"postgresql+psycopg://domain_role:secret@postgres.internal/app?sslmode=require",
		"postgresql+psycopg2://domain_role:secret@postgres.internal/app?sslmode=require",
	} {
		cfg := DefaultConfig(uri)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate(%q) error = %v", uri, err)
		}
		user, err := ConnectionUser(uri)
		if err != nil {
			t.Fatal(err)
		}
		if user != "domain_role" {
			t.Fatalf("ConnectionUser(%q) = %q", uri, user)
		}
	}
}
