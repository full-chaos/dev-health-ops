package clickhouse

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
		{DSN: "clickhouse://localhost/default", MaxOpenConns: 1, MaxIdleConns: 2, DialTimeout: time.Second, ReadTimeout: time.Second, ConnMaxLifetime: time.Second},
		{DSN: "://not-a-dsn", MaxOpenConns: 1, DialTimeout: time.Second, ReadTimeout: time.Second, ConnMaxLifetime: time.Second},
	}
	for _, config := range tests {
		if err := config.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
		}
	}
}

func TestSafeSurfaceRedactsDSN(t *testing.T) {
	t.Parallel()

	const secret = "never-log-this-password"
	config := DefaultConfig("clickhouse://worker:" + secret + "@clickhouse.internal/production")
	surface := fmt.Sprint(config.SafeAttributes(), config.Validate())
	if strings.Contains(surface, secret) || strings.Contains(surface, config.DSN) {
		t.Fatalf("safe surface exposed ClickHouse connection material: %s", surface)
	}
}

func TestOpenReturnsSanitizedUnavailableError(t *testing.T) {
	t.Parallel()

	const secret = "never-log-this-password"
	config := DefaultConfig("clickhouse://worker:" + secret + "@127.0.0.1:1/production")
	config.DialTimeout = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := Open(ctx, config)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Open() error = %v, want ErrUnavailable", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), config.DSN) {
		t.Fatalf("Open() exposed ClickHouse connection material: %v", err)
	}
}
