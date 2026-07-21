package valkey

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
		{URI: "redis://localhost:6379/1", ClientName: "worker", DialTimeout: time.Second, WriteTimeout: time.Second, ConnLifetime: time.Second, BlockingPoolSize: 1, BlockingPoolMinSize: 2},
		{URI: "://not-a-uri", ClientName: "worker", DialTimeout: time.Second, WriteTimeout: time.Second, ConnLifetime: time.Second, BlockingPoolSize: 1},
		DefaultConfig("redis://localhost:6379/0"),
		DefaultConfig("redis://localhost:6379"),
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
	config := DefaultConfig("redis://worker:" + secret + "@valkey.internal:6379/1")
	surface := fmt.Sprint(config.SafeAttributes(), config.Validate())
	if strings.Contains(surface, secret) || strings.Contains(surface, config.URI) {
		t.Fatalf("safe surface exposed Valkey connection material: %s", surface)
	}
}

func TestOpenReturnsSanitizedUnavailableError(t *testing.T) {
	t.Parallel()

	const secret = "never-log-this-password"
	config := DefaultConfig("redis://worker:" + secret + "@127.0.0.1:1/1")
	config.DialTimeout = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := Open(ctx, config)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Open() error = %v, want ErrUnavailable", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), config.URI) {
		t.Fatalf("Open() exposed Valkey connection material: %v", err)
	}
}
