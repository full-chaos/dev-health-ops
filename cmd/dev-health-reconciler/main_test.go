package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

func TestReconcilerSpecConfiguresFailClosedDependencies(t *testing.T) {
	if reconcilerSpec.Service != "dev-health-reconciler" {
		t.Fatalf("service = %q", reconcilerSpec.Service)
	}
	if reconcilerSpec.ConfigureDependenciesWithLogger == nil || reconcilerSpec.ConfigureDependencies != nil {
		t.Fatal("reconciler logger-aware dependency configuration is not exclusively wired")
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithLogger(
		context.Background(),
		config.Config{},
		registry,
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithLogger() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no runtime pools without database configuration", len(components))
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	want := []string{"domain_postgres", "job_registry", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer", "sync_dispatch_registry"}
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestReconcilerOperatorStaysLiveWhenDependenciesAreMissing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lookup := func(key string) (string, bool) {
		values := map[string]string{
			"DEV_HEALTH_HTTP_ADDR":        address,
			"DEV_HEALTH_SHUTDOWN_TIMEOUT": "1s",
		}
		value, ok := values[key]
		return value, ok
	}
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- shell.Execute(ctx, reconcilerSpec, nil, secrets.LookupEnv(lookup), shell.IO{
			Stdout: &stdout,
			Stderr: &stderr,
		})
	}()

	client := &http.Client{Timeout: 100 * time.Millisecond}
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/healthz")
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("healthz status = %d", response.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("operator HTTP did not start: %v logs=%s stderr=%s", requestErr, stdout.String(), stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	response, err := client.Get("http://" + address + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}

	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("shell exit = %d logs=%s stderr=%s", code, stdout.String(), stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("operator HTTP did not stop after cancellation")
	}
}
