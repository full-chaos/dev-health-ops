package main

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func TestStreamRunnerSpecConfiguresFailClosedDependencies(t *testing.T) {
	if streamRunnerSpec.Service != "dev-health-stream-runner" || streamRunnerSpec.DefaultProfile != "ingest" {
		t.Fatalf("unexpected stream-runner spec: %#v", streamRunnerSpec)
	}
	if !slices.Equal(streamRunnerSpec.Profiles, []string{"ingest", "external"}) {
		t.Fatalf("unexpected stream profiles: %v", streamRunnerSpec.Profiles)
	}
	if streamRunnerSpec.ConfigureDependencies == nil {
		t.Fatal("stream-runner dependency configuration is not wired")
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureStreamRunnerDependencies(
		context.Background(),
		config.Config{Profile: "ingest"},
		registry,
	)
	if err != nil {
		t.Fatalf("configureStreamRunnerDependencies() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no stream runtime before its consumer is implemented", len(components))
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	want := []string{"clickhouse", "domain_postgres", "stream_consumer", "valkey"}
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}
