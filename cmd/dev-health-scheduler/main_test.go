package main

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func TestSchedulerSpecConfiguresFailClosedDependencies(t *testing.T) {
	if schedulerSpec.Service != "dev-health-scheduler" {
		t.Fatalf("service = %q", schedulerSpec.Service)
	}
	if schedulerSpec.ConfigureDependencies == nil {
		t.Fatal("scheduler dependency configuration is not wired")
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureSchedulerDependencies(context.Background(), config.Config{}, registry)
	if err != nil {
		t.Fatalf("configureSchedulerDependencies() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no scheduler runtime before its loop is implemented", len(components))
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	want := []string{"domain_postgres", "queue_postgres", "river_schema", "scheduler_loop"}
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}
