package main

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func TestReconcilerSpecConfiguresFailClosedDependencies(t *testing.T) {
	if reconcilerSpec.Service != "dev-health-reconciler" {
		t.Fatalf("service = %q", reconcilerSpec.Service)
	}
	if reconcilerSpec.ConfigureDependencies == nil {
		t.Fatal("reconciler dependency configuration is not wired")
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependencies(context.Background(), config.Config{}, registry)
	if err != nil {
		t.Fatalf("configureReconcilerDependencies() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no reconciler runtime before its loop is implemented", len(components))
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	want := []string{"domain_postgres", "queue_postgres", "reconciler_loop", "river_schema"}
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}
