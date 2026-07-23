package processreadiness

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func TestRegisterUnavailableFailsClosedWithStableNames(t *testing.T) {
	registry := health.NewRegistry(100 * time.Millisecond)
	if err := RegisterUnavailable(registry, "queue_postgres", "domain_postgres"); err != nil {
		t.Fatalf("RegisterUnavailable() error = %v", err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	status := registry.Readiness(context.Background())
	want := []string{"domain_postgres", "queue_postgres"}
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestRegisterUnavailableRejectsNilRegistry(t *testing.T) {
	if err := RegisterUnavailable(nil, "domain_postgres"); err == nil {
		t.Fatal("RegisterUnavailable() unexpectedly accepted a nil registry")
	}
}
