package main

import (
	"log/slog"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

// TestFinalizeServiceIsBuiltWithTheCoverageCacheInvalidator is the
// constructive half of the CHAOS-4226 reachability proof (AGENTS.md: "a
// cited constructor is not proof of capability"): the exact helper
// buildSyncCoordinatorWorker calls must hand the Valkey client to the
// finalize service, observable through the service's own configuration
// probe rather than through which constructor a reader can cite.
func TestFinalizeServiceIsBuiltWithTheCoverageCacheInvalidator(t *testing.T) {
	t.Parallel()
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	var client valkeygo.Client = fakeValkeyClient{}
	service, err := buildFinalizeSyncRunService(&pgxpool.Pool{}, slog.Default(), collector, client)
	if err != nil {
		t.Fatal(err)
	}
	if !service.CoverageCacheInvalidatorConfigured() {
		t.Fatal("finalize service built without the coverage cache invalidator")
	}
	if _, err := buildFinalizeSyncRunService(&pgxpool.Pool{}, slog.Default(), collector, nil); err == nil {
		t.Fatal("finalize service built with a nil Valkey client")
	}
}

// fakeValkeyClient satisfies valkeygo.Client without a live server; the
// builder under test only stores the client, it never issues a command.
type fakeValkeyClient struct{ valkeygo.Client }
