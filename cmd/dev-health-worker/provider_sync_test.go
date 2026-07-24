package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type independentRiverClient struct {
	name     string
	config   *river.Config
	handlers map[string]struct{}
}

func TestIndependentSyncClientsDoNotShareQueuesWithDisjointHandlers(
	t *testing.T,
) {
	coordinator := independentRiverClient{
		name: "sync-coordinator",
		config: &river.Config{
			Queues: map[string]river.QueueConfig{
				"sync": {MaxWorkers: 2},
			},
		},
		handlers: handlerSet(
			syncdispatchcontract.KindDispatchSyncRun,
			syncdispatchcontract.KindFinalizeSyncRun,
			syncdispatchcontract.KindPostSync,
			syncdispatchcontract.KindReferenceDiscovery,
		),
	}
	provider := independentRiverClient{
		name: "provider-unit",
		config: providerSyncRiverConfig(
			slog.Default(), river.NewWorkers(), "river",
		),
		handlers: handlerSet(jobcontract.KindSyncProviderUnit),
	}

	assertNoSharedQueueWithDisjointHandlers(t, coordinator, provider)
	if _, ok := provider.config.Queues[providerUnitQueue]; !ok {
		t.Fatalf("provider client does not own %q", providerUnitQueue)
	}
}

func TestProviderSyncClientOwnsItsRegistryQueue(t *testing.T) {
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := registry.Descriptor(jobcontract.KindSyncProviderUnit)
	if !ok {
		t.Fatal("provider-unit descriptor missing")
	}
	config := providerSyncRiverConfig(
		slog.Default(), river.NewWorkers(), "river",
	)
	if descriptor.Queue != providerUnitQueue {
		t.Fatalf(
			"provider-unit registry queue=%q want=%q",
			descriptor.Queue, providerUnitQueue,
		)
	}
	if _, ok := config.Queues[descriptor.Queue]; !ok {
		t.Fatalf("provider client does not consume registry queue %q", descriptor.Queue)
	}
}

func TestProviderSyncRiverConfigPassesRiverClientValidation(t *testing.T) {
	pool, err := pgxpool.New(
		context.Background(),
		"postgresql://unused:unused@127.0.0.1:1/unused",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	_, err = river.NewClient(
		riverpgxv5.New(pool),
		providerSyncRiverConfig(
			slog.Default(), river.NewWorkers(), "river",
		),
	)
	if err != nil {
		t.Fatalf("provider sync River config is invalid: %v", err)
	}
}

func handlerSet(kinds ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(kinds))
	for _, kind := range kinds {
		result[kind] = struct{}{}
	}
	return result
}

func assertNoSharedQueueWithDisjointHandlers(
	t *testing.T,
	clients ...independentRiverClient,
) {
	t.Helper()
	for left := range clients {
		for right := left + 1; right < len(clients); right++ {
			if !disjointHandlerSets(clients[left].handlers, clients[right].handlers) {
				continue
			}
			for queue := range clients[left].config.Queues {
				if _, shared := clients[right].config.Queues[queue]; shared {
					t.Fatalf(
						"independent River clients %q and %q share queue %q with disjoint handlers",
						clients[left].name, clients[right].name, queue,
					)
				}
			}
		}
	}
}

func disjointHandlerSets(left, right map[string]struct{}) bool {
	for kind := range left {
		if _, shared := right[kind]; shared {
			return false
		}
	}
	return true
}
