package main

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

type streamCommandHandler struct{}

func (streamCommandHandler) Handle(context.Context, streamrunner.Message) error { return nil }

type streamCommandTransport struct{}

func (*streamCommandTransport) EnsureGroup(context.Context, string, string) error { return nil }
func (*streamCommandTransport) ReadNew(context.Context, []string, string, string, int, time.Duration) ([]streamrunner.Message, error) {
	return nil, nil
}
func (*streamCommandTransport) Pending(context.Context, string, string, int, time.Duration) ([]streamrunner.Pending, error) {
	return nil, nil
}
func (*streamCommandTransport) Claim(context.Context, string, string, string, []string, time.Duration) ([]streamrunner.Message, error) {
	return nil, nil
}
func (*streamCommandTransport) Ack(context.Context, string, string, string) error { return nil }
func (*streamCommandTransport) Quarantine(context.Context, streamrunner.Message, string) error {
	return nil
}
func (*streamCommandTransport) Stats(context.Context, string, string) (streamrunner.StreamStats, error) {
	return streamrunner.StreamStats{}, nil
}
func (*streamCommandTransport) Discover(context.Context, []string, int) ([]string, error) {
	return nil, nil
}
func (*streamCommandTransport) Close() {}

type streamCommandStorage struct {
	handlers []streamHandlerKind
	closed   bool
}

func (*streamCommandStorage) ClickHouseReady(context.Context) error     { return nil }
func (*streamCommandStorage) DomainPostgresReady(context.Context) error { return nil }
func (*streamCommandStorage) ValkeyReady(context.Context) error         { return nil }
func (storage *streamCommandStorage) Handler(kind streamHandlerKind) (streamrunner.Handler, error) {
	storage.handlers = append(storage.handlers, kind)
	return streamCommandHandler{}, nil
}
func (*streamCommandStorage) NewTransport() (streamrunner.Transport, error) {
	return &streamCommandTransport{}, nil
}
func (*streamCommandStorage) ControlComponents() []lifecycle.Component { return nil }
func (storage *streamCommandStorage) Close()                           { storage.closed = true }

func TestStreamRunnerSpecBuildsProductionProfiles(t *testing.T) {
	if streamRunnerSpec.Service != "dev-health-stream-runner" || streamRunnerSpec.DefaultProfile != "ingest" {
		t.Fatalf("unexpected stream-runner spec: %#v", streamRunnerSpec)
	}
	if !slices.Equal(streamRunnerSpec.Profiles, []string{"ingest", "external"}) {
		t.Fatalf("unexpected stream profiles: %v", streamRunnerSpec.Profiles)
	}
	if streamRunnerSpec.ConfigureDependencies == nil {
		t.Fatal("stream-runner dependency configuration is not wired")
	}

	t.Run("ingest owns two isolated loops over process storage", func(t *testing.T) {
		storage := &streamCommandStorage{}
		registry := health.NewRegistry(100 * time.Millisecond)
		components, err := configureStreamRunnerDependenciesWithSources(
			context.Background(),
			config.Config{Profile: "ingest", StreamConfiguredReplicas: 1},
			registry,
			streamDependencySources{
				openStorage: func(context.Context, config.Config) (streamStorage, error) {
					return storage, nil
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(components) != 3 {
			t.Fatalf("components = %d, want storage plus two loops", len(components))
		}
		if !slices.Equal(storage.handlers, []streamHandlerKind{
			internalIngestHandlerKind,
			productTelemetryHandlerKind,
		}) {
			t.Fatalf("handlers = %v", storage.handlers)
		}
		if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		status := registry.Readiness(context.Background())
		want := []string{"internal_ingest_loop", "product_telemetry_loop"}
		if status.Ready || !slices.Equal(status.Failed, want) {
			t.Fatalf("readiness = %#v, want failed %v", status, want)
		}
	})

	t.Run("external singleton configuration fails closed", func(t *testing.T) {
		storage := &streamCommandStorage{}
		_, err := configureStreamRunnerDependenciesWithSources(
			context.Background(),
			config.Config{Profile: "external", StreamConfiguredReplicas: 2},
			health.NewRegistry(time.Second),
			streamDependencySources{
				openStorage: func(context.Context, config.Config) (streamStorage, error) {
					return storage, nil
				},
			},
		)
		if err == nil || !storage.closed {
			t.Fatalf("duplicate external replicas: err=%v storage_closed=%v", err, storage.closed)
		}
	})
}

func TestProductionRunnerConfigsPreservePythonStreamContracts(t *testing.T) {
	internal := internalIngestRunnerConfig(1)
	if internal.ConsumerGroup != "ingest-consumers" || internal.BatchSize != 100 ||
		!slices.Equal(internal.Patterns, []string{
			"ingest:*:commits",
			"ingest:*:deployments",
			"ingest:*:incidents",
			"ingest:*:pull-requests",
			"ingest:*:work-items",
		}) {
		t.Fatalf("internal config = %#v", internal)
	}
	product := productTelemetryRunnerConfig(1)
	if product.ConsumerGroup != "product-telemetry-consumers" ||
		!slices.Equal(product.Patterns, []string{"product-telemetry:*:events"}) {
		t.Fatalf("product config = %#v", product)
	}
	external := externalIngestRunnerConfig(1)
	if external.ConsumerGroup != "external-ingest-consumers" ||
		external.ReclaimIdle != 15*time.Minute || external.MaxDeliveries != 5 ||
		!external.Singleton || external.ConfiguredReplicas != 1 {
		t.Fatalf("external config = %#v", external)
	}
}
