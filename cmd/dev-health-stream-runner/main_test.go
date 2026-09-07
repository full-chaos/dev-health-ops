package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/pagerduty"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
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
	handlers   []streamHandlerKind
	handlerErr error
	valkeyErr  error
	closed     bool
}

func (*streamCommandStorage) ClickHouseReady(context.Context) error     { return nil }
func (*streamCommandStorage) DomainPostgresReady(context.Context) error { return nil }

func (*streamCommandStorage) PostureManifestLockstep(
	context.Context, string,
) (postgres.PostureManifestLockstepResult, error) {
	return postgres.PostureManifestLockstepResult{Lockstep: true}, nil
}
func (storage *streamCommandStorage) ValkeyReady(context.Context) error { return storage.valkeyErr }
func (storage *streamCommandStorage) Handler(kind streamHandlerKind, _ streamhandlers.ExternalIngestObserver) (streamrunner.Handler, error) {
	if storage.handlerErr != nil {
		return nil, storage.handlerErr
	}
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
	if !slices.Equal(streamRunnerSpec.Profiles, []string{"ingest", "external", "pagerduty"}) {
		t.Fatalf("unexpected stream profiles: %v", streamRunnerSpec.Profiles)
	}
	if streamRunnerSpec.ConfigureDependenciesWithLogger == nil {
		t.Fatal("stream-runner dependency configuration is not wired")
	}

	t.Run("unconfigured storage stays live and fails readiness", func(t *testing.T) {
		registry := health.NewRegistry(100 * time.Millisecond)
		components, err := configureStreamRunnerDependenciesWithLogger(
			context.Background(), config.Config{}, registry, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(components) != 0 {
			t.Fatalf("components = %d, want no stream consumers without storage", len(components))
		}
		if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		want := []string{"clickhouse", "domain_postgres", "posture_manifest_lockstep", "stream_consumer", "valkey"}
		if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, want) {
			t.Fatalf("readiness = %#v, want failed %v", status, want)
		}
	})

	t.Run("ingest owns two isolated loops over process storage", func(t *testing.T) {
		storage := &streamCommandStorage{}
		registry := health.NewRegistry(100 * time.Millisecond)
		components, err := configureStreamRunnerDependenciesWithSources(
			context.Background(),
			config.Config{Profile: "ingest", StreamConfiguredReplicas: 1},
			registry,
			streamDependencySources{
				openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
					return storage, nil
				},
			},
			nil,
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

	t.Run("unavailable stream consumer stays live and fails readiness", func(t *testing.T) {
		storage := &streamCommandStorage{handlerErr: errors.New("consumer unavailable")}
		registry := health.NewRegistry(100 * time.Millisecond)
		components, err := configureStreamRunnerDependenciesWithSources(
			context.Background(),
			config.Config{Profile: "ingest", StreamConfiguredReplicas: 1},
			registry,
			streamDependencySources{
				openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
					return storage, nil
				},
			},
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(components) != 0 || !storage.closed {
			t.Fatalf("components=%d storage_closed=%v, want no components and closed storage", len(components), storage.closed)
		}
		if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		want := []string{"stream_consumer"}
		if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, want) {
			t.Fatalf("readiness = %#v, want failed %v", status, want)
		}
	})

	t.Run("unavailable storage defers stream consumer construction", func(t *testing.T) {
		storage := &streamCommandStorage{valkeyErr: errors.New("valkey unavailable")}
		registry := health.NewRegistry(100 * time.Millisecond)
		components, err := configureStreamRunnerDependenciesWithSources(
			context.Background(),
			config.Config{Profile: "ingest", StreamConfiguredReplicas: 1},
			registry,
			streamDependencySources{
				openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
					return storage, nil
				},
			},
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(components) != 0 || len(storage.handlers) != 0 || !storage.closed {
			t.Fatalf("components=%d handlers=%v storage_closed=%v, want deferred consumer construction", len(components), storage.handlers, storage.closed)
		}
		if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		want := []string{"stream_consumer", "valkey"}
		if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, want) {
			t.Fatalf("readiness = %#v, want failed %v", status, want)
		}
	})

	t.Run("pagerduty owns one webhook loop over process storage", func(t *testing.T) {
		storage := &streamCommandStorage{}
		registry := health.NewRegistry(100 * time.Millisecond)
		components, err := configureStreamRunnerDependenciesWithSources(
			context.Background(),
			config.Config{Profile: "pagerduty", StreamConfiguredReplicas: 2},
			registry,
			streamDependencySources{
				openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
					return storage, nil
				},
			},
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(components) != 2 {
			t.Fatalf("components = %d, want storage plus one webhook loop", len(components))
		}
		if !slices.Equal(storage.handlers, []streamHandlerKind{pagerdutyHandlerKind}) {
			t.Fatalf("handlers = %v", storage.handlers)
		}
		if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		status := registry.Readiness(context.Background())
		want := []string{"pagerduty_webhooks_loop"}
		if status.Ready || !slices.Equal(status.Failed, want) {
			t.Fatalf("readiness = %#v, want failed %v", status, want)
		}
	})

	t.Run("unavailable pagerduty bridge stays live and fails readiness", func(t *testing.T) {
		// The compatibility reconciler is unconfigured until the worker bridge
		// URL and token are deployed; that must never kill the process.
		storage := &streamCommandStorage{handlerErr: errors.New("bridge unavailable")}
		registry := health.NewRegistry(100 * time.Millisecond)
		components, err := configureStreamRunnerDependenciesWithSources(
			context.Background(),
			config.Config{Profile: "pagerduty", StreamConfiguredReplicas: 1},
			registry,
			streamDependencySources{
				openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
					return storage, nil
				},
			},
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(components) != 0 || !storage.closed {
			t.Fatalf("components=%d storage_closed=%v, want no components and closed storage", len(components), storage.closed)
		}
		if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		want := []string{"stream_consumer"}
		if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, want) {
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
				openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
					return storage, nil
				},
			},
			nil,
		)
		if err == nil || !storage.closed {
			t.Fatalf("duplicate external replicas: err=%v storage_closed=%v", err, storage.closed)
		}
	})
}

func TestStreamRunnerOperatorStaysLiveWhenDependenciesAreMissing(t *testing.T) {
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
		done <- shell.Execute(ctx, streamRunnerSpec, nil, secrets.LookupEnv(lookup), shell.IO{
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

	// The readiness gate (health.Gate) is a SEPARATE lifecycle component that
	// opens admission (Registry.SetReady(true)) only after earlier components
	// -- including the HTTP server this loop above just confirmed is
	// listening -- have started. Until the gate opens, Registry.Readiness
	// fails closed with the sentinel Failed: []string{"runtime"} (registry.go
	// SetReady/Readiness), never the real per-dependency check names. That
	// window is a normal, sequential part of startup, not a race in the
	// product: it only WIDENS under -race/hosted-CI load, which is what
	// turned a single immediate /readyz check flaky (CHAOS-5338). Poll with a
	// generous bounded deadline instead of asserting on the first response.
	readyDeadline := time.Now().Add(30 * time.Second)
	var readiness []byte
	for {
		response, err := client.Get("http://" + address + "/readyz")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("readyz status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
		}
		readiness = body
		missingDependency := false
		for _, dependency := range []string{"clickhouse", "domain_postgres", "stream_consumer", "valkey"} {
			if !strings.Contains(string(readiness), `"`+dependency+`"`) {
				missingDependency = true
				break
			}
		}
		if !missingDependency {
			break
		}
		if time.Now().After(readyDeadline) {
			t.Fatalf("readyz never reported all four dependency checks within the poll deadline: %s logs=%s stderr=%s",
				readiness, stdout.String(), stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	response, err := client.Get("http://" + address + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", response.StatusCode, http.StatusOK)
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
	// Python: max_retries=3 (four attempts) with a 30s first backoff, one
	// stream per binding, and horizontally scalable webhook processing.
	pagerdutyRunner := pagerdutyRunnerConfig(2)
	if pagerdutyRunner.ConsumerGroup != "pagerduty-webhook-consumers" ||
		!slices.Equal(pagerdutyRunner.Patterns, []string{"pagerduty-webhooks:*"}) ||
		pagerdutyRunner.MaxDeliveries != 4 ||
		pagerdutyRunner.ReclaimIdle != pagerduty.ReceiptLease+time.Minute ||
		pagerdutyRunner.ReclaimEvery != 30*time.Second ||
		pagerdutyRunner.Singleton || pagerdutyRunner.ConfiguredReplicas != 2 {
		t.Fatalf("pagerduty config = %#v", pagerdutyRunner)
	}
	// A reclaim must imply the previous holder can no longer own the receipt.
	// If the idle threshold ever drops below the lease, a reclaimed entry reads
	// as in-flight on every delivery and is dead-lettered without a single
	// reconciliation attempt.
	if pagerdutyRunner.ReclaimIdle <= pagerduty.ReceiptLease {
		t.Fatalf("reclaim idle %s does not exceed receipt lease %s",
			pagerdutyRunner.ReclaimIdle, pagerduty.ReceiptLease)
	}
}

// TestPagerDutyConsumerIsTheOnlyOwnerOfTheStream replaces
// TestPagerDutyConsumerRefusesToRaceCelery and
// TestPagerDutyTransportDefaultsToCelery (CHAOS-4105). Those two pinned a
// two-runtime ownership switch: while Python's Celery task also consumed
// pagerduty-webhooks:*, constructing a Go consumer alongside it let both
// reconcile one event and let Python XDEL an entry pending in the Go group,
// so exactly one runtime could own the stream and PAGERDUTY_WEBHOOK_TRANSPORT
// chose which.
//
// That task, the ingress .delay that fed it and the flag itself are deleted.
// There is one runtime, so the property worth pinning is the inverse: the
// handler must construct on its dependencies alone, never refuse on a
// configuration gate that no longer exists.
func TestPagerDutyConsumerIsTheOnlyOwnerOfTheStream(t *testing.T) {
	t.Parallel()
	storage := &productionStreamStorage{}
	_, err := storage.Handler(pagerdutyHandlerKind, nil)
	if errors.Is(err, streamrunner.ErrInvalidConfig) {
		t.Fatal("the pagerduty handler still refuses on an ownership gate")
	}
	// This fake storage has no ClickHouse connection and no domain pool, so
	// construction must still fail -- as a missing dependency, which is the
	// condition that keeps the readiness gate honest.
	if !errors.Is(err, errStreamDependencyUnavailable) {
		t.Fatalf("handler error = %v, want the dependency refusal", err)
	}
}
