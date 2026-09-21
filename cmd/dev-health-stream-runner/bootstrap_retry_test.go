package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

// outageStreamStorage answers each dependency check from a per-check budget
// of failures: a check fails until its budget is spent, then passes for good.
// That is the shape of a dependency that is down when the process starts and
// comes back later without the process being restarted.
type outageStreamStorage struct {
	clickHouseFailures atomic.Int64
	domainFailures     atomic.Int64
	postureFailures    atomic.Int64
	valkeyFailures     atomic.Int64

	handlers   atomic.Int64
	transports atomic.Int64
	closed     atomic.Int64
}

func spend(budget *atomic.Int64) error {
	if budget.Add(-1) >= 0 {
		return errors.New("dependency is down")
	}
	return nil
}

func (storage *outageStreamStorage) ClickHouseReady(context.Context) error {
	return spend(&storage.clickHouseFailures)
}

func (storage *outageStreamStorage) DomainPostgresReady(context.Context) error {
	return spend(&storage.domainFailures)
}

func (storage *outageStreamStorage) PostureManifestLockstep(
	context.Context, string,
) (postgres.PostureManifestLockstepResult, error) {
	if err := spend(&storage.postureFailures); err != nil {
		return postgres.PostureManifestLockstepResult{}, err
	}
	return postgres.PostureManifestLockstepResult{Lockstep: true}, nil
}

func (storage *outageStreamStorage) ValkeyReady(context.Context) error {
	return spend(&storage.valkeyFailures)
}

func (storage *outageStreamStorage) Handler(streamHandlerKind, streamhandlers.ExternalIngestObserver) (streamrunner.Handler, error) {
	storage.handlers.Add(1)
	return streamCommandHandler{}, nil
}

func (storage *outageStreamStorage) NewTransport() (streamrunner.Transport, error) {
	storage.transports.Add(1)
	return &streamCommandTransport{}, nil
}

func (*outageStreamStorage) ControlComponents() []lifecycle.Component { return nil }
func (storage *outageStreamStorage) Close()                           { storage.closed.Add(1) }

// runStreamComponents starts the configured components and the readiness gate
// the way lifecycle.Runtime does, and returns a stop function that shuts them
// down in reverse order.
func runStreamComponents(t *testing.T, registry *health.Registry, components []lifecycle.Component) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	all := append(append([]lifecycle.Component(nil), components...), health.Gate{Registry: registry})
	started := make([]lifecycle.Component, 0, len(all))
	for _, component := range all {
		if err := component.Start(ctx); err != nil {
			cancel()
			t.Fatalf("start %s: %v", component.Name(), err)
		}
		started = append(started, component)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			// The start context stays open until every component has shut
			// down: the runtime also stops components after one of them
			// fails, when no signal has cancelled anything, so Shutdown must
			// stop background work on its own.
			defer cancel()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			for index := len(started) - 1; index >= 0; index-- {
				if err := started[index].Shutdown(shutdownCtx); err != nil {
					t.Errorf("shutdown %s: %v", started[index].Name(), err)
				}
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func waitForReadiness(t *testing.T, registry *health.Registry, within time.Duration) health.Readiness {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		status := registry.Readiness(context.Background())
		if status.Ready {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("readiness never recovered within %s: failed=%v", within, status.Failed)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestStreamRunnerReadinessRecoversAfterAStartupDependencyOutage pins the
// property the stream runner exists to keep: a dependency that is down while
// the process starts, and comes back later, leaves the process ready and
// consuming without a restart. Each case takes one dependency (or storage
// construction itself) down at start-up.
func TestStreamRunnerReadinessRecoversAfterAStartupDependencyOutage(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		openFailures int64
		nilOpen      bool
		arm          func(*outageStreamStorage)
	}{
		{name: "storage construction", openFailures: 2, arm: func(*outageStreamStorage) {}},
		{name: "storage construction returning nothing", openFailures: 2, nilOpen: true, arm: func(*outageStreamStorage) {}},
		{name: "clickhouse", arm: func(storage *outageStreamStorage) { storage.clickHouseFailures.Store(2) }},
		{name: "domain_postgres", arm: func(storage *outageStreamStorage) { storage.domainFailures.Store(2) }},
		{name: "posture_manifest_lockstep", arm: func(storage *outageStreamStorage) { storage.postureFailures.Store(2) }},
		{name: "valkey", arm: func(storage *outageStreamStorage) { storage.valkeyFailures.Store(2) }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			storage := &outageStreamStorage{}
			testCase.arm(storage)
			var opens atomic.Int64
			var logs syncBuffer
			registry := health.NewRegistry(100 * time.Millisecond)
			components, err := configureStreamRunnerDependenciesWithSources(
				context.Background(),
				config.Config{Profile: "ingest", StreamConfiguredReplicas: 1},
				registry,
				streamDependencySources{
					openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
						if opens.Add(1) <= testCase.openFailures {
							if testCase.nilOpen {
								return nil, nil
							}
							return nil, dependencyUnavailable("stream_clickhouse_open_failed")
						}
						return storage, nil
					},
				},
				slog.New(slog.NewJSONHandler(&logs, nil)),
			)
			if err != nil {
				t.Fatalf("a start-up dependency outage must not stop the process: %v", err)
			}
			runStreamComponents(t, registry, components)

			status := waitForReadiness(t, registry, 15*time.Second)
			want := []string{
				"clickhouse", "domain_postgres", "internal_ingest_loop", "posture_manifest_lockstep",
				"product_telemetry_loop", "stream_consumer", "valkey",
			}
			var names []string
			for _, check := range status.Checks {
				names = append(names, check.Name)
			}
			if !slices.Equal(names, want) {
				t.Fatalf("ready checks = %v, want %v", names, want)
			}
			// One storage, one consumer set: a retry never builds a second
			// consumer beside the first.
			if got := opens.Load(); got != testCase.openFailures+1 {
				t.Fatalf("storage opened %d times, want %d", got, testCase.openFailures+1)
			}
			if storage.handlers.Load() != 2 || storage.transports.Load() != 2 {
				t.Fatalf("handlers=%d transports=%d, want one per ingest loop", storage.handlers.Load(), storage.transports.Load())
			}
			output := logs.String()
			if testCase.openFailures > 0 {
				reason := `"reason":"stream_clickhouse_open_failed"`
				if testCase.nilOpen {
					reason = `"reason":"stream_storage_open_failed"`
				}
				if !strings.Contains(output, reason) {
					t.Fatalf("log is missing %s:\n%s", reason, output)
				}
			}
			for _, line := range []string{"stream runner start-up attempt failed", "stream runner dependencies recovered", "stream consumers started"} {
				if !strings.Contains(output, line) {
					t.Fatalf("log is missing %q:\n%s", line, output)
				}
			}
		})
	}
}

// TestStreamRunnerShutdownDuringAStartupOutageIsClean pins that a process told
// to stop while its dependencies are still down stops promptly, closes nothing
// it did not open, and makes no further attempt after shutdown returns.
func TestStreamRunnerShutdownDuringAStartupOutageIsClean(t *testing.T) {
	var opens atomic.Int64
	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureStreamRunnerDependenciesWithSources(
		context.Background(),
		config.Config{Profile: "ingest", StreamConfiguredReplicas: 1},
		registry,
		streamDependencySources{
			openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
				opens.Add(1)
				return nil, dependencyUnavailable("stream_valkey_open_failed")
			},
		},
		nil,
	)
	if err != nil {
		t.Fatalf("a start-up dependency outage must not stop the process: %v", err)
	}
	stop := runStreamComponents(t, registry, components)
	deadline := time.Now().Add(5 * time.Second)
	for opens.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if opens.Load() == 0 {
		t.Fatal("storage was never attempted")
	}
	status := registry.Readiness(context.Background())
	want := []string{"clickhouse", "domain_postgres", "posture_manifest_lockstep", "stream_consumer", "valkey"}
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness during the outage = %#v, want failed %v", status, want)
	}
	began := time.Now()
	stop()
	if elapsed := time.Since(began); elapsed > 2*time.Second {
		t.Fatalf("shutdown took %s during a start-up outage", elapsed)
	}
	after := opens.Load()
	time.Sleep(1500 * time.Millisecond)
	if opens.Load() != after {
		t.Fatalf("storage attempted %d more times after shutdown returned", opens.Load()-after)
	}
}

// waitForSupervisor waits until the supervisor's background start-up has
// finished, whatever its outcome.
func waitForSupervisor(t *testing.T, supervisor *streamConsumerSupervisor) {
	t.Helper()
	supervisor.mu.Lock()
	done := supervisor.done
	supervisor.mu.Unlock()
	if done == nil {
		t.Fatal("supervisor was never started")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor start-up did not finish")
	}
}

// TestStreamStorageOpenFailuresSeparateConfigurationFromOutage pins which
// storage failures the start-up loop retries: an unreachable dependency is
// retried, a configuration the driver rejects is not.
func TestStreamStorageOpenFailuresSeparateConfigurationFromOutage(t *testing.T) {
	for _, testCase := range []struct {
		err       error
		retryable bool
	}{
		{err: openFailure("stream_clickhouse_open_failed", clickhouse.ErrUnavailable), retryable: true},
		{err: openFailure("stream_clickhouse_open_failed", clickhouse.ErrInvalidConfig), retryable: false},
		{err: openFailure("stream_valkey_open_failed", valkey.ErrUnavailable), retryable: true},
		{err: openFailure("stream_valkey_open_failed", valkey.ErrInvalidConfig), retryable: false},
		{err: openFailure("stream_domain_postgres_open_failed", postgres.ErrUnavailable), retryable: true},
		{err: openFailure("stream_domain_postgres_open_failed", postgres.ErrInvalidConfig), retryable: false},
		{err: openFailure("stream_domain_postgres_open_failed", fmt.Errorf("wrapped: %w", postgres.ErrDomainDatabaseRequired)), retryable: false},
		{err: dependencyMisconfigured("stream_storage_uris_unconfigured"), retryable: false},
		{err: dependencyMisconfigured("stream_credential_cipher_unconfigured"), retryable: false},
		{err: startupProbeFailure{checks: []string{"valkey"}}, retryable: true},
		{err: errors.New("unclassified"), retryable: true},
	} {
		if got := startupRetryable(testCase.err); got != testCase.retryable {
			t.Errorf("startupRetryable(%v) = %v, want %v", testCase.err, got, testCase.retryable)
		}
	}
}

// failingStartStorage opens and passes every probe, but its consumers cannot
// be built or started as configured.
type failingStartStorage struct {
	outageStreamStorage
	handlerErr  error
	discoverErr error
}

func (storage *failingStartStorage) Handler(kind streamHandlerKind, observer streamhandlers.ExternalIngestObserver) (streamrunner.Handler, error) {
	if storage.handlerErr != nil {
		return nil, storage.handlerErr
	}
	return storage.outageStreamStorage.Handler(kind, observer)
}

func (storage *failingStartStorage) NewTransport() (streamrunner.Transport, error) {
	storage.transports.Add(1)
	return &discoverFailingTransport{err: storage.discoverErr}, nil
}

type discoverFailingTransport struct {
	streamCommandTransport
	err error
}

func (transport *discoverFailingTransport) Discover(context.Context, []string, int) ([]string, error) {
	return nil, transport.err
}

// TestStreamConsumerFailuresAfterStartupReachTheRuntime pins that a consumer
// the runner refuses, or one that will not start, is reported on Errors() so
// the runtime stops the process, and that no consumer is left running.
func TestStreamConsumerFailuresAfterStartupReachTheRuntime(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		storage *failingStartStorage
		want    error
	}{
		{name: "refused configuration", storage: &failingStartStorage{handlerErr: streamrunner.ErrInvalidConfig}, want: streamrunner.ErrInvalidConfig},
		{name: "consumer start", storage: &failingStartStorage{discoverErr: errors.New("discovery down")}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			registry := health.NewRegistry(100 * time.Millisecond)
			components, err := configureStreamRunnerDependenciesWithSources(
				context.Background(),
				config.Config{Profile: "ingest", StreamConfiguredReplicas: 1},
				registry,
				streamDependencySources{
					openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
						return testCase.storage, nil
					},
				},
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			supervisor := components[0].(*streamConsumerSupervisor)
			stop := runStreamComponents(t, registry, components)
			select {
			case failure := <-supervisor.Errors():
				if testCase.want != nil && !errors.Is(failure, testCase.want) {
					t.Fatalf("failure = %v, want %v", failure, testCase.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("no failure reached the runtime")
			}
			if err := supervisor.consumersReady(context.Background()); err == nil {
				t.Fatal("stream_consumer reports ready after a consumer failure")
			}
			stop()
			if testCase.storage.closed.Load() != 1 {
				t.Fatalf("storage closed %d times, want once", testCase.storage.closed.Load())
			}
		})
	}
}

func TestStreamConsumerSupervisorStartsOnceAndReservesDrain(t *testing.T) {
	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureStreamRunnerDependenciesWithSources(
		context.Background(),
		config.Config{Profile: "ingest", StreamConfiguredReplicas: 1},
		registry,
		streamDependencySources{
			openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
				return &outageStreamStorage{}, nil
			},
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	supervisor := components[0].(*streamConsumerSupervisor)
	runStreamComponents(t, registry, components)
	if err := supervisor.Start(context.Background()); !errors.Is(err, errStreamConsumersAlreadyStarted) {
		t.Fatalf("second Start = %v, want %v", err, errStreamConsumersAlreadyStarted)
	}
	want := internalIngestRunnerConfig(1).ShutdownDrain + productTelemetryRunnerConfig(1).ShutdownDrain
	if got := supervisor.ShutdownBudget(); got != want {
		t.Fatalf("shutdown budget = %s, want %s", got, want)
	}
}
