package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/externalrecompute"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

type failingPendingScopesDispatcher struct{ err error }

func (dispatcher failingPendingScopesDispatcher) Dispatch(context.Context, externalrecompute.Claim) error {
	return nil
}

func (dispatcher failingPendingScopesDispatcher) PendingScopes(
	context.Context, int,
) ([]streamhandlers.ExternalRecomputeScope, error) {
	return nil, dispatcher.err
}

type noopRecomputeStore struct{}

func (noopRecomputeStore) Coalesce(context.Context, streamhandlers.ExternalRecomputeScope, time.Time, time.Duration) error {
	return nil
}

func (noopRecomputeStore) ClaimDue(context.Context, time.Time, int, time.Duration) ([]externalrecompute.Claim, error) {
	return nil, nil
}

func (noopRecomputeStore) Complete(context.Context, externalrecompute.Claim) error { return nil }

// TestExternalRecomputeControllerReceivesTheComposedLogger covers the seam
// cmd/dev-health-stream-runner uses to construct this controller, rather than
// calling externalrecompute.New directly with a hand-built Config -- which
// would prove nothing about cmd/. Before this wiring, externalrecompute had
// zero slog references at all: a failed step was discarded outright
// (`_ = controller.step(ctx)`) with no counter and no readiness flip.
//
// KNOWN COVERAGE BOUNDARY (CHAOS-3907): this calls newExternalRecomputeController
// itself, so it proves the seam propagates its logger argument -- it does NOT
// prove the production call site passes the composed logger INTO the seam.
// That call site is productionStreamStorage.Handler's external-ingest case
// (`newExternalRecomputeController(recomputeStore, dispatcher, storage.logger)`),
// which sits behind live ClickHouse/Valkey/Postgres construction and is not
// reachable without containers. Verified by mutation: replacing that argument
// with nil does NOT fail any test, whereas every other logger wiring site in
// this batch does fail under the same treatment. If that line is ever changed,
// nothing here will catch it -- close this with an integration-tagged test if
// the controller's logging becomes load-bearing.
func TestExternalRecomputeControllerReceivesTheComposedLogger(t *testing.T) {
	forcedErr := errors.New("pending scopes probe failure")

	var buf syncBuffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	controller, err := newExternalRecomputeController(
		noopRecomputeStore{},
		failingPendingScopesDispatcher{err: forcedErr},
		logger,
	)
	if err != nil {
		t.Fatalf("newExternalRecomputeController() error = %v", err)
	}
	if err := controller.Start(context.Background()); err != nil {
		t.Fatalf("start controller: %v", err)
	}
	t.Cleanup(func() { _ = controller.Shutdown(context.Background()) })

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(buf.String(), "external recompute step failed") {
		time.Sleep(5 * time.Millisecond)
	}

	output := buf.String()
	if !strings.Contains(output, "external recompute step failed") {
		t.Fatalf("composed logger did not observe the controller's step failure: %s", output)
	}
	if !strings.Contains(output, forcedErr.Error()) {
		t.Fatalf("composed logger output omitted the failure cause: %s", output)
	}
}

// failingReadTransport discovers one stream successfully (so Start's initial
// refreshStreams succeeds and the runner goroutine actually starts) but fails
// every read, so the runner's background loop hits a cycle failure on its
// first iteration.
type failingReadTransport struct {
	readErr error
}

func (*failingReadTransport) EnsureGroup(context.Context, string, string) error { return nil }
func (t *failingReadTransport) ReadNew(context.Context, []string, string, string, int, time.Duration) ([]streamrunner.Message, error) {
	return nil, t.readErr
}
func (*failingReadTransport) Pending(context.Context, string, string, int, time.Duration) ([]streamrunner.Pending, error) {
	return nil, nil
}
func (*failingReadTransport) Claim(context.Context, string, string, string, []string, time.Duration) ([]streamrunner.Message, error) {
	return nil, nil
}
func (*failingReadTransport) Ack(context.Context, string, string, string) error { return nil }
func (*failingReadTransport) Quarantine(context.Context, streamrunner.Message, string) error {
	return nil
}
func (*failingReadTransport) Stats(context.Context, string, string) (streamrunner.StreamStats, error) {
	return streamrunner.StreamStats{}, nil
}
func (*failingReadTransport) Discover(context.Context, []string, int) ([]string, error) {
	return []string{"probe-stream"}, nil
}
func (*failingReadTransport) Close() {}

type failingReadStorage struct {
	readErr error
}

func (*failingReadStorage) ClickHouseReady(context.Context) error     { return nil }
func (*failingReadStorage) DomainPostgresReady(context.Context) error { return nil }
func (*failingReadStorage) ValkeyReady(context.Context) error         { return nil }
func (*failingReadStorage) Handler(streamHandlerKind) (streamrunner.Handler, error) {
	return streamCommandHandler{}, nil
}
func (storage *failingReadStorage) NewTransport() (streamrunner.Transport, error) {
	return &failingReadTransport{readErr: storage.readErr}, nil
}
func (*failingReadStorage) ControlComponents() []lifecycle.Component { return nil }
func (*failingReadStorage) Close()                                   {}

// TestStreamRunnerReceivesTheComposedLoggerFromStreamRunnerCompositionRoot
// proves the wiring, not just the field (CHAOS-3907): it goes through the
// real composition root (configureStreamRunnerDependenciesWithSources with a
// real streamrunner.New), not a direct
// streamrunner.New(..., Config{Logger: ...}) call, which would prove nothing
// about cmd/dev-health-stream-runner. Before this wiring, a cycle failure
// flipped ready=false with no output and no Errors() channel at all.
func TestStreamRunnerReceivesTheComposedLoggerFromStreamRunnerCompositionRoot(t *testing.T) {
	forcedErr := errors.New("stream read probe failure")
	storage := &failingReadStorage{readErr: forcedErr}

	var buf syncBuffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

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
		logger,
	)
	if err != nil {
		t.Fatalf("configureStreamRunnerDependenciesWithSources() error = %v", err)
	}

	var runner lifecycle.Component
	for _, component := range components {
		if component.Name() == "stream-internal_ingest" {
			runner = component
		}
	}
	if runner == nil {
		t.Fatalf("stream-internal_ingest was not constructed: components=%v", componentNames(components))
	}
	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("start runner: %v", err)
	}
	t.Cleanup(func() { _ = runner.Shutdown(context.Background()) })

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(buf.String(), "stream runner cycle failed") {
		time.Sleep(5 * time.Millisecond)
	}

	output := buf.String()
	if !strings.Contains(output, "stream runner cycle failed") {
		t.Fatalf("composed logger did not observe the runner's cycle failure: %s", output)
	}
	if !strings.Contains(output, forcedErr.Error()) {
		t.Fatalf("composed logger output omitted the failure cause: %s", output)
	}
}

func componentNames(components []lifecycle.Component) []string {
	names := make([]string, 0, len(components))
	for _, component := range components {
		names = append(names, component.Name())
	}
	return names
}

// syncBuffer guards the capture buffer with a mutex. These tests start a real
// component whose run loop logs from its own goroutine while the test polls
// for the expected line, so an unguarded bytes.Buffer is a data race that
// -race fails on (CHAOS-3907). slog handlers are safe for concurrent use; the
// io.Writer underneath them is the caller's responsibility.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
