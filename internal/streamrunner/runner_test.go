package streamrunner

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type fakeTransport struct {
	mu          sync.Mutex
	new         []Message
	pending     []Pending
	claimed     []Message
	acked       []string
	quarantined []string
	discovered  []string
	ensured     []string
	readStreams [][]string
	readNotify  chan struct{}
	readDelay   time.Duration
	ackErr      error
	statCalls   int
	stats       StreamStats
	closed      bool
}

func (f *fakeTransport) EnsureGroup(_ context.Context, stream, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensured = append(f.ensured, stream)
	return nil
}
func (f *fakeTransport) Discover(context.Context, []string, int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.discovered...), nil
}
func (f *fakeTransport) ReadNew(ctx context.Context, streams []string, _ string, _ string, _ int, _ time.Duration) ([]Message, error) {
	if f.readDelay > 0 {
		timer := time.NewTimer(f.readDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readStreams = append(f.readStreams, append([]string(nil), streams...))
	if f.readNotify != nil {
		select {
		case f.readNotify <- struct{}{}:
		default:
		}
	}
	messages := f.new
	f.new = nil
	return messages, nil
}
func (f *fakeTransport) Pending(context.Context, string, string, int, time.Duration) ([]Pending, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Pending(nil), f.pending...), nil
}
func (f *fakeTransport) Claim(context.Context, string, string, string, []string, time.Duration) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	messages := f.claimed
	f.claimed = nil
	return messages, nil
}
func (f *fakeTransport) Ack(_ context.Context, _ string, _ string, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ackErr != nil {
		err := f.ackErr
		f.ackErr = nil
		return err
	}
	f.acked = append(f.acked, id)
	for index := range f.pending {
		if f.pending[index].MessageID == id {
			f.pending = append(f.pending[:index], f.pending[index+1:]...)
			break
		}
	}
	return nil
}
func (f *fakeTransport) Quarantine(_ context.Context, message Message, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.quarantined = append(f.quarantined, message.ID+":"+reason)
	return nil
}
func (f *fakeTransport) Stats(context.Context, string, string) (StreamStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statCalls++
	return f.stats, nil
}
func (f *fakeTransport) Close() { f.mu.Lock(); defer f.mu.Unlock(); f.closed = true }

type handlerFunc func(context.Context, Message) error

func (h handlerFunc) Handle(ctx context.Context, message Message) error { return h(ctx, message) }

func testConfig() Config {
	return Config{Name: "stream_test", Streams: []string{"test:stream"}, ConsumerGroup: "group", ConsumerName: "consumer", BatchSize: 4, Block: 10 * time.Millisecond, ReclaimEvery: 10 * time.Millisecond, ReclaimIdle: 10 * time.Millisecond, MaxDeliveries: 3, ShutdownDrain: 100 * time.Millisecond}
}

func dynamicTestConfig() Config {
	config := testConfig()
	config.Streams = nil
	config.Patterns = []string{"ingest:*:commits"}
	config.DiscoveryLimit = 32
	return config
}

func TestRunnerAcknowledgesOnlyAfterDurableHandler(t *testing.T) {
	transport := &fakeTransport{new: []Message{{Stream: "test:stream", ID: "1-0"}}, stats: StreamStats{Lag: 2}}
	registry := health.NewRegistry(time.Second)
	committed := false
	runner, err := New(transport, handlerFunc(func(context.Context, Message) error { committed = true; return nil }), testConfig(), registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.window(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("handler did not reach durable commit")
	}
	if !slices.Equal(transport.acked, []string{"1-0"}) {
		t.Fatalf("acks = %v", transport.acked)
	}
}

func TestRunnerLeavesTransientFailurePendingForReclaim(t *testing.T) {
	transport := &fakeTransport{new: []Message{{Stream: "test:stream", ID: "1-0"}}, stats: StreamStats{Pending: 1}}
	runner, err := New(transport, handlerFunc(func(context.Context, Message) error { return errors.New("clickhouse unavailable") }), testConfig(), health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.window(context.Background()); !errors.Is(err, errTransientWrite) {
		t.Fatalf("transient window error = %v", err)
	}
	if len(transport.acked) != 0 || len(transport.quarantined) != 0 {
		t.Fatalf("transient failure was terminal: acked=%v quarantine=%v", transport.acked, transport.quarantined)
	}

	transport.pending = []Pending{{MessageID: "1-0", TimesDelivered: 1, Idle: time.Second}}
	transport.claimed = []Message{{Stream: "test:stream", ID: "1-0"}}
	runner.handler = handlerFunc(func(context.Context, Message) error { return nil })
	if err := runner.reclaim(context.Background(), "test:stream"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(transport.acked, []string{"1-0"}) {
		t.Fatalf("reclaimed durable message not acked: %v", transport.acked)
	}
}

func TestRunnerDiscoversStreamsEnsuresGroupsAndRemovesStaleLanes(t *testing.T) {
	transport := &fakeTransport{
		discovered: []string{"ingest:org-b:commits", "ingest:org-a:commits"},
		stats:      StreamStats{Lag: 1},
	}
	runner, err := New(transport, handlerFunc(func(context.Context, Message) error { return nil }), dynamicTestConfig(), health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.window(context.Background()); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	if got, want := transport.readStreams[0], []string{"ingest:org-a:commits", "ingest:org-b:commits"}; !slices.Equal(got, want) {
		t.Fatalf("read lanes = %v want %v", got, want)
	}
	transport.discovered = []string{"ingest:org-b:commits"}
	transport.mu.Unlock()
	if err := runner.window(context.Background()); err != nil {
		t.Fatal(err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if !slices.Equal(runner.streams, []string{"ingest:org-b:commits"}) {
		t.Fatalf("active lanes = %v", runner.streams)
	}
	if _, stale := runner.lastStats["ingest:org-a:commits"]; stale {
		t.Fatal("stale lane metrics were not removed")
	}
}

func TestRunnerDoesNotStarveLaterLaneAfterTransientSinkFailure(t *testing.T) {
	transport := &fakeTransport{
		new: []Message{
			{Stream: "test:a", ID: "1-0"},
			{Stream: "test:b", ID: "2-0"},
		},
	}
	config := testConfig()
	config.Streams = []string{"test:a", "test:b"}
	runner, err := New(transport, handlerFunc(func(_ context.Context, message Message) error {
		if message.Stream == "test:a" {
			return errors.New("sink unavailable")
		}
		return nil
	}), config, health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.window(context.Background()); !errors.Is(err, errTransientWrite) {
		t.Fatalf("window error = %v", err)
	}
	if !slices.Equal(transport.acked, []string{"2-0"}) {
		t.Fatalf("later lane was starved, acks = %v", transport.acked)
	}
	if runner.ready.Load() {
		t.Fatal("transient sink failure left readiness open")
	}
}

func TestRunnerReadsContinuouslyBetweenReclaimCadences(t *testing.T) {
	transport := &fakeTransport{readNotify: make(chan struct{}, 10), readDelay: time.Millisecond}
	config := testConfig()
	config.ReclaimEvery = 100 * time.Millisecond
	config.ReclaimIdle = 100 * time.Millisecond
	runner, err := New(transport, handlerFunc(func(context.Context, Message) error { return nil }), config, health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		select {
		case <-transport.readNotify:
		case <-time.After(40 * time.Millisecond):
			t.Fatal("successful reads waited for reclaim cadence")
		}
	}
	transport.mu.Lock()
	if transport.statCalls != 1 {
		t.Fatalf("continuous reads performed full stats sweeps: calls=%d", transport.statCalls)
	}
	transport.mu.Unlock()
	cancel()
	if err := runner.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRestartReclaimsPendingAndAckFailureReplaysDurableWrite(t *testing.T) {
	transport := &fakeTransport{
		new:    []Message{{Stream: "test:stream", ID: "1-0"}},
		ackErr: errors.New("valkey unavailable after commit"),
	}
	commits := 0
	handler := handlerFunc(func(context.Context, Message) error {
		commits++
		return nil
	})
	first, err := New(transport, handler, testConfig(), health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.window(context.Background()); err == nil {
		t.Fatal("ACK failure was treated as success")
	}
	transport.pending = []Pending{{MessageID: "1-0", TimesDelivered: 1, Idle: time.Second}}
	transport.claimed = []Message{{Stream: "test:stream", ID: "1-0"}}
	secondConfig := testConfig()
	secondConfig.Name = "stream_test_restart"
	second, err := New(transport, handler, secondConfig, health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.window(context.Background()); err != nil {
		t.Fatal(err)
	}
	if commits != 2 {
		t.Fatalf("durable idempotent write count = %d, want replay", commits)
	}
	if !slices.Equal(transport.acked, []string{"1-0"}) {
		t.Fatalf("restart did not ACK replay: %v", transport.acked)
	}
}

func TestRunnerQuarantinesPoisonOnlyAfterQuarantineWrite(t *testing.T) {
	transport := &fakeTransport{new: []Message{{Stream: "test:stream", ID: "1-0"}}}
	runner, err := New(transport, handlerFunc(func(context.Context, Message) error { return &PermanentError{Reason: "schema_invalid"} }), testConfig(), health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.window(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(transport.quarantined, []string{"1-0:schema_invalid"}) || !slices.Equal(transport.acked, []string{"1-0"}) {
		t.Fatalf("poison outcome = quarantine=%v ack=%v", transport.quarantined, transport.acked)
	}
}

func TestRunnerReclaimsPoisonDeliveryCountAndExportsBoundedMetrics(t *testing.T) {
	transport := &fakeTransport{pending: []Pending{{MessageID: "1-0", TimesDelivered: 3, Idle: time.Hour}}, stats: StreamStats{Lag: 5, Pending: 1, OldestPending: time.Minute}}
	runner, err := New(transport, handlerFunc(func(context.Context, Message) error { return nil }), testConfig(), health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.reclaim(context.Background(), "test:stream"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(transport.quarantined, []string{"1-0:max_deliveries_exceeded"}) || !slices.Equal(transport.acked, []string{"1-0"}) {
		t.Fatalf("poison reclaim = %v %v", transport.quarantined, transport.acked)
	}
	if err := runner.window(context.Background()); err != nil {
		t.Fatal(err)
	}
	var metrics bytes.Buffer
	if err := runner.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"worker_stream_lag 5", "worker_stream_pending 1", "worker_stream_oldest_pending_seconds 60", "worker_stream_quarantined_total 1"} {
		if !bytes.Contains(metrics.Bytes(), []byte(want+"\n")) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
	if bytes.Contains(metrics.Bytes(), []byte("test:stream")) {
		t.Fatalf("metrics leak stream identity: %s", metrics.String())
	}
}

func TestExternalSingletonRejectsDuplicateReplicaConfig(t *testing.T) {
	config := testConfig()
	config.Singleton = true
	config.ConfiguredReplicas = 2
	if _, err := New(&fakeTransport{}, handlerFunc(func(context.Context, Message) error { return nil }), config, health.NewRegistry(time.Second)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("duplicate singleton configuration error = %v", err)
	}
}

func TestShutdownClosesReadinessAndLeavesUncommittedMessagePending(t *testing.T) {
	transport := &fakeTransport{}
	registry := health.NewRegistry(time.Second)
	runner, err := New(transport, handlerFunc(func(ctx context.Context, _ Message) error { <-ctx.Done(); return ctx.Err() }), testConfig(), registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runner.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("shutdown readiness = %#v", readiness)
	}
	if !transport.closed || len(transport.acked) != 0 {
		t.Fatalf("shutdown should close transport without ack: closed=%v acked=%v", transport.closed, transport.acked)
	}
}
