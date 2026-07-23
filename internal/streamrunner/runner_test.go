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
	stats       StreamStats
	closed      bool
}

func (*fakeTransport) EnsureGroup(context.Context, string, string) error { return nil }
func (f *fakeTransport) ReadNew(context.Context, string, string, string, int, time.Duration) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	return f.stats, nil
}
func (f *fakeTransport) Close() { f.mu.Lock(); defer f.mu.Unlock(); f.closed = true }

type handlerFunc func(context.Context, Message) error

func (h handlerFunc) Handle(ctx context.Context, message Message) error { return h(ctx, message) }

func testConfig() Config {
	return Config{Name: "stream_test", Streams: []string{"test:stream"}, ConsumerGroup: "group", ConsumerName: "consumer", BatchSize: 4, Block: 10 * time.Millisecond, ReclaimEvery: 10 * time.Millisecond, ReclaimIdle: 10 * time.Millisecond, MaxDeliveries: 3, ShutdownDrain: 100 * time.Millisecond}
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
	if err := runner.window(context.Background()); err != nil {
		t.Fatal(err)
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
