package revisionfeed

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
)

// orderLog records the order of InvalidateAll calls and backoff waits.
type orderLog struct {
	mu    sync.Mutex
	steps []string
}

func (o *orderLog) add(step string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.steps = append(o.steps, step)
}

func (o *orderLog) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.steps)
}

// orderedRecorder is a recorder whose InvalidateAll also writes the order log.
type orderedRecorder struct {
	*recorder
	log *orderLog
}

func (o orderedRecorder) InvalidateAll(ctx context.Context) error {
	o.log.add("invalidate_all")
	return o.recorder.InvalidateAll(ctx)
}

// gatedHashClient holds HGetAll while gate is set, after it signals entered.
type gatedHashClient struct {
	*scriptedClient
	mu      sync.Mutex
	gate    chan struct{}
	entered chan struct{}
}

func (g *gatedHashClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	g.mu.Lock()
	gate := g.gate
	g.mu.Unlock()
	if gate != nil {
		g.entered <- struct{}{}
		<-gate
	}
	return g.scriptedClient.HGetAll(ctx, key)
}

func (g *gatedHashClient) hold() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gate = make(chan struct{})
}

func (g *gatedHashClient) release() {
	g.mu.Lock()
	gate := g.gate
	g.gate = nil
	g.mu.Unlock()
	close(gate)
}

func startSubscriberWith(t *testing.T, inv Invalidator, client Client, backoff func(int) time.Duration) *metric.ManualReader {
	t.Helper()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	consumer, err := NewConsumer(Config{Invalidator: inv, Logger: logger, Meter: provider.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := NewSubscriber(SubscriberConfig{Client: client, Consumer: consumer, Logger: logger, Meter: provider.Meter("test"), Backoff: backoff})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); sub.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after its context ended")
		}
	})
	return reader
}

// a49ea6fe: a failed InvalidateAll of a subscribe is tried again only after a backoff wait, never at once.
func TestAFailedResubscribeInvalidateAllWaitsTheBackoffBeforeItsSecondTry(t *testing.T) {
	log := &orderLog{}
	rec := &recorder{}
	rec.failNext(2)
	client := newScriptedClient()
	startSubscriberWith(t, orderedRecorder{rec, log}, client, func(int) time.Duration {
		log.add("backoff")
		return time.Millisecond
	})
	client.next(t).hooks.OnSubscribed()
	waitFor(t, "three steps", func() bool { return len(log.snapshot()) >= 3 })
	if got := log.snapshot()[:3]; !slices.Equal(got, []string{"invalidate_all", "backoff", "invalidate_all"}) {
		t.Fatalf("steps %q, want invalidate_all, backoff, invalidate_all: the second try came before the backoff", got)
	}
}

func allCalls(rec *recorder) int {
	n := 0
	for _, c := range rec.snapshot() {
		if c.all {
			n++
		}
	}
	return n
}

// b07906e3 item 1: a second confirmation inside one Subscribe call (the client reconnected) whose InvalidateAll fails
// takes the subscription down, so the gauge reads 0.
func TestASecondConfirmationWhoseInvalidateAllFailsTakesTheGaugeDown(t *testing.T) {
	rec := &recorder{}
	client := newScriptedClient()
	reader := startSubscriberWith(t, rec, client, func(int) time.Duration { return time.Millisecond })
	s := client.next(t)
	s.hooks.OnSubscribed()
	waitFor(t, "gauge 1", func() bool { return gaugeValue(t, reader) == 1 })
	rec.failNext(1 << 20)
	s.hooks.OnSubscribed()
	waitFor(t, "three InvalidateAll calls", func() bool { return allCalls(rec) >= 3 })
	if got := gaugeValue(t, reader); got != 0 {
		t.Fatalf("gauge %d while the second confirmation's InvalidateAll fails, want 0", got)
	}
}

// b07906e3 item 1: a catch-up of an older confirmation that ends after a newer confirmation does not raise the gauge.
func TestACatchUpOfAnOlderConfirmationDoesNotRaiseTheGauge(t *testing.T) {
	rec := &recorder{}
	client := &gatedHashClient{scriptedClient: newScriptedClient(), entered: make(chan struct{}, 1)}
	reader := startSubscriberWith(t, rec, client, func(int) time.Duration { return time.Millisecond })
	s := client.next(t)
	client.hold()
	s.hooks.OnSubscribed()
	select {
	case <-client.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first confirmation's catch-up never read the hash")
	}
	rec.failNext(1 << 20)
	s.hooks.OnSubscribed()
	client.release()
	waitFor(t, "the newer confirmation's InvalidateAll retried", func() bool { return allCalls(rec) >= 3 })
	if got := gaugeValue(t, reader); got != 0 {
		t.Fatalf("gauge %d: the older confirmation's catch-up raised it after the newer confirmation failed", got)
	}
}
