package revisionfeed

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type session struct {
	hooks SubscribeHooks
	done  chan error
}

// scriptedClient hands each Subscribe call to the test, which drives it.
type scriptedClient struct {
	fakeClient
	sessions chan *session
}

func newScriptedClient() *scriptedClient {
	c := &scriptedClient{sessions: make(chan *session, 8)}
	c.subscribe = func(ctx context.Context, _ string, hooks SubscribeHooks) error {
		s := &session{hooks: hooks, done: make(chan error, 1)}
		c.sessions <- s
		select {
		case err := <-s.done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c
}

func (c *scriptedClient) next(t *testing.T) *session {
	t.Helper()
	select {
	case s := <-c.sessions:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("no subscribe call")
		return nil
	}
}

func gaugeValue(t *testing.T, reader *metric.ManualReader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "devhealth_auth_revision_feed_subscription_up" {
				points := m.Data.(metricdata.Gauge[int64]).DataPoints
				if len(points) != 1 || points[0].Attributes.Len() != 0 {
					t.Fatalf("the gauge has %d points or a label", len(points))
				}
				return points[0].Value
			}
		}
	}
	t.Fatal("no subscription_up gauge")
	return -1
}

type subscriberRig struct {
	sub    *Subscriber
	client *scriptedClient
	rec    *recorder
	reader *metric.ManualReader
	cancel context.CancelFunc
	done   chan struct{}
	logs   *bytes.Buffer
}

func startSubscriber(t *testing.T) *subscriberRig {
	t.Helper()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	rec := &recorder{}
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	consumer, err := NewConsumer(Config{Invalidator: rec, Logger: logger, Meter: provider.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	client := newScriptedClient()
	sub, err := NewSubscriber(SubscriberConfig{
		Client: client, Consumer: consumer, Logger: logger, Meter: provider.Meter("test"),
		Backoff: func(int) time.Duration { return time.Millisecond },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rig := &subscriberRig{sub: sub, client: client, rec: rec, reader: reader, cancel: cancel, done: make(chan struct{}), logs: logs}
	go func() { defer close(rig.done); sub.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-rig.done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after its context ended")
		}
	})
	return rig
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSubscriberResubscribesTheConsumerAndRaisesTheGaugeOnEverySubscribe(t *testing.T) {
	rig := startSubscriber(t)
	first := rig.client.next(t)
	if got := gaugeValue(t, rig.reader); got != 0 {
		t.Fatalf("gauge %d before the subscription is confirmed, want 0", got)
	}
	first.hooks.OnSubscribed()
	waitFor(t, "gauge 1", func() bool { return gaugeValue(t, rig.reader) == 1 })
	if all := countAll(rig.rec); all != 1 {
		t.Fatalf("%d InvalidateAll after the first subscribe, want 1", all)
	}
	first.done <- errors.New("connection reset by SENTINEL_URI")
	waitFor(t, "gauge back to 0", func() bool { return gaugeValue(t, rig.reader) == 0 })
	second := rig.client.next(t)
	second.hooks.OnSubscribed()
	waitFor(t, "gauge 1 again", func() bool { return gaugeValue(t, rig.reader) == 1 })
	waitFor(t, "second InvalidateAll", func() bool { return countAll(rig.rec) == 2 })
}

func countAll(r *recorder) int {
	n := 0
	for _, c := range r.snapshot() {
		if c.all {
			n++
		}
	}
	return n
}

func TestSubscriberFeedsEveryMessageToTheConsumer(t *testing.T) {
	rig := startSubscriber(t)
	s := rig.client.next(t)
	s.hooks.OnSubscribed()
	s.hooks.OnMessage("principal:prn_1:1")
	s.hooks.OnMessage("principal:prn_1:1") // a duplicate
	s.hooks.OnMessage("principal:prn_1:3") // a gap
	s.hooks.OnMessage("user:a:b:c:2")      // an id with colons: type is up to the first colon, revision after the last
	waitFor(t, "three Invalidate calls", func() bool {
		n := 0
		for _, c := range rig.rec.snapshot() {
			if !c.all {
				n++
			}
		}
		return n == 3
	})
	var seen []call
	for _, c := range rig.rec.snapshot() {
		if !c.all {
			seen = append(seen, c)
		}
	}
	want := []call{{typ: "principal", id: "prn_1", revision: 1}, {typ: "principal", id: "prn_1", revision: 3}, {typ: "user", id: "a:b:c", revision: 2}}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("call %d = %+v, want %+v", i, seen[i], want[i])
		}
	}
}

func TestSubscriberDropsAMalformedMessageWithoutInvalidatingOrLoggingIt(t *testing.T) {
	rig := startSubscriber(t)
	s := rig.client.next(t)
	s.hooks.OnSubscribed()
	waitFor(t, "InvalidateAll", func() bool { return countAll(rig.rec) == 1 })
	for _, bad := range []string{"", "nocolons", "principal:SENTINEL_BODY", "principal:prn_1:notanumber", "principal:prn_1:0", "principal:prn_1:-4", ":prn_1:2", "principal::2"} {
		s.hooks.OnMessage(bad)
	}
	s.hooks.OnMessage("principal:prn_2:1")
	waitFor(t, "the one valid message", func() bool { return len(rig.rec.snapshot()) == 2 })
	if n := len(rig.rec.snapshot()); n != 2 {
		t.Fatalf("%d calls: a malformed message invalidated something", n)
	}
	if strings.Contains(rig.logs.String(), "SENTINEL_BODY") {
		t.Fatalf("a log line holds a message body:\n%s", rig.logs.String())
	}
}

func TestSubscriberLogsNeverCarryTheConnectionErrorText(t *testing.T) {
	rig := startSubscriber(t)
	s := rig.client.next(t)
	s.hooks.OnSubscribed()
	s.done <- errors.New("dial redis://user:SENTINEL_PASSWORD@host:6379/1")
	rig.client.next(t)
	if strings.Contains(rig.logs.String(), "SENTINEL_PASSWORD") || strings.Contains(rig.logs.String(), "redis://") {
		t.Fatalf("a log line holds the connection error:\n%s", rig.logs.String())
	}
	if !strings.Contains(rig.logs.String(), "subscription") {
		t.Fatalf("nothing logged about the lost subscription:\n%s", rig.logs.String())
	}
}

func TestSubscriberStopsWhenItsContextEnds(t *testing.T) {
	rig := startSubscriber(t)
	s := rig.client.next(t)
	s.hooks.OnSubscribed()
	waitFor(t, "gauge 1", func() bool { return gaugeValue(t, rig.reader) == 1 })
	rig.cancel()
	select {
	case <-rig.done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	if got := gaugeValue(t, rig.reader); got != 0 {
		t.Fatalf("gauge %d after stop, want 0", got)
	}
}

var _ sync.Locker = (*sync.Mutex)(nil)
