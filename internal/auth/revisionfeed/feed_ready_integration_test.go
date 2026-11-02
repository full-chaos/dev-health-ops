//go:build integration

package revisionfeed

import (
	"context"
	"testing"
	"time"

	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
)

// PLAN-10842-C cell (f): on a real Valkey, a dropped subscription makes the subscriber not ready (the decision cache is
// then not asked), and the reconnect runs InvalidateAll before it is ready again.
func TestADroppedSubscriptionIsNotReadyUntilTheReconnectClearedTheCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	_, admin, instance := startValkeyClient(t, ctx)
	replicaRaw, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(replicaRaw.Close)
	rec := &recorder{}
	consumer, err := NewConsumer(Config{Invalidator: slowClear{rec}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := NewSubscriber(SubscriberConfig{Client: NewClient(replicaRaw), Consumer: consumer, Backoff: func(int) time.Duration { return 2 * time.Second }})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go sub.Run(runCtx)
	until := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	until("ready", sub.Ready)
	if n := countAll(rec); n != 1 {
		t.Fatalf("%d InvalidateAll before the first ready, want 1", n)
	}
	if err := admin.Do(ctx, admin.B().Arbitrary("CLIENT", "KILL", "TYPE", "pubsub").Build()).Error(); err != nil {
		t.Fatalf("client kill: %v", err)
	}
	until("not ready after the subscription dropped", func() bool { return !sub.Ready() })
	until("ready again", sub.Ready)
	if n := countAll(rec); n < 2 {
		t.Fatalf("%d InvalidateAll, want a second one before ready again", n)
	}
}

// slowClear holds every InvalidateAll after the first for 500 ms, so the not-ready window of a reconnect is long enough
// to observe whether the client reconnects inside one Subscribe call or the Run loop does.
type slowClear struct{ *recorder }

func (s slowClear) InvalidateAll(ctx context.Context) error {
	if countAll(s.recorder) >= 1 {
		time.Sleep(500 * time.Millisecond)
	}
	return s.recorder.InvalidateAll(ctx)
}
