package apimetrics

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type forceFlusher interface {
	ForceFlush(context.Context) error
}

func sumValues(c *collector, name string) []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var values []int64
	for _, m := range c.metrics {
		if m.GetName() != name {
			continue
		}
		sum, ok := m.GetData().(*metricpb.Metric_Sum)
		if !ok {
			continue
		}
		for _, point := range sum.Sum.GetDataPoints() {
			values = append(values, point.GetAsInt())
		}
	}
	return values
}

// TestSourceForceFlushExportsTheZeroSeedThenTheFirstRequest: a series created at 0 reaches the collector at 0 on a
// flush, before its first tick (the reader interval here is an hour), and the next flush carries 1. A rate query then
// has the 0 sample to subtract from.
func TestSourceForceFlushExportsTheZeroSeedThenTheFirstRequest(t *testing.T) {
	c, addr := startCollector(t)
	registry := health.NewRegistry(time.Second)
	provider, source, err := buildProvider(PushOptions{Options: pushOptions(addr), Registry: registry, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("buildProvider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	flusher, ok := any(source).(forceFlusher)
	if !ok {
		t.Fatal("apimetrics.Source has no ForceFlush: nothing exports a seeded series before the reader's first tick")
	}
	counter, err := provider.Meter("probe").Int64Counter("probe_seeded_total")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	counter.Add(ctx, 0)
	if err := flusher.ForceFlush(ctx); err != nil {
		t.Fatalf("first flush: %v", err)
	}
	if got := sumValues(c, "probe_seeded_total"); len(got) != 1 || got[0] != 0 {
		t.Fatalf("after the seed flush the collector holds %v, want [0]", got)
	}
	counter.Add(ctx, 1)
	if err := flusher.ForceFlush(ctx); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if got := sumValues(c, "probe_seeded_total"); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("after one request the collector holds %v, want [0 1]", got)
	}
}

// TestSourceForceFlushWithoutAPushIsAnImmediateNoOp: a process without the push (the kill switch, or a nil source when
// the push could not be built) exports nothing and spends no time on the flush.
func TestSourceForceFlushWithoutAPushIsAnImmediateNoOp(t *testing.T) {
	c, addr := startCollector(t)
	registry := health.NewRegistry(time.Second)
	options := pushOptions(addr)
	options.Enabled = false
	provider, source, err := buildProvider(PushOptions{Options: options, Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	counter, _ := provider.Meter("probe").Int64Counter("unpushed_total")
	counter.Add(context.Background(), 0)
	for name, candidate := range map[string]any{"no push": source, "nil source": (*Source)(nil)} {
		flusher, ok := candidate.(forceFlusher)
		if !ok {
			t.Fatalf("%s: apimetrics.Source has no ForceFlush", name)
		}
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			if err := flusher.ForceFlush(context.Background()); err != nil {
				t.Errorf("%s: ForceFlush = %v, want nil", name, err)
			}
			if waited := time.Since(start); waited != 0 {
				t.Errorf("%s: ForceFlush waited %v on the virtual clock, want 0", name, waited)
			}
		})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.metrics) != 0 {
		t.Errorf("the collector received %d metrics without a push", len(c.metrics))
	}
}
