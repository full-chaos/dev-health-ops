package apiservice

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// startupCapture is the exporter behind a push reader, socket-free: it keeps the int64 sum points of one metric, per export, copied at
// export time (the reader reuses its buffers).
type startupCapture struct {
	name    string
	mu      sync.Mutex
	exports []map[string]int64
}

func (c *startupCapture) Temporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(kind)
}

func (c *startupCapture) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(kind)
}

func (c *startupCapture) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	points := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != c.name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				var labels []string
				for _, kv := range point.Attributes.ToSlice() {
					labels = append(labels, string(kv.Key)+"="+kv.Value.Emit())
				}
				sort.Strings(labels)
				points[strings.Join(labels, ",")] = point.Value
			}
		}
	}
	c.mu.Lock()
	c.exports = append(c.exports, points)
	c.mu.Unlock()
	return nil
}

func (c *startupCapture) ForceFlush(context.Context) error { return nil }

func (c *startupCapture) Shutdown(context.Context) error { return nil }

func (c *startupCapture) export(t *testing.T, index int) map[string]int64 {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.exports) <= index {
		t.Fatalf("%d export(s), want export %d", len(c.exports), index)
	}
	return c.exports[index]
}

// TestAStartupFlushExportsTheAuthProxySeedAtZeroBeforeTheFirstRequest is the end-to-end claim on one counter: the go-api route set seeds
// the auth proxy counter at construction, so a flush of the push reader before any request (what the shell's metricsFlush.Start runs)
// exports every registered series at 0, and the first request then reads 1 on the next export: a rate query has its zero sample. The push
// reader is the SDK PeriodicReader the OTLP pipeline uses, with an hour interval so only the explicit flushes export.
func TestAStartupFlushExportsTheAuthProxySeedAtZeroBeforeTheFirstRequest(t *testing.T) {
	const name = "devhealth_api_auth_proxy_requests_total"
	capture := &startupCapture{name: name}
	reader := sdkmetric.NewPeriodicReader(capture, sdkmetric.WithInterval(time.Hour))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	counter, err := provider.Meter("startupflush").Int64Counter(name)
	if err != nil {
		t.Fatal(err)
	}
	previous := authProxyRequests
	authProxyRequests = counter
	t.Cleanup(func() { authProxyRequests = previous })

	// never dialled: the pool is only a presence check for the route set
	pool, err := pgxpool.New(context.Background(), "postgres://binding@127.0.0.1:1/binding?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_ = Routes(Deps{Pool: pool, Guard: policy.NewGuard(nil, quietLogger()), ClickHouse: nilConn{},
		AuthProxy: mustProxyConfig(t, allOn("http://127.0.0.1:1"))}, quietLogger())

	ctx := context.Background()
	if err := reader.ForceFlush(ctx); err != nil {
		t.Fatalf("startup flush: %v", err)
	}
	const forwarded = "aggregate=oidc,outcome=forwarded"
	startup := capture.export(t, 0)
	if len(startup) != 6 {
		t.Fatalf("the startup flush exported %d auth proxy series, want 6 (oidc and saml x 3 outcomes) at 0: %v", len(startup), startup)
	}
	for series, value := range startup {
		if value != 0 {
			t.Errorf("startup export %s = %d, want 0", series, value)
		}
	}
	if _, ok := startup[forwarded]; !ok {
		t.Fatalf("the startup export lacks %s: %v", forwarded, startup)
	}

	count(ctx, AuthOIDC, outcomeForwarded)
	if err := reader.ForceFlush(ctx); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	after := capture.export(t, 1)
	if after[forwarded] != 1 {
		t.Errorf("after one forwarded OIDC request %s = %d, want 1 (0 then 1)", forwarded, after[forwarded])
	}
	for series, value := range after {
		if series != forwarded && value != 0 {
			t.Errorf("after one request %s = %d, want 0", series, value)
		}
	}
}
