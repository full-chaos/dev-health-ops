package apiservice

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// The production call sites: Routes (the auth proxy counter, seeded once for the registered aggregates) and NewServer (the per-route series
// of the registered proxied routes), driven with a real route set. With a pool and a guard the SSO routes register and the session routes
// do not (they also need Auth, Verifier and Signer).

func TestRoutesSeedsTheProxyCounterOnceForTheAggregatesItRegisters(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	base, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("callsite").Int64Counter("devhealth_api_auth_proxy_requests_total")
	if err != nil {
		t.Fatal(err)
	}
	counter := &countingCounter{Int64Counter: base}
	previous := authProxyRequests
	authProxyRequests = counter
	t.Cleanup(func() { authProxyRequests = previous })

	// never dialled: the pool is only a presence check for the route set
	pool, err := pgxpool.New(context.Background(), "postgres://binding@127.0.0.1:1/binding?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	routes := Routes(Deps{Pool: pool, Guard: policy.NewGuard(nil, quietLogger()), ClickHouse: nilConn{},
		AuthProxy: mustProxyConfig(t, allOn("http://127.0.0.1:1"))}, quietLogger())

	got := seriesSet(t, reader, "devhealth_api_auth_proxy_requests_total")
	if len(got) != 6 || counter.adds != 6 {
		t.Fatalf("%d series from %d Add calls with the SSO routes registered, want 6 of each (oidc and saml x 3 outcomes): %v", len(got), counter.adds, got)
	}
	for series := range got {
		if !strings.Contains(series, "aggregate=oidc,") && !strings.Contains(series, "aggregate=saml,") {
			t.Errorf("series %s is not an SSO aggregate", series)
		}
	}

	metrics := withManualProvider(t)
	if _, err := NewServer(publicConfig(t), quietLogger(), routes); err != nil {
		t.Fatal(err)
	}
	proxied := 0
	for key, value := range seriesSet(t, metrics, "dev_health_api_http_requests_total") {
		if !strings.Contains(key, "route=/api/v1/auth/oidc/") && !strings.Contains(key, "route=/api/v1/auth/saml/") {
			continue
		}
		proxied++
		if value != 0 {
			t.Errorf("series %s seeded at %d, want 0", key, value)
		}
	}
	if proxied != 20 {
		t.Errorf("%d SSO route series seeded from the real route set, want 20 (5 patterns x 4 classes)", proxied)
	}
}
