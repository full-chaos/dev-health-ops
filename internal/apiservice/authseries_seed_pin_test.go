package apiservice

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// The seeded label sets are PINNED: a new aggregate, outcome or proxied pattern must change these literals on purpose, and the series
// count stays visible in review (6 aggregates x 3 outcomes = 18; 15 patterns x 3 status classes = 45; 63 in all with every flag on).

func seriesSet(t *testing.T, reader *sdkmetric.ManualReader, name string) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				out[point.Attributes.Encoded(attribute.DefaultEncoder())] = point.Value
			}
		}
	}
	return out
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestTheSeededAuthProxySeriesAreExactlyThePinnedSetAndTheFirstRequestIsAVisibleDelta(t *testing.T) {
	var all []string
	for _, aggregate := range []string{"credentials", "local_login", "oidc", "saml", "sessions", "social"} {
		for _, outcome := range []string{"forwarded", "local", "upstream_error"} {
			all = append(all, "aggregate="+aggregate+",outcome="+outcome)
		}
	}
	var localOnly []string
	for _, aggregate := range []string{"credentials", "local_login", "oidc", "saml", "sessions", "social"} {
		localOnly = append(localOnly, "aggregate="+aggregate+",outcome=local")
	}
	for _, c := range []struct {
		name string
		cfg  AuthProxyConfig
		want []string
	}{
		{"every flag on", mustProxyConfig(t, allOn("http://127.0.0.1:1")), all},
		{"no flag on: a disabled aggregate can only count local", AuthProxyConfig{}, localOnly},
	} {
		t.Run(c.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("pin").Int64Counter("devhealth_api_auth_proxy_requests_total")
			if err != nil {
				t.Fatal(err)
			}
			seedAuthProxySeries(counter, c.cfg)
			got := seriesSet(t, reader, "devhealth_api_auth_proxy_requests_total")
			if strings.Join(sortedKeys(got), "\n") != strings.Join(c.want, "\n") {
				t.Errorf("seeded series\n%s\nwant\n%s", strings.Join(sortedKeys(got), "\n"), strings.Join(c.want, "\n"))
			}
			for series, value := range got {
				if value != 0 {
					t.Errorf("series %s seeded at %d, want 0", series, value)
				}
			}
			previous := authProxyRequests
			authProxyRequests = counter
			t.Cleanup(func() { authProxyRequests = previous })
			count(context.Background(), AuthSessions, outcomeForwarded)
			after := seriesSet(t, reader, "devhealth_api_auth_proxy_requests_total")
			if c.name == "every flag on" && after["aggregate=sessions,outcome=forwarded"] != 1 {
				t.Errorf("one request on a seeded series is not a visible +1: %v", after)
			}
		})
	}
}

func TestTheSeededAuthRouteRequestSeriesAreExactlyThePinnedSet(t *testing.T) {
	wantSeeds := []httpapi.RequestSeed{
		{Method: "POST", Route: "/api/v1/auth/change-password"},
		{Method: "POST", Route: "/api/v1/auth/forgot-password"},
		{Method: "POST", Route: "/api/v1/auth/login"},
		{Method: "POST", Route: "/api/v1/auth/logout"},
		{Method: "POST", Route: "/api/v1/auth/oidc/{provider_id}/authorize"},
		{Method: "POST", Route: "/api/v1/auth/oidc/{provider_id}/callback"},
		{Method: "POST", Route: "/api/v1/auth/refresh"},
		{Method: "POST", Route: "/api/v1/auth/register"},
		{Method: "POST", Route: "/api/v1/auth/resend-verification"},
		{Method: "POST", Route: "/api/v1/auth/reset-password"},
		{Method: "POST", Route: "/api/v1/auth/saml/{provider_id}/acs"},
		{Method: "POST", Route: "/api/v1/auth/saml/{provider_id}/initiate"},
		{Method: "GET", Route: "/api/v1/auth/saml/{provider_id}/metadata"},
		{Method: "POST", Route: "/api/v1/auth/social-login"},
		{Method: "GET", Route: "/api/v1/auth/verify"},
	}
	got := authProxyRequestSeeds()
	if len(got) != len(wantSeeds) {
		t.Fatalf("%d seeds, want %d: %v", len(got), len(wantSeeds), got)
	}
	for i := range wantSeeds {
		if got[i] != wantSeeds[i] {
			t.Errorf("seed %d = %v, want %v", i, got[i], wantSeeds[i])
		}
	}
	if classes := strings.Join(httpapi.SeededStatusClasses, ","); classes != "2xx,4xx,5xx" {
		t.Errorf("seeded status classes %q, want 2xx,4xx,5xx", classes)
	}

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(previous); _ = provider.Shutdown(context.Background()) })
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	route := httpapi.Route{Method: http.MethodPost, Pattern: "/echo", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})}
	if _, err := NewServer(cfg, quietLogger(), []httpapi.Route{route}); err != nil {
		t.Fatal(err)
	}
	series := seriesSet(t, reader, "dev_health_api_http_requests_total")
	if len(series) != 45 {
		t.Fatalf("%d seeded route series, want 45 (15 patterns x 3 status classes): %v", len(series), sortedKeys(series))
	}
	for key, value := range series {
		if value != 0 {
			t.Errorf("series %s seeded at %d, want 0", key, value)
		}
		if !strings.Contains(key, "listener=public") || !strings.Contains(key, "route=/api/v1/auth/") {
			t.Errorf("series %s is not a public-listener auth route", key)
		}
	}
}
