package apiservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// Review fold for the seeded series: a 3xx class is seeded (a redirect route reads 0 then 1), and only the proxied routes the listener
// really registers get a series (the static pattern map is a predicate, never a seed list).

func withManualProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(previous); _ = provider.Shutdown(context.Background()) })
	return reader
}

func publicConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func routeSeries(points map[string]int64, route string) map[string]int64 {
	out := map[string]int64{}
	for key, value := range points {
		if strings.Contains(key, "route="+route+",") {
			out[key] = value
		}
	}
	return out
}

func TestAFirstRedirectOnTheOIDCAuthorizeRouteReadsZeroThenOne(t *testing.T) {
	reader := withManualProvider(t)
	const pattern = "/api/v1/auth/oidc/{provider_id}/authorize"
	redirect := httpapi.Route{Method: http.MethodPost, Pattern: pattern, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://idp.example/authorize", http.StatusFound)
	})}
	server, err := NewServer(publicConfig(t), quietLogger(), []httpapi.Route{redirect})
	if err != nil {
		t.Fatal(err)
	}
	const key = "listener=public,method=POST,route=" + pattern + ",status_class=3xx"
	before := counterPoints(t, reader, "dev_health_api_http_requests_total")
	if value, ok := before[key]; !ok || value != 0 {
		t.Fatalf("3xx series before the first request = %d (present %v), want present at 0: %v", value, ok, routeSeries(before, pattern))
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/oidc/google/authorize", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusFound {
		t.Fatalf("status %d, want 302", response.Code)
	}
	if after := counterPoints(t, reader, "dev_health_api_http_requests_total"); after[key] != 1 {
		t.Errorf("3xx series after one 302 = %d, want 1: %v", after[key], routeSeries(after, pattern))
	}
}

func TestAnUnservedProxiedRouteHasNoSeries(t *testing.T) {
	t.Run("no pool registers no proxied route", func(t *testing.T) {
		reader := withManualProvider(t)
		routes := Routes(Deps{AuthProxy: mustProxyConfig(t, allOn("http://127.0.0.1:1"))}, quietLogger())
		if _, err := NewServer(publicConfig(t), quietLogger(), routes); err != nil {
			t.Fatal(err)
		}
		for key := range counterPoints(t, reader, "dev_health_api_http_requests_total") {
			if strings.Contains(key, "route=/api/v1/auth/") {
				t.Errorf("series %s exists although no proxied auth route is registered", key)
			}
		}
	})
	t.Run("one registered route seeds only itself, with its own method", func(t *testing.T) {
		reader := withManualProvider(t)
		ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
		routes := []httpapi.Route{
			{Method: http.MethodPost, Pattern: "/api/v1/auth/login", Handler: ok},
			{Method: http.MethodGet, Pattern: "/api/v1/auth/login", Handler: ok},
		}
		if _, err := NewServer(publicConfig(t), quietLogger(), routes); err != nil {
			t.Fatal(err)
		}
		points := counterPoints(t, reader, "dev_health_api_http_requests_total")
		want := map[string]bool{}
		for _, class := range []string{"2xx", "3xx", "4xx", "5xx"} {
			want["listener=public,method=POST,route=/api/v1/auth/login,status_class="+class] = true
		}
		for key := range points {
			if !want[key] {
				t.Errorf("unexpected seeded series %s", key)
			}
		}
		for key := range want {
			if _, present := points[key]; !present {
				t.Errorf("missing seeded series %s", key)
			}
		}
	})
}

func TestTheAuthProxyCounterHasNoSeriesWhenNoProxiedRouteIsRegistered(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("fold").Int64Counter("devhealth_api_auth_proxy_requests_total")
	if err != nil {
		t.Fatal(err)
	}
	previous := authProxyRequests
	authProxyRequests = counter
	t.Cleanup(func() { authProxyRequests = previous })

	_ = Routes(Deps{AuthProxy: mustProxyConfig(t, allOn("http://127.0.0.1:1"))}, quietLogger())

	if points := counterPoints(t, reader, "devhealth_api_auth_proxy_requests_total"); len(points) != 0 {
		t.Errorf("%d proxy series are seeded with no proxied route registered: %v", len(points), points)
	}
}
