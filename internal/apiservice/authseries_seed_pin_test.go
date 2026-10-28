package apiservice

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// The seeded label sets are PINNED and follow the REGISTERED proxied routes: a new aggregate, outcome, class or proxied pattern must
// change these literals on purpose. Public listener, every route registered and every flag on: proxy 6 x 3 = 18, routes 15 x 4 = 60, 78
// in all; SSO routes only 26; session routes only 52; no pool 0.

var (
	ssoAggregates     = map[AuthAggregate]bool{AuthOIDC: true, AuthSAML: true}
	sessionAggregates = map[AuthAggregate]bool{AuthSessions: true, AuthLocalLogin: true, AuthCredentials: true, AuthSocial: true}
)

// proxiedRoutes are no-op routes with the method and pattern of every forwardable auth route whose aggregate keep accepts.
func proxiedRoutes(keep func(AuthAggregate) bool) []httpapi.Route {
	var routes []httpapi.Route
	for key, aggregate := range proxiedRouteKeys {
		if !keep(aggregate) {
			continue
		}
		method, pattern, _ := strings.Cut(key, " ")
		routes = append(routes, httpapi.Route{Method: method, Pattern: pattern, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})})
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].Pattern+routes[i].Method < routes[j].Pattern+routes[j].Method })
	return routes
}

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

// countingCounter counts the Add calls that reach the real counter, to pin that a series is seeded exactly once.
type countingCounter struct {
	metric.Int64Counter
	adds int
}

func (c *countingCounter) Add(ctx context.Context, value int64, options ...metric.AddOption) {
	c.adds++
	c.Int64Counter.Add(ctx, value, options...)
}

func TestTheSeededAuthProxySeriesFollowTheRegisteredAggregatesOneAddEach(t *testing.T) {
	all := func(AuthAggregate) bool { return true }
	for _, c := range []struct {
		name   string
		flags  bool
		keep   func(AuthAggregate) bool
		series int
		sample string
	}{
		{"every route, every flag", true, all, 18, "aggregate=sessions,outcome=forwarded"},
		{"every route, no flag: only local", false, all, 6, "aggregate=sessions,outcome=local"},
		{"SSO routes only, every flag", true, func(a AuthAggregate) bool { return ssoAggregates[a] }, 6, "aggregate=oidc,outcome=upstream_error"},
		{"SSO routes only, no flag", false, func(a AuthAggregate) bool { return ssoAggregates[a] }, 2, "aggregate=saml,outcome=local"},
		{"session routes only, every flag", true, func(a AuthAggregate) bool { return sessionAggregates[a] }, 12, "aggregate=social,outcome=forwarded"},
		{"session routes only, no flag", false, func(a AuthAggregate) bool { return sessionAggregates[a] }, 4, "aggregate=credentials,outcome=local"},
		{"no proxied route registered", true, func(AuthAggregate) bool { return false }, 0, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			base, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("pin").Int64Counter("devhealth_api_auth_proxy_requests_total")
			if err != nil {
				t.Fatal(err)
			}
			counter := &countingCounter{Int64Counter: base}
			previous := authProxyRequests
			authProxyRequests = counter
			t.Cleanup(func() { authProxyRequests = previous })

			cfg := AuthProxyConfig{}
			if c.flags {
				cfg = mustProxyConfig(t, allOn("http://127.0.0.1:1"))
			}
			newAuthProxy(cfg, quietLogger()).seedSeries(proxiedRoutes(c.keep))

			got := seriesSet(t, reader, "devhealth_api_auth_proxy_requests_total")
			if len(got) != c.series || counter.adds != c.series {
				t.Errorf("%d series from %d Add calls, want %d of each: %v", len(got), counter.adds, c.series, got)
			}
			for series, value := range got {
				if value != 0 {
					t.Errorf("series %s seeded at %d, want 0", series, value)
				}
			}
			if c.sample != "" {
				if _, ok := got[c.sample]; !ok {
					t.Errorf("series %q is not seeded: %v", c.sample, got)
				}
			}
		})
	}
}

func TestOneRequestOnASeededAuthProxySeriesIsAVisiblePlusOne(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("pin").Int64Counter("devhealth_api_auth_proxy_requests_total")
	if err != nil {
		t.Fatal(err)
	}
	previous := authProxyRequests
	authProxyRequests = counter
	t.Cleanup(func() { authProxyRequests = previous })
	newAuthProxy(mustProxyConfig(t, allOn("http://127.0.0.1:1")), quietLogger()).seedSeries(proxiedRoutes(func(AuthAggregate) bool { return true }))
	count(context.Background(), AuthSessions, outcomeForwarded)
	if got := seriesSet(t, reader, "devhealth_api_auth_proxy_requests_total"); got["aggregate=sessions,outcome=forwarded"] != 1 {
		t.Errorf("one request on a seeded series is not a visible +1: %v", got)
	}
}

func TestTheSeededRouteSeedsAreExactlyThePinnedFifteenInOrder(t *testing.T) {
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
	got := authProxyRequestSeeds(proxiedRoutes(func(AuthAggregate) bool { return true }))
	if len(got) != len(wantSeeds) {
		t.Fatalf("%d seeds, want %d: %v", len(got), len(wantSeeds), got)
	}
	for i := range wantSeeds {
		if got[i] != wantSeeds[i] {
			t.Errorf("seed %d = %v, want %v", i, got[i], wantSeeds[i])
		}
	}
	if classes := strings.Join(httpapi.SeededStatusClasses, ","); classes != "2xx,3xx,4xx,5xx" {
		t.Errorf("seeded status classes %q, want 2xx,3xx,4xx,5xx", classes)
	}
}

func TestTheSeededRouteSeriesFollowTheRegisteredProxiedRoutes(t *testing.T) {
	for _, c := range []struct {
		name   string
		keep   func(AuthAggregate) bool
		series int
	}{
		{"every proxied route registered: 15 x 4", func(AuthAggregate) bool { return true }, 60},
		{"SSO routes only: 5 x 4", func(a AuthAggregate) bool { return ssoAggregates[a] }, 20},
		{"session routes only: 10 x 4", func(a AuthAggregate) bool { return sessionAggregates[a] }, 40},
		{"no proxied route registered", func(AuthAggregate) bool { return false }, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			reader := withManualProvider(t)
			routes := append(proxiedRoutes(c.keep), httpapi.Route{Method: http.MethodGet, Pattern: "/api/v1/other", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})})
			if _, err := NewServer(publicConfig(t), quietLogger(), routes); err != nil {
				t.Fatal(err)
			}
			series := seriesSet(t, reader, "dev_health_api_http_requests_total")
			if len(series) != c.series {
				t.Fatalf("%d seeded route series, want %d: %v", len(series), c.series, series)
			}
			for key, value := range series {
				if value != 0 {
					t.Errorf("series %s seeded at %d, want 0", key, value)
				}
				if !strings.Contains(key, "listener=public") || !strings.Contains(key, "route=/api/v1/auth/") {
					t.Errorf("series %s is not a public-listener proxied auth route", key)
				}
			}
		})
	}
}
