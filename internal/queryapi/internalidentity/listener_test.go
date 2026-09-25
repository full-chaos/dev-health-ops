package internalidentity

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestPublicDeletesEveryInternalHeaderInEverySpelling(t *testing.T) {
	var seen http.Header
	handler := Public(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { seen = r.Header.Clone() }))
	req := httptest.NewRequest(http.MethodPost, "/query", nil)
	req.Header.Set("Authorization", "Bearer keep-me")
	req.Header.Set("X-Other", "keep")
	// Different spellings canonicalise to the same key; duplicates too.
	req.Header["x-dh-internal-org-id"] = []string{"raw-lowercase-key"}
	req.Header.Add(HeaderOrgID, "org-1")
	req.Header.Add(HeaderOrgID, "org-2")
	req.Header.Set(HeaderRole, "owner")
	req.Header.Set(HeaderSuperuser, "true")
	req.Header.Set(HeaderImpersonationActive, "true")
	handler.ServeHTTP(httptest.NewRecorder(), req)
	if Present(seen) {
		t.Fatalf("headers reached the handler: %v", seen)
	}
	for name := range seen {
		if len(name) >= 12 && (name[:12] == "X-Dh-Interna" || name[:12] == "x-dh-interna") {
			t.Fatalf("spelling %q survived: %v", name, seen)
		}
	}
	if seen.Get("Authorization") != "Bearer keep-me" || seen.Get("X-Other") != "keep" {
		t.Fatalf("unrelated headers were touched: %v", seen)
	}
}

func TestOnInternalListenerIsTrueOnlyBehindInternal(t *testing.T) {
	probe := func(wrap func(http.Handler) http.Handler) bool {
		var on bool
		wrap(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { on = OnInternalListener(r.Context()) })).
			ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		return on
	}
	if !probe(Internal) {
		t.Fatal("a request behind Internal is not on the internal listener")
	}
	if probe(Public) {
		t.Fatal("a request behind Public is on the internal listener")
	}
	if OnInternalListener(httptest.NewRequest(http.MethodGet, "/", nil).Context()) {
		t.Fatal("a bare request context is on the internal listener")
	}
}

func TestPathClassBoundsTheMetricLabel(t *testing.T) {
	for path, want := range map[string]string{
		"/query": "query", "/query/proof": "query_proof", "/buildinfo": "buildinfo",
		"/api/v1/meta": "rest", "/api/v1/people/x/summary": "rest",
		"/anything/attacker/controls/1234": "other", "": "other",
	} {
		if got := pathClass(path); got != want {
			t.Errorf("pathClass(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestPublicCountsEveryDroppedRequestByBoundedPathClass(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(noopmetric.NewMeterProvider()) })
	handler := Public(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	for _, path := range []string{"/query", "/query", "/attacker/chosen/1", "/attacker/chosen/2"} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		req.Header.Set(HeaderRole, "owner")
		handler.ServeHTTP(httptest.NewRecorder(), req)
	}
	// A request without the headers is not counted.
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/query", nil))

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "devhealth_query_api_internal_headers_dropped_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				class, _ := point.Attributes.Value("path_class")
				got[class.AsString()] += point.Value
			}
		}
	}
	if got["query"] != 2 || got["other"] != 2 || len(got) != 2 {
		t.Fatalf("dropped counts = %v, want query=2 other=2 and nothing else", got)
	}
}
