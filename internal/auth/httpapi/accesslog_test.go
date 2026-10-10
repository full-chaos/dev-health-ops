package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const canary = "leakcanary"

type accessHarness struct {
	logs    *bytes.Buffer
	reader  *sdkmetric.ManualReader
	handler http.Handler
}

func newAccessHarness(t *testing.T, level slog.Level, mutate func(*ServerOptions), routes ...Route) *accessHarness {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	logs := &bytes.Buffer{}
	options := testOptions(routes...)
	options.Logger = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: level}))
	options.Listener = "public"
	options.StrictPaths = true
	if mutate != nil {
		mutate(&options)
	}
	return &accessHarness{logs: logs, reader: reader, handler: handlerFor(t, options)}
}

func (h *accessHarness) do(method, target string, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, nil)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, request)
	return recorder
}

// accessLines are the "http request" log lines, decoded.
func (h *accessHarness) accessLines(t *testing.T) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line %q is not JSON: %v", raw, err)
		}
		if line["msg"] == "http request" {
			lines = append(lines, line)
		}
	}
	return lines
}

// series is one metric data point: its labels rendered "k=v,k=v" in a fixed
// order, and its count (counter value, or histogram observation count).
func (h *accessHarness) series(t *testing.T, name string) map[string]uint64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	out := map[string]uint64{}
	label := func(set attribute.Set) string {
		parts := make([]string, 0, 4)
		for _, key := range []string{"route", "method", "status_class", "listener"} {
			value, _ := set.Value(attribute.Key(key))
			parts = append(parts, key+"="+value.AsString())
		}
		if set.Len() != 4 {
			t.Fatalf("series carries %d labels, want exactly route, method, status_class, listener", set.Len())
		}
		return strings.Join(parts, ",")
	}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					out[label(point.Attributes)] += uint64(point.Value)
				}
			case metricdata.Histogram[float64]:
				for _, point := range data.DataPoints {
					out[label(point.Attributes)] += point.Count
				}
			default:
				t.Fatalf("metric %s has unexpected type %T", name, m.Data)
			}
		}
	}
	return out
}

func itemsRoute() Route {
	return okRoute(http.MethodGet, "/v1/items/{id}")
}

func TestAccessLineAndMetricsForAKnownRouteCarryThePatternNeverThePath(t *testing.T) {
	h := newAccessHarness(t, slog.LevelInfo, nil, itemsRoute())
	response := h.do(http.MethodGet, "/v1/items/item-7731?api_key="+canary, map[string]string{
		"Authorization": "Bearer " + canary,
		"Cookie":        "session=" + canary,
		// A forwarded header is chosen by the client: it never becomes the peer.
		"X-Forwarded-For": canary,
		RequestIDHeader:   "0b1b2c3d-0000-4000-8000-000000000000",
	})
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
	lines := h.accessLines(t)
	if len(lines) != 1 {
		t.Fatalf("got %d access lines, want 1:\n%s", len(lines), h.logs.String())
	}
	line := lines[0]
	want := map[string]any{
		"method": "GET", "route": "/v1/items/{id}", "status": float64(204),
		"listener": "public", "request_id": "0b1b2c3d-0000-4000-8000-000000000000", "level": "INFO",
		// httptest's RemoteAddr is 192.0.2.1:1234: the host part only, no port.
		"peer": "192.0.2.1",
	}
	for key, value := range want {
		if line[key] != value {
			t.Errorf("log field %s = %v, want %v", key, line[key], value)
		}
	}
	if duration, ok := line["duration_ms"].(float64); !ok || duration < 0 {
		t.Errorf("duration_ms = %v, want a non-negative number", line["duration_ms"])
	}
	allowed := map[string]bool{"time": true, "level": true, "msg": true, "method": true, "route": true,
		"status": true, "duration_ms": true, "listener": true, "request_id": true, "peer": true}
	for key := range line {
		if !allowed[key] {
			t.Errorf("access line carries unexpected field %q", key)
		}
	}
	for _, leaked := range []string{"item-7731", canary, "api_key", "Bearer", "session="} {
		if strings.Contains(h.logs.String(), leaked) {
			t.Errorf("log leaks %q:\n%s", leaked, h.logs.String())
		}
	}

	wantSeries := map[string]uint64{"route=/v1/items/{id},method=GET,status_class=2xx,listener=public": 1}
	for _, name := range []string{requestsMetricName, durationMetricName} {
		got := h.series(t, name)
		if fmt.Sprint(got) != fmt.Sprint(wantSeries) {
			t.Errorf("%s = %v, want %v", name, got, wantSeries)
		}
	}
}

func TestUnknownPathsShareOneUnmatchedSeriesAndNeverLogTheRawPath(t *testing.T) {
	h := newAccessHarness(t, slog.LevelInfo, nil, itemsRoute())
	for index := 0; index < 40; index++ {
		response := h.do(http.MethodGet, fmt.Sprintf("/probe/%d/%s", index, canary), nil)
		if response.Code != http.StatusNotFound {
			t.Fatalf("unknown path answered %d, want 404", response.Code)
		}
	}
	want := map[string]uint64{"route=unmatched,method=GET,status_class=4xx,listener=public": 40}
	for _, name := range []string{requestsMetricName, durationMetricName} {
		if got := h.series(t, name); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	for _, line := range h.accessLines(t) {
		if line["route"] != UnmatchedRoute || line["status"] != float64(404) {
			t.Errorf("unknown path line = %v, want route=unmatched status=404", line)
		}
	}
	if strings.Contains(h.logs.String(), "probe") || strings.Contains(h.logs.String(), canary) {
		t.Errorf("log carries the raw path:\n%s", h.logs.String())
	}
}

func TestRefusalsBeforeTheHandlerStayAttributedToTheirRoute(t *testing.T) {
	h := newAccessHarness(t, slog.LevelInfo, func(o *ServerOptions) {
		o.RateLimit, o.RateLimitBurst = 0.0001, 1
	}, itemsRoute(), okRoute(http.MethodPost, "/v1/items/{id}"))

	if code := h.do(http.MethodGet, "/v1/items/a", nil).Code; code != http.StatusNoContent {
		t.Fatalf("first request = %d, want 204", code)
	}
	if code := h.do(http.MethodGet, "/v1/items/b", nil).Code; code != http.StatusTooManyRequests {
		t.Fatalf("second request = %d, want 429", code)
	}
	if code := h.do(http.MethodDelete, "/v1/items/c", nil).Code; code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE = %d, want 405", code)
	}
	want := map[string]uint64{
		"route=/v1/items/{id},method=GET,status_class=2xx,listener=public":    1,
		"route=/v1/items/{id},method=GET,status_class=4xx,listener=public":    1,
		"route=/v1/items/{id},method=DELETE,status_class=4xx,listener=public": 1,
	}
	if got := h.series(t, requestsMetricName); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("requests = %v, want %v", got, want)
	}
}

func TestAnUnknownMethodTokenIsOneBoundedLabel(t *testing.T) {
	h := newAccessHarness(t, slog.LevelInfo, nil, itemsRoute())
	for _, method := range []string{"BREW", "PROPFIND", "X-" + canary} {
		request := httptest.NewRequest(http.MethodGet, "/v1/items/a", nil)
		request.Method = method
		h.handler.ServeHTTP(httptest.NewRecorder(), request)
	}
	got := h.series(t, requestsMetricName)
	if len(got) != 1 {
		t.Fatalf("three odd methods made %d series %v, want 1", len(got), got)
	}
	for key := range got {
		if !strings.Contains(key, "method=OTHER") {
			t.Errorf("series %q, want method=OTHER", key)
		}
	}
	if strings.Contains(h.logs.String(), canary) || strings.Contains(h.logs.String(), "BREW") {
		t.Errorf("log carries the raw method:\n%s", h.logs.String())
	}
}

func TestAPanickingHandlerIsCountedAs5xx(t *testing.T) {
	boom := Route{Method: http.MethodGet, Pattern: "/v1/boom", Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("kaboom " + canary)
	})}
	h := newAccessHarness(t, slog.LevelInfo, nil, boom)
	if code := h.do(http.MethodGet, "/v1/boom", nil).Code; code != http.StatusInternalServerError {
		t.Fatalf("panic answered %d, want 500", code)
	}
	want := map[string]uint64{"route=/v1/boom,method=GET,status_class=5xx,listener=public": 1}
	if got := h.series(t, requestsMetricName); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("requests = %v, want %v", got, want)
	}
}

func TestTheAccessLineFollowsTheLogLevelWhileMetricsAlwaysRecord(t *testing.T) {
	h := newAccessHarness(t, slog.LevelWarn, nil, itemsRoute())
	h.do(http.MethodGet, "/v1/items/a", nil)
	if lines := h.accessLines(t); len(lines) != 0 {
		t.Errorf("warn-level logger wrote %d access lines, want 0", len(lines))
	}
	want := map[string]uint64{"route=/v1/items/{id},method=GET,status_class=2xx,listener=public": 1}
	if got := h.series(t, requestsMetricName); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("requests = %v, want %v", got, want)
	}
}

func TestListenerLabelDefaultsToTheServerName(t *testing.T) {
	h := newAccessHarness(t, slog.LevelInfo, func(o *ServerOptions) { o.Listener, o.Name = "", "billing-edge-http" }, itemsRoute())
	h.do(http.MethodGet, "/v1/items/a", nil)
	for key := range h.series(t, requestsMetricName) {
		if !strings.HasSuffix(key, "listener=billing-edge-http") {
			t.Errorf("series %q, want listener=billing-edge-http", key)
		}
	}
}

func TestOnlyACanonicalUUIDOr32HexIdIsLogged(t *testing.T) {
	uuid := "0b1b2c3d-0000-4000-8000-000000000000"
	hex32 := "0b1b2c3d000040008000000000000000"
	cases := []struct{ name, id, want string }{
		{"uuid", uuid, uuid},
		{"uuid upper", strings.ToUpper(uuid), strings.ToUpper(uuid)},
		{"32 hex", hex32, hex32},
		{"31 hex", hex32[:31], invalidRequestID},
		{"33 hex", hex32 + "0", invalidRequestID},
		{"35 uuid-ish", uuid[:35], invalidRequestID},
		{"37 uuid-ish", uuid + "0", invalidRequestID},
		{"uuid wrong dash", "0b1b2c3d_0000-4000-8000-000000000000", invalidRequestID},
		{"uuid non hex", "0b1b2c3d-0000-4000-8000-00000000000g", invalidRequestID},
		{"32 non hex", hex32[:31] + "g", invalidRequestID},
		{"credential canary", "credential_canary.jwt.segment", invalidRequestID},
		{"dotted jwt shaped", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl", invalidRequestID},
		{"plain word", "req-abc-1", invalidRequestID},
		{"bearer shaped", "Bearer review-only-" + canary, invalidRequestID},
		{"non ascii", "caf\u00e9", invalidRequestID},
		{"far over", strings.Repeat("a", 4000), invalidRequestID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newAccessHarness(t, slog.LevelInfo, func(o *ServerOptions) {
				o.AcceptRequestID = func(id string) bool { return id != "" }
			}, itemsRoute())
			h.do(http.MethodGet, "/v1/items/a", map[string]string{RequestIDHeader: tc.id})
			lines := h.accessLines(t)
			if len(lines) != 1 || lines[0]["request_id"] != tc.want {
				t.Errorf("access lines = %v, want one with request_id %q", lines, tc.want)
			}
			if strings.Contains(h.logs.String(), canary) {
				t.Errorf("log carries the canary:\n%s", h.logs.String())
			}
		})
	}
}

func TestThePanicLogSharesTheRequestIDRule(t *testing.T) {
	boom := Route{Method: http.MethodGet, Pattern: "/v1/boom", Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})}
	h := newAccessHarness(t, slog.LevelInfo, func(o *ServerOptions) {
		o.AcceptRequestID = func(id string) bool { return id != "" }
	}, boom)
	h.do(http.MethodGet, "/v1/boom", map[string]string{RequestIDHeader: "Bearer " + canary})
	if !strings.Contains(h.logs.String(), "handler panicked") {
		t.Fatalf("no panic log:\n%s", h.logs.String())
	}
	if strings.Contains(h.logs.String(), canary) {
		t.Errorf("a log line carries the client id:\n%s", h.logs.String())
	}
}

func TestDurationCoversTheHandlersWholeRun(t *testing.T) {
	slow := Route{Method: http.MethodGet, Pattern: "/v1/slow", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(40 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	})}
	h := newAccessHarness(t, slog.LevelInfo, nil, slow)
	h.do(http.MethodGet, "/v1/slow", nil)
	lines := h.accessLines(t)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	if duration, _ := lines[0]["duration_ms"].(float64); duration < 40 || duration > 5000 {
		t.Errorf("duration_ms = %v, want the handler's 40ms in milliseconds", duration)
	}
}

func TestAnImplicitOKIsLoggedAs200(t *testing.T) {
	body := Route{Method: http.MethodGet, Pattern: "/v1/body", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("hello"))
	})}
	silent := Route{Method: http.MethodGet, Pattern: "/v1/silent", Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}
	h := newAccessHarness(t, slog.LevelInfo, nil, body, silent)
	h.do(http.MethodGet, "/v1/body", nil)
	h.do(http.MethodGet, "/v1/silent", nil)
	want := map[string]uint64{
		"route=/v1/body,method=GET,status_class=2xx,listener=public":   1,
		"route=/v1/silent,method=GET,status_class=2xx,listener=public": 1,
	}
	if got := h.series(t, requestsMetricName); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("requests = %v, want %v", got, want)
	}
	for _, line := range h.accessLines(t) {
		if line["status"] != float64(200) {
			t.Errorf("line %v, want status 200", line)
		}
	}
}

func TestTheRecorderFlushesAndUnwrapsToTheRealWriter(t *testing.T) {
	inner := httptest.NewRecorder()
	recorder := &accessRecorder{ResponseWriter: inner}
	recorder.Flush()
	if !inner.Flushed || recorder.status != http.StatusOK {
		t.Errorf("flushed=%v status=%d, want the flush forwarded and 200 recorded", inner.Flushed, recorder.status)
	}
	if recorder.Unwrap() != http.ResponseWriter(inner) {
		t.Error("Unwrap does not return the wrapped writer")
	}
	informational := &accessRecorder{ResponseWriter: httptest.NewRecorder()}
	informational.WriteHeader(http.StatusEarlyHints)
	informational.WriteHeader(http.StatusAccepted)
	informational.WriteHeader(http.StatusInternalServerError)
	if informational.status != http.StatusAccepted {
		t.Errorf("status after a 103, a 202 and a late 500 = %d, want the first final status 202", informational.status)
	}
}

func TestAPanicAfterTheBodyStartedKeepsTheCommittedStatus(t *testing.T) {
	late := Route{Method: http.MethodGet, Pattern: "/v1/late", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("partial"))
		panic("after the body")
	})}
	h := newAccessHarness(t, slog.LevelInfo, nil, late)
	h.do(http.MethodGet, "/v1/late", nil)
	want := map[string]uint64{"route=/v1/late,method=GET,status_class=2xx,listener=public": 1}
	if got := h.series(t, requestsMetricName); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("requests = %v, want %v", got, want)
	}
}

func TestAnAbortedResponseKeepsWhatWasCommittedAndOtherwiseHasNoStatusClass(t *testing.T) {
	abort := func(write bool) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if write {
				_, _ = w.Write([]byte("partial"))
			}
			panic(http.ErrAbortHandler)
		})
	}
	h := newAccessHarness(t, slog.LevelInfo, nil,
		Route{Method: http.MethodGet, Pattern: "/v1/abort-after-body", Handler: abort(true)},
		Route{Method: http.MethodGet, Pattern: "/v1/abort-before-body", Handler: abort(false)})
	for _, target := range []string{"/v1/abort-after-body", "/v1/abort-before-body"} {
		func() {
			defer func() {
				if recovered := recover(); recovered != http.ErrAbortHandler {
					t.Errorf("%s: recovered %v, want http.ErrAbortHandler re-panicked to net/http", target, recovered)
				}
			}()
			h.do(http.MethodGet, target, nil)
		}()
	}
	want := map[string]uint64{
		"route=/v1/abort-after-body,method=GET,status_class=2xx,listener=public":   1,
		"route=/v1/abort-before-body,method=GET,status_class=none,listener=public": 1,
	}
	if got := h.series(t, requestsMetricName); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("requests = %v, want %v", got, want)
	}
}

func TestALoggableRequestIDIsEmptyWhenNoneWasBound(t *testing.T) {
	if got := LoggableRequestID(context.Background()); got != "" {
		t.Errorf("LoggableRequestID(no id) = %q, want empty", got)
	}
}

func TestASlashRedirectIsCountedAgainstTheRouteItAddresses(t *testing.T) {
	h := newAccessHarness(t, slog.LevelInfo, func(o *ServerOptions) { o.RedirectSlashes = true }, itemsRoute())
	if code := h.do(http.MethodGet, "/v1/items/a/", nil).Code; code != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307", code)
	}
	want := map[string]uint64{"route=/v1/items/{id},method=GET,status_class=3xx,listener=public": 1}
	for _, name := range []string{requestsMetricName, durationMetricName} {
		if got := h.series(t, name); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	lines := h.accessLines(t)
	if len(lines) != 1 || lines[0]["route"] != "/v1/items/{id}" || lines[0]["status"] != float64(307) {
		t.Errorf("access lines = %v, want one 307 on /v1/items/{id}", lines)
	}
}

func TestAnUnmatchedPathWithRedirectSlashesStaysUnmatched(t *testing.T) {
	h := newAccessHarness(t, slog.LevelInfo, func(o *ServerOptions) { o.RedirectSlashes = true }, itemsRoute())
	if code := h.do(http.MethodGet, "/nothing/here/", nil).Code; code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}
	want := map[string]uint64{"route=unmatched,method=GET,status_class=4xx,listener=public": 1}
	if got := h.series(t, requestsMetricName); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("requests = %v, want %v", got, want)
	}
}
