package legacyingest

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMatchesAnyKey(t *testing.T) {
	keys := []string{"alpha", "bravo-longer-key", "c"}
	for _, item := range []struct {
		presented string
		want      bool
	}{
		{"alpha", true}, {"bravo-longer-key", true}, {"c", true},
		{"alph", false}, {"alphaa", false}, {"", false}, {"C", false}, {"bravo-longer-ke", false},
	} {
		if got := matchesAnyKey(item.presented, keys); got != item.want {
			t.Errorf("matchesAnyKey(%q) = %v, want %v", item.presented, got, item.want)
		}
	}
	if matchesAnyKey("x", nil) {
		t.Error("no configured key must match nothing")
	}
}

func TestUsageCounterByRouteAndKeyClass(t *testing.T) {
	metrics := NewMetrics()
	call := func(path string, values map[string]string, headers map[string]string) {
		t.Helper()
		routes := Routes(Deps{Metrics: metrics, Getenv: env(values), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		for _, route := range routes {
			if route.Pattern != "/api/v1/ingest/"+path {
				continue
			}
			request := httptest.NewRequest(http.MethodPost, route.Pattern, strings.NewReader(commitsBody))
			request.Header.Set("Content-Type", "application/json")
			for key, value := range headers {
				request.Header.Set(key, value)
			}
			route.Handler.ServeHTTP(httptest.NewRecorder(), request)
			return
		}
		t.Fatalf("no route %s", path)
	}
	keyed := map[string]string{"INGEST_API_KEYS": "k1"}
	signed := map[string]string{"INGEST_SIGNING_SECRET": "s"}
	both := map[string]string{"INGEST_API_KEYS": "k1", "INGEST_SIGNING_SECRET": "s"}
	call("commits", keyed, map[string]string{"X-API-Key": "k1"})
	call("commits", keyed, map[string]string{"X-API-Key": "k1"})
	call("commits", keyed, nil)
	call("telemetry", signed, map[string]string{"X-Signature-256": sign("s", commitsBody)})
	call("telemetry", signed, nil)
	call("incidents", both, map[string]string{"X-API-Key": "k1", "X-Signature-256": sign("s", commitsBody)})
	call("incidents", nil, nil)

	var out bytes.Buffer
	if err := metrics.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for series, want := range map[string]string{
		`devhealth_ingest_legacy_requests_total{route="commits",key_class="api_key",outcome="accepted"}`:             "2",
		`devhealth_ingest_legacy_requests_total{route="commits",key_class="api_key",outcome="rejected"}`:             "1",
		`devhealth_ingest_legacy_requests_total{route="telemetry",key_class="signature",outcome="accepted"}`:         "1",
		`devhealth_ingest_legacy_requests_total{route="telemetry",key_class="signature",outcome="rejected"}`:         "1",
		`devhealth_ingest_legacy_requests_total{route="incidents",key_class="api_key_signature",outcome="accepted"}`: "1",
		`devhealth_ingest_legacy_requests_total{route="incidents",key_class="none",outcome="rejected"}`:              "1",
		`devhealth_ingest_legacy_requests_total{route="work-items",key_class="none",outcome="rejected"}`:             "0",
	} {
		if !strings.Contains(text, series+" "+want+"\n") {
			t.Errorf("missing %s %s in\n%s", series, want, text)
		}
	}
	if got := strings.Count(text, "\ndevhealth_ingest_legacy_requests_total{"); got != 6*7 {
		t.Errorf("%d request series, want %d (fixed vocabulary)", got, 6*7)
	}
}
