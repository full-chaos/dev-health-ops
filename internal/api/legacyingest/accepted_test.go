package legacyingest

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

const acceptedName = "devhealth_ingest_legacy_accepted_total"

func acceptedPost(t *testing.T, metrics *Metrics, path string, values, headers map[string]string, body string) int {
	t.Helper()
	for _, route := range Routes(Deps{Metrics: metrics, Getenv: env(values), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}) {
		if route.Pattern != "/api/v1/ingest/"+path {
			continue
		}
		request := httptest.NewRequest(http.MethodPost, route.Pattern, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		for key, value := range headers {
			request.Header.Set(key, value)
		}
		recorder := httptest.NewRecorder()
		route.Handler.ServeHTTP(recorder, request)
		return recorder.Code
	}
	t.Fatalf("no route %s", path)
	return 0
}

func acceptedScrape(t *testing.T, metrics *Metrics) string {
	t.Helper()
	var out bytes.Buffer
	if err := metrics.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func acceptedValue(t *testing.T, metrics *Metrics, mechanism string) string {
	t.Helper()
	prefix := acceptedName + `{mechanism="` + mechanism + `"} `
	for _, line := range strings.Split(acceptedScrape(t, metrics), "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return "absent"
}

func TestAcceptedCounterByMechanism(t *testing.T) {
	keyed := map[string]string{"INGEST_API_KEYS": "k1"}
	signed := map[string]string{"INGEST_SIGNING_SECRET": "s"}
	both := map[string]string{"INGEST_API_KEYS": "k1", "INGEST_SIGNING_SECRET": "s"}
	signature := map[string]string{"X-Signature-256": sign("s", commitsBody)}
	bothHeaders := map[string]string{"X-API-Key": "k1", "X-Signature-256": sign("s", commitsBody)}
	for _, tc := range []struct {
		name            string
		values, headers map[string]string
		apiKey, hmac    string
	}{
		{"key only", keyed, map[string]string{"X-API-Key": "k1"}, "1", "0"},
		{"secret only", signed, signature, "0", "1"},
		{"both pass count once as hmac", both, bothHeaders, "0", "1"},
	} {
		for _, path := range []string{"commits", "pull-requests", "work-items", "deployments", "incidents", "telemetry"} {
			metrics := NewMetrics()
			acceptedPost(t, metrics, path, tc.values, tc.headers, commitsBody)
			if got := acceptedValue(t, metrics, "api_key"); got != tc.apiKey {
				t.Errorf("%s %s: api_key = %s, want %s", tc.name, path, got, tc.apiKey)
			}
			if got := acceptedValue(t, metrics, "hmac"); got != tc.hmac {
				t.Errorf("%s %s: hmac = %s, want %s", tc.name, path, got, tc.hmac)
			}
		}
	}
}

func TestAcceptedCounterOncePerRequest(t *testing.T) {
	metrics := NewMetrics()
	keyed := map[string]string{"INGEST_API_KEYS": "k1"}
	headers := map[string]string{"X-API-Key": "k1"}
	acceptedPost(t, metrics, "commits", keyed, headers, commitsBody)
	acceptedPost(t, metrics, "commits", keyed, headers, commitsBody)
	acceptedPost(t, metrics, "incidents", keyed, headers, commitsBody)
	if got := acceptedValue(t, metrics, "api_key"); got != "3" {
		t.Fatalf("api_key = %s after three accepted requests, want 3", got)
	}
}

func TestAcceptedCounterNotCountedOnRefusal(t *testing.T) {
	metrics := NewMetrics()
	keyed := map[string]string{"INGEST_API_KEYS": "k1"}
	signed := map[string]string{"INGEST_SIGNING_SECRET": "s"}
	both := map[string]string{"INGEST_API_KEYS": "k1", "INGEST_SIGNING_SECRET": "s"}
	acceptedPost(t, metrics, "commits", nil, nil, commitsBody)
	acceptedPost(t, metrics, "commits", map[string]string{"ENVIRONMENT": "development"}, nil, commitsBody)
	acceptedPost(t, metrics, "commits", keyed, map[string]string{"X-API-Key": "wrong"}, commitsBody)
	acceptedPost(t, metrics, "commits", keyed, nil, commitsBody)
	acceptedPost(t, metrics, "commits", signed, map[string]string{"X-Signature-256": sign("other", commitsBody)}, commitsBody)
	acceptedPost(t, metrics, "commits", signed, map[string]string{"X-Signature-256": "sha256=\xe9"}, commitsBody)
	acceptedPost(t, metrics, "commits", both, map[string]string{"X-API-Key": "k1", "X-Signature-256": sign("other", commitsBody)}, commitsBody)
	acceptedPost(t, metrics, "commits", keyed, map[string]string{"X-API-Key": "k1"}, "{")
	for _, mechanism := range []string{"api_key", "hmac"} {
		if got := acceptedValue(t, metrics, mechanism); got != "0" {
			t.Errorf("%s = %s after refused requests only, want 0", mechanism, got)
		}
	}
}

func TestAcceptedCounterLabelSetIsClosed(t *testing.T) {
	metrics := NewMetrics()
	before := acceptedScrape(t, metrics)
	keyValue := "key-" + strings.Repeat("q", 12)
	acceptedPost(t, metrics, "commits", map[string]string{"INGEST_API_KEYS": keyValue}, map[string]string{"X-API-Key": keyValue}, commitsBody)
	text := acceptedScrape(t, metrics)
	for _, scrape := range []string{before, text} {
		var lines []string
		for _, line := range strings.Split(scrape, "\n") {
			if strings.HasPrefix(line, acceptedName+"{") {
				lines = append(lines, line)
			}
		}
		if len(lines) != 2 {
			t.Fatalf("%d accepted series, want exactly 2:\n%s", len(lines), scrape)
		}
		for _, line := range lines {
			if !regexp.MustCompile(`^` + acceptedName + `\{mechanism="(api_key|hmac)"\} [0-9]+$`).MatchString(line) {
				t.Errorf("series outside the closed label set: %s", line)
			}
		}
	}
	if strings.Contains(text, "dev_bypass") || strings.Contains(text, keyValue) {
		t.Fatalf("scrape carries a mechanism or value outside the closed set:\n%s", text)
	}
	if !strings.Contains(before, acceptedName+`{mechanism="api_key"} 0`) || !strings.Contains(before, acceptedName+`{mechanism="hmac"} 0`) {
		t.Fatalf("series are not present at zero before any request:\n%s", before)
	}
}

func TestAcceptedCounterStillCountsAfterLaterRejection(t *testing.T) {
	metrics := NewMetrics()
	keyed := map[string]string{"INGEST_API_KEYS": "k1"}
	headers := map[string]string{"X-API-Key": "k1"}
	if code := acceptedPost(t, metrics, "commits", keyed, headers, `{"org_id":"o"}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("status %d, want 422", code)
	}
	if got := acceptedValue(t, metrics, "api_key"); got != "1" {
		t.Fatalf("api_key = %s for a request whose credential was accepted, want 1", got)
	}
}

func TestAcceptedCounterNilMetrics(t *testing.T) {
	keyed := map[string]string{"INGEST_API_KEYS": "k1"}
	acceptedPost(t, nil, "commits", keyed, map[string]string{"X-API-Key": "k1"}, commitsBody)
}
