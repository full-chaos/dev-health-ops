package legacyingest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const commitsBody = `{"org_id":"org-1","repo_url":"https://example.test/r.git","items":[{"hash":"abc","message":"m","author_name":"a","author_email":"a@example.test","author_when":"2026-01-01T00:00:00Z"}]}`

func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func serve(t *testing.T, values map[string]string, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	routes := Routes(Deps{Getenv: env(values), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	var target http.Handler
	for _, route := range routes {
		if route.Pattern == "/api/v1/ingest/commits" {
			target = route.Handler
		}
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/commits", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	target.ServeHTTP(recorder, request)
	return recorder
}

func sign(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestAuthenticationMatrix(t *testing.T) {
	for _, item := range []struct {
		name    string
		env     map[string]string
		headers map[string]string
		body    string
		status  int
		detail  string
	}{
		{"nothing configured, production default", nil, nil, commitsBody, 401, `{"detail":"Invalid API key"}`},
		{"nothing configured, ENVIRONMENT=prod", map[string]string{"ENVIRONMENT": "prod"}, nil, commitsBody, 401, `{"detail":"Invalid API key"}`},
		{"nothing configured, development", map[string]string{"ENVIRONMENT": " Development "}, nil, commitsBody, 202, ""},
		{"nothing configured, APP_ENV=local", map[string]string{"APP_ENV": "local"}, nil, commitsBody, 202, ""},
		{"ENVIRONMENT wins over APP_ENV", map[string]string{"ENVIRONMENT": "prod", "APP_ENV": "dev"}, nil, commitsBody, 401, `{"detail":"Invalid API key"}`},
		{"api key missing", map[string]string{"INGEST_API_KEYS": "k1,k2"}, nil, commitsBody, 401, `{"detail":"Invalid API key"}`},
		{"api key wrong", map[string]string{"INGEST_API_KEYS": "k1,k2"}, map[string]string{"X-API-Key": "k3"}, commitsBody, 401, `{"detail":"Invalid API key"}`},
		{"api key right, stripped list", map[string]string{"INGEST_API_KEYS": " k1 , ,k2 "}, map[string]string{"X-API-Key": "k2"}, commitsBody, 202, ""},
		{"blank key list is nothing configured", map[string]string{"INGEST_API_KEYS": "  ,  "}, nil, commitsBody, 401, `{"detail":"Invalid API key"}`},
		{"signature missing", map[string]string{"INGEST_SIGNING_SECRET": "s"}, nil, commitsBody, 401, `{"detail":"Invalid signature"}`},
		{"signature without prefix", map[string]string{"INGEST_SIGNING_SECRET": "s"}, map[string]string{"X-Signature-256": strings.TrimPrefix(sign("s", commitsBody), "sha256=")}, commitsBody, 401, `{"detail":"Invalid signature"}`},
		{"signature wrong", map[string]string{"INGEST_SIGNING_SECRET": "s"}, map[string]string{"X-Signature-256": sign("other", commitsBody)}, commitsBody, 401, `{"detail":"Invalid signature"}`},
		{"signature right", map[string]string{"INGEST_SIGNING_SECRET": "s"}, map[string]string{"X-Signature-256": sign("s", commitsBody)}, commitsBody, 202, ""},
		{"signature non-ASCII is compare_digest's TypeError", map[string]string{"INGEST_SIGNING_SECRET": "s"}, map[string]string{"X-Signature-256": "sha256=\xe9"}, commitsBody, 500, `{"detail":"Internal Server Error"}`},
		{"both configured, key wrong first", map[string]string{"INGEST_API_KEYS": "k", "INGEST_SIGNING_SECRET": "s"}, map[string]string{"X-Signature-256": sign("s", commitsBody)}, commitsBody, 401, `{"detail":"Invalid API key"}`},
		{"both configured, both right", map[string]string{"INGEST_API_KEYS": "k", "INGEST_SIGNING_SECRET": "s"}, map[string]string{"X-API-Key": "k", "X-Signature-256": sign("s", commitsBody)}, commitsBody, 202, ""},
		// FastAPI decodes the body before any dependency: a decode failure is
		// answered without a credential check; a validation error only after it.
		{"bad json beats auth", nil, nil, `{`, 422, `"type":"json_invalid"`},
		{"validation error after auth", nil, nil, `{}`, 401, `{"detail":"Invalid API key"}`},
		{"validation error once authenticated", map[string]string{"ENVIRONMENT": "dev"}, nil, `{}`, 422, `"type":"missing"`},
	} {
		t.Run(item.name, func(t *testing.T) {
			got := serve(t, item.env, item.body, item.headers)
			if got.Code != item.status {
				t.Fatalf("status %d (%s), want %d", got.Code, got.Body.String(), item.status)
			}
			if item.detail != "" && !strings.Contains(got.Body.String(), item.detail) {
				t.Fatalf("body %s, want it to contain %s", got.Body.String(), item.detail)
			}
		})
	}
}

func TestAcceptedResponse(t *testing.T) {
	got := serve(t, map[string]string{"ENVIRONMENT": "dev"}, commitsBody, nil)
	body := got.Body.String()
	if got.Code != 202 || !strings.HasPrefix(body, `{"ingestion_id":"`) || !strings.HasSuffix(body, `","status":"accepted","items_received":1,"stream":"ingest:org-1:commits"}`) {
		t.Fatalf("%d %s", got.Code, body)
	}
}

func TestPayloadDumpMatchesModelDumpJSON(t *testing.T) {
	// Every field in declaration order, defaults included, the offset and
	// microseconds as pydantic writes them, an ignored extra key dropped.
	parsed, problems := batchParser(true, parseCommit)(mustBody(t, `{"repo_url":"r","org_id":"o","zzz":1,"items":[{"hash":"h","message":"m","author_name":"a","author_email":"e","author_when":"2026-01-01T02:00:00.5+02:00","committer_when":1767225600,"extra":true}]}`))
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	dump := marshal(t, parsed)
	want := `{"org_id":"o","repo_url":"r","items":[{"hash":"h","message":"m","author_name":"a","author_email":"e","author_when":"2026-01-01T02:00:00.500000+02:00","committer_name":null,"committer_email":null,"committer_when":"2026-01-01T00:00:00Z","parents":1}]}`
	if dump != want {
		t.Fatalf("dump %s\nwant %s", dump, want)
	}
}

type scriptedStore struct {
	claim       func(key string) (bool, error)
	appendError error
	claims      []string
	appended    [][2]string
}

func (s *scriptedStore) Append(_ context.Context, stream, _ string, payload string) error {
	s.appended = append(s.appended, [2]string{stream, payload})
	return s.appendError
}

func (s *scriptedStore) Claim(_ context.Context, key string) (bool, error) {
	s.claims = append(s.claims, key)
	return s.claim(key)
}

func post(t *testing.T, store Store, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	routes := Routes(Deps{Store: store, Getenv: env(map[string]string{"ENVIRONMENT": "dev"}), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/commits", strings.NewReader(commitsBody))
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	routes[0].Handler.ServeHTTP(recorder, request)
	return recorder
}

func TestIdempotencyKey(t *testing.T) {
	seen := map[string]bool{}
	store := &scriptedStore{claim: func(key string) (bool, error) {
		if seen[key] {
			return false, nil
		}
		seen[key] = true
		return true, nil
	}}
	if got := post(t, store, map[string]string{"X-Idempotency-Key": "k1"}); got.Code != 202 {
		t.Fatalf("first: %d %s", got.Code, got.Body.String())
	}
	got := post(t, store, map[string]string{"X-Idempotency-Key": "k1"})
	if got.Code != 409 || strings.TrimSpace(got.Body.String()) != `{"detail":"Duplicate request"}` {
		t.Fatalf("repeat: %d %s", got.Code, got.Body.String())
	}
	if len(store.appended) != 1 {
		t.Fatalf("a duplicate must not be streamed: %v", store.appended)
	}
	if got := post(t, store, map[string]string{"X-Idempotency-Key": "k2"}); got.Code != 202 {
		t.Fatalf("other key: %d", got.Code)
	}
	// No header, or an empty one, claims nothing.
	before := len(store.claims)
	post(t, store, nil)
	post(t, store, map[string]string{"X-Idempotency-Key": ""})
	if len(store.claims) != before {
		t.Fatalf("claims made without a key: %v", store.claims[before:])
	}
	if store.claims[0] != "idem:k1" {
		t.Fatalf("key = %q, want idem:k1", store.claims[0])
	}
}

func TestValkeyFailuresDegrade(t *testing.T) {
	// A failing claim goes on (Python logs and skips the check); a failing
	// stream write still answers 202, the payload just is not streamed.
	store := &scriptedStore{claim: func(string) (bool, error) { return false, errors.New("down") }, appendError: errors.New("down")}
	if got := post(t, store, map[string]string{"X-Idempotency-Key": "k"}); got.Code != 202 {
		t.Fatalf("%d %s", got.Code, got.Body.String())
	}
	if got := post(t, nil, map[string]string{"X-Idempotency-Key": "k"}); got.Code != 202 {
		t.Fatalf("no store: %d", got.Code)
	}
}

func TestInvalidRequestConsumesTheIdempotencyKey(t *testing.T) {
	// FastAPI runs the dependencies before it reports the body's validation
	// errors, so a request refused with 422 has already claimed its key.
	store := &scriptedStore{claim: func(string) (bool, error) { return true, nil }}
	routes := Routes(Deps{Store: store, Getenv: env(map[string]string{"ENVIRONMENT": "dev"}), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/commits", strings.NewReader(`{}`))
	request.Header.Set("X-Idempotency-Key", "k")
	recorder := httptest.NewRecorder()
	routes[0].Handler.ServeHTTP(recorder, request)
	if recorder.Code != 422 || len(store.claims) != 1 || len(store.appended) != 0 {
		t.Fatalf("%d claims=%v appended=%v", recorder.Code, store.claims, store.appended)
	}
}

const telemetryBody = `{"org_id":"org-1","items":[{"signal_type":"s","signal_count":1,"session_count":1,"environment":"e","bucket_start":"2026-01-01T00:00:00Z","bucket_end":"2026-01-01T01:00:00Z","dedupe_key":"d"}]}`

type failingClickHouse struct{ err error }

func (f failingClickHouse) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, f.err
}

func postTelemetry(t *testing.T, clickhouse ClickHouse) *httptest.ResponseRecorder {
	t.Helper()
	routes := Routes(Deps{ClickHouse: clickhouse, Getenv: env(map[string]string{"ENVIRONMENT": "dev"}), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	for _, route := range routes {
		if route.Pattern == "/api/v1/ingest/telemetry" {
			request := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/telemetry", strings.NewReader(telemetryBody))
			recorder := httptest.NewRecorder()
			route.Handler.ServeHTTP(recorder, request)
			return recorder
		}
	}
	t.Fatal("no telemetry route")
	return nil
}

func TestTelemetryWithoutClickHouseIsAcceptedAndSkipped(t *testing.T) {
	got := postTelemetry(t, nil)
	if got.Code != 202 || !strings.HasSuffix(got.Body.String(), `"stream":"ingest:org-1:telemetry"}`) {
		t.Fatalf("%d %s", got.Code, got.Body.String())
	}
}

func TestTelemetryInsertFailureIsTheUnhandled500(t *testing.T) {
	got := postTelemetry(t, failingClickHouse{err: errors.New("down")})
	if got.Code != 500 || strings.TrimSpace(got.Body.String()) != `{"detail":"Internal Server Error"}` {
		t.Fatalf("%d %s", got.Code, got.Body.String())
	}
}
