package legacyingest

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens/withissuer"
)

type countingStore struct {
	mu             sync.Mutex
	appends, claim int
}

func (s *countingStore) Append(context.Context, string, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appends++
	return nil
}
func (s *countingStore) Claim(context.Context, string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claim++
	return true, nil
}

type countingClickHouse struct {
	mu       sync.Mutex
	prepared int
}

func (c *countingClickHouse) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prepared++
	return nil, context.Canceled
}

func legacyRequest(route, body string, headers map[string]string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/"+route, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		r.Header.Set(name, value)
	}
	return r
}

// The six legacy ingest routes authenticate with a configured API key and/or an HMAC signature of the
// body. A user token of any kind, presented as the key, as the signature, and as the Authorization
// bearer, is refused 401 under every credential configuration (key only, signature only, both), before the
// batch is idempotency-claimed, streamed or written: counted on the store and the ClickHouse sink.
func TestLegacyIngestRefusesUserTokens(t *testing.T) {
	tokens := withissuer.All(t)
	configs := []struct {
		name   string
		env    map[string]string
		detail string
	}{
		{"api key only", map[string]string{"INGEST_API_KEYS": "the-real-key"}, "Invalid API key"},
		{"signature only", map[string]string{"INGEST_SIGNING_SECRET": "the-real-secret"}, "Invalid signature"},
		{"both", map[string]string{"INGEST_API_KEYS": "the-real-key", "INGEST_SIGNING_SECRET": "the-real-secret"}, "Invalid API key"},
	}
	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			store, sink := &countingStore{}, &countingClickHouse{}
			var logs bytes.Buffer
			routes := Routes(Deps{Store: store, ClickHouse: sink, Metrics: NewMetrics(),
				Getenv: func(key string) string { return cfg.env[key] },
				Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
			for _, route := range routes {
				name := strings.TrimPrefix(route.Pattern, "/api/v1/ingest/")
				body := batchText(name, `"org-1"`, `"https://example.test/r.git"`, "["+itemText(name, nil, nil)+"]")
				for _, tok := range tokens {
					recorder := httptest.NewRecorder()
					route.Handler.ServeHTTP(recorder, legacyRequest(name, body, map[string]string{
						"X-API-Key": tok.Value, "X-Signature-256": "sha256=" + tok.Value, "Authorization": "Bearer " + tok.Value,
					}))
					if recorder.Code != http.StatusUnauthorized || !strings.Contains(recorder.Body.String(), cfg.detail) {
						t.Errorf("%s with %s: %d %s, want 401 %s", name, tok.Kind, recorder.Code, recorder.Body.String(), cfg.detail)
					}
					usertokens.NoLeak(t, name+" body", recorder.Body.String(), tokens)
				}
			}
			if store.claim != 0 || store.appends != 0 {
				t.Errorf("a user token reached the stream store: %d claims, %d appends, want 0", store.claim, store.appends)
			}
			if sink.prepared != 0 {
				t.Errorf("a user token reached ClickHouse: %d prepared batches, want 0", sink.prepared)
			}
			usertokens.NoLeak(t, "the logs", logs.String(), tokens)
		})
	}
}

// The control: the configured key passes the same gate on the same routes and reaches the store, so the
// zeros above are the gate's work.
func TestLegacyIngestControlTheConfiguredKeyReachesTheStore(t *testing.T) {
	store := &countingStore{}
	routes := Routes(Deps{Store: store, Getenv: func(key string) string {
		return map[string]string{"INGEST_API_KEYS": "the-real-key"}[key]
	}, Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))})
	for _, route := range routes {
		name := strings.TrimPrefix(route.Pattern, "/api/v1/ingest/")
		if name != "commits" {
			continue
		}
		body := batchText(name, `"org-1"`, `"https://example.test/r.git"`, "["+itemText(name, nil, nil)+"]")
		recorder := httptest.NewRecorder()
		route.Handler.ServeHTTP(recorder, legacyRequest(name, body, map[string]string{"X-API-Key": "the-real-key", "Idempotency-Key": "k-1"}))
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("the configured key was refused: %d %s", recorder.Code, recorder.Body)
		}
	}
	if store.appends == 0 {
		t.Fatal("an authenticated batch never reached the stream store: the counters detect nothing")
	}
}
