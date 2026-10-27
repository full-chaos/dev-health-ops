package legacyingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

const incidentsBody = `{"org_id":"org-1","repo_url":"u","items":[{"incident_id":"i","status":"open","started_at":"2026-01-01T00:00:00Z"}]}`

type downStore struct {
	mu         sync.Mutex
	appendErr  error
	releaseErr error
	claimed    map[string]bool
	appends    int
	released   []string
}

func (s *downStore) Append(context.Context, string, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appends++
	return s.appendErr
}

func (s *downStore) Claim(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed == nil {
		s.claimed = map[string]bool{}
	}
	if s.claimed[key] {
		return false, nil
	}
	s.claimed[key] = true
	return true, nil
}

func (s *downStore) Release(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = append(s.released, key)
	if s.releaseErr != nil {
		return s.releaseErr
	}
	delete(s.claimed, key)
	return nil
}

func postRoute(t *testing.T, store Store, logs io.Writer, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	routes := Routes(Deps{Store: store, Getenv: env(map[string]string{"INGEST_API_KEYS": devKey}), Logger: slog.New(slog.NewTextHandler(logs, nil))})
	for _, route := range routes {
		if route.Pattern != "/api/v1/ingest/"+path {
			continue
		}
		request := httptest.NewRequest(http.MethodPost, route.Pattern, strings.NewReader(body))
		request.Header.Set("X-API-Key", devKey)
		for key, value := range headers {
			request.Header.Set(key, value)
		}
		recorder := httptest.NewRecorder()
		route.Handler.ServeHTTP(recorder, request)
		return recorder
	}
	t.Fatalf("no route %s", path)
	return nil
}

func TestStreamWriteFailureAnswers503(t *testing.T) {
	for _, tc := range []struct{ path, body string }{{"commits", commitsBody}, {"incidents", incidentsBody}} {
		t.Run(tc.path, func(t *testing.T) {
			store := &downStore{appendErr: errors.New("valkey down")}
			got := postRoute(t, store, io.Discard, tc.path, tc.body, nil)
			if got.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503, body %s", got.Code, got.Body.String())
			}
			if got.Header().Get("Retry-After") != "1" {
				t.Fatalf("Retry-After = %q, want 1", got.Header().Get("Retry-After"))
			}
			if !strings.HasPrefix(got.Body.String(), `{"detail":"`) {
				t.Fatalf("body %s is not the detail envelope", got.Body.String())
			}
			for _, banned := range []string{"ingestion_id", `"accepted"`, `"stream"`} {
				if strings.Contains(got.Body.String(), banned) {
					t.Fatalf("body %s acknowledges the batch (%s)", got.Body.String(), banned)
				}
			}
			if store.appends != 1 {
				t.Fatalf("appends = %d, want exactly one attempt", store.appends)
			}
		})
	}
}

func TestStreamWriteFailureReleasesTheIdempotencyClaim(t *testing.T) {
	store := &downStore{appendErr: errors.New("valkey down")}
	headers := map[string]string{"X-Idempotency-Key": "k1"}
	if got := postRoute(t, store, io.Discard, "commits", commitsBody, headers); got.Code != http.StatusServiceUnavailable {
		t.Fatalf("first = %d, want 503", got.Code)
	}
	if len(store.released) != 1 || store.released[0] != "idem:k1" {
		t.Fatalf("released = %v, want [idem:k1]", store.released)
	}
	store.appendErr = nil
	if got := postRoute(t, store, io.Discard, "commits", commitsBody, headers); got.Code != http.StatusAccepted {
		t.Fatalf("retry with the same key = %d, want 202 (not 409)", got.Code)
	}
}

func TestStreamWriteFailureWithReleaseFailureStillAnswers503(t *testing.T) {
	store := &downStore{appendErr: errors.New("valkey down"), releaseErr: errors.New("valkey down")}
	var logs bytes.Buffer
	got := postRoute(t, store, &logs, "commits", commitsBody, map[string]string{"X-Idempotency-Key": "k2"})
	if got.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", got.Code)
	}
	if !strings.Contains(logs.String(), "idempotency claim not released") {
		t.Fatalf("no named WARN for the unreleased claim, logs: %s", logs.String())
	}
}

func TestNoStoreAndUnencodablePayloadStayAccepted(t *testing.T) {
	if got := postRoute(t, nil, io.Discard, "commits", commitsBody, nil); got.Code != http.StatusAccepted {
		t.Fatalf("no store = %d, want 202", got.Code)
	}
}
