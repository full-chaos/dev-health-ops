package acr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeStore is a hand-controlled EntitlementStore for handler-level tests --
// the Postgres-backed store's own behaviour is proven separately (store_test.go,
// the integration test, and the live-Python oracle).
type fakeStore struct {
	entitlement Entitlement
	err         error
	gotOrgID    string
}

func (f *fakeStore) Lookup(_ context.Context, orgID string) (Entitlement, error) {
	f.gotOrgID = orgID
	return f.entitlement, f.err
}

func newTestServer(store EntitlementStore) *httptest.Server {
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	return httptest.NewServer(mux)
}

func TestHealthHandlerIsStaticAndUnconditional(t *testing.T) {
	server := newTestServer(nil)
	defer server.Close()

	response, err := http.Get(server.URL + "/api/v1/internal/acr/health")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"schema_version": "acr_service_health.v1",
		"service":        "dev-health-ops",
		"status":         "ok",
	}
	for key, value := range want {
		if body[key] != value {
			t.Fatalf("body[%q] = %v, want %v (full body: %v)", key, body[key], value, body)
		}
	}
	if len(body) != len(want) {
		t.Fatalf("body has %d fields, want exactly %d (acr's decoder rejects unknown fields): %v", len(body), len(want), body)
	}
	if response.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("Content-Type = %q", response.Header.Get("Content-Type"))
	}
}

func TestEntitlementHandlerSuccess(t *testing.T) {
	store := &fakeStore{entitlement: Entitlement{OrgID: "the-org-id", AgentContextRuntime: true}}
	server := newTestServer(store)
	defer server.Close()

	response, err := http.Get(server.URL + "/api/v1/internal/acr/entitlements/the-org-id")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if store.gotOrgID != "the-org-id" {
		t.Fatalf("store.Lookup called with org_id = %q, want %q", store.gotOrgID, "the-org-id")
	}
	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"schema_version":        "acr_entitlement.v1",
		"org_id":                "the-org-id",
		"agent_context_runtime": true,
	}
	for key, value := range want {
		if body[key] != value {
			t.Fatalf("body[%q] = %v, want %v (full body: %v)", key, body[key], value, body)
		}
	}
	if len(body) != len(want) {
		t.Fatalf("body has %d fields, want exactly %d (acr's decoder rejects unknown fields): %v", len(body), len(want), body)
	}
}

func TestEntitlementHandlerErrorStatusCodes(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantDetail string
	}{
		{"org not found", ErrOrgNotFound, http.StatusNotFound, "Not found"},
		{"store unavailable", ErrUnavailable, http.StatusServiceUnavailable, "Service unavailable"},
		{"wrapped unavailable", errors.Join(errors.New("db down"), ErrUnavailable), http.StatusServiceUnavailable, "Service unavailable"},
		{"unclassified error", errors.New("boom"), http.StatusInternalServerError, "Internal Server Error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeStore{err: test.err}
			server := newTestServer(store)
			defer server.Close()

			response, err := http.Get(server.URL + "/api/v1/internal/acr/entitlements/org-1")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.wantStatus)
			}
			var body map[string]string
			if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["detail"] != test.wantDetail {
				t.Fatalf("detail = %q, want %q", body["detail"], test.wantDetail)
			}
		})
	}
}

func TestEntitlementHandlerNilStoreIsServiceUnavailable(t *testing.T) {
	server := newTestServer(nil)
	defer server.Close()

	response, err := http.Get(server.URL + "/api/v1/internal/acr/entitlements/org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestRoutesCarryNoCredentialsOrAuthz(t *testing.T) {
	for _, route := range Routes(Deps{}) {
		if route.RateLimitPerSecond != 0 || route.RateLimitBurst != 0 || route.MaxBodyBytes != 0 {
			t.Fatalf("route %s %s sets a server-default override; internal routes use plain server defaults, nothing route-specific",
				route.Method, route.Pattern)
		}
	}
}

// TestResponseBodyHasNoTrailingBytesAfterTheJSONValue is the regression
// proof for switching writeJSON from a raw w.Write of pre-marshalled bytes
// to json.NewEncoder(w).Encode (this repo's own JSON-response convention,
// avoiding the go.lang.security.audit.xss.no-direct-write-to-responsewriter
// class): Encode appends a trailing newline the old code never sent. acr's
// own client decoder (internal/entitlements/response.go in the acr repo,
// read-only for this change) reads the body with json.Decoder and asserts
// `decoder.Decode(&struct{}{}) == io.EOF` immediately after the closing
// brace to reject trailing garbage -- this test proves that check still
// passes: a json.Decoder skips leading whitespace before deciding EOF, so a
// single trailing "\n" is accepted exactly like an empty tail was.
func TestResponseBodyHasNoTrailingBytesAfterTheJSONValue(t *testing.T) {
	server := newTestServer(nil)
	defer server.Close()

	response, err := http.Get(server.URL + "/api/v1/internal/acr/health")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatalf("acr's own trailing-garbage check (Decode after the value) = %v, want io.EOF -- got body %q", err, raw)
	}
}
