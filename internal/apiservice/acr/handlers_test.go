package acr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeStore is a hand-controlled EntitlementStore for handler-level tests --
// the Postgres-backed store's own behaviour is proven separately (store_test.go,
// the integration test, and the live-Python oracle).
type fakeStore struct {
	entitlement Entitlement
	err         error
	readyErr    error
	gotOrgID    string
}

func (f *fakeStore) Ready(context.Context) error { return f.readyErr }

func (f *fakeStore) Lookup(_ context.Context, orgID string) (Entitlement, error) {
	f.gotOrgID = orgID
	return f.entitlement, f.err
}

func newTestServer(store EntitlementStore) *httptest.Server {
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{
		Store: store, Auth: newFakeCredentials(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	return httptest.NewServer(mux)
}

func TestHealthHandlerReadyStoreIsTheStaticBody(t *testing.T) {
	server := newTestServer(&fakeStore{})
	defer server.Close()

	response, err := getAuthed(server.URL + "/api/v1/internal/acr/health")
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

// TestHealthHandlerUnusableStoreIsServiceUnavailable: Python's health route
// answers 503 when its Postgres read fails, and acr reads a non-200 health
// as "entitlements unavailable".
func TestHealthHandlerUnusableStoreIsServiceUnavailable(t *testing.T) {
	for name, store := range map[string]EntitlementStore{
		"nil store":      nil,
		"ping fails":     &fakeStore{readyErr: errors.Join(errors.New("connection refused"), ErrUnavailable)},
		"any ping error": &fakeStore{readyErr: errors.New("boom")},
	} {
		t.Run(name, func(t *testing.T) {
			server := newTestServer(store)
			defer server.Close()
			response, err := getAuthed(server.URL + "/api/v1/internal/acr/health")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			raw, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusServiceUnavailable || string(raw) != `{"detail":"Service unavailable"}` {
				t.Fatalf("health = %d %q, want 503 {\"detail\":\"Service unavailable\"}", response.StatusCode, raw)
			}
		})
	}
}

func TestEntitlementHandlerSuccess(t *testing.T) {
	for _, serviceModel := range []bool{false, true} {
		t.Run(fmt.Sprintf("acr_service_model=%t", serviceModel), func(t *testing.T) {
			testEntitlementHandlerSuccess(t, serviceModel)
		})
	}
}

func testEntitlementHandlerSuccess(t *testing.T, serviceModel bool) {
	store := &fakeStore{entitlement: Entitlement{OrgID: "the-org-id", AgentContextRuntime: true, ACRServiceModel: serviceModel}}
	server := newTestServer(store)
	defer server.Close()

	response, err := getAuthed(server.URL + "/api/v1/internal/acr/entitlements/the-org-id")
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
		"acr_service_model":     serviceModel,
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
		{"wrapped malformed id", errors.Join(errors.New("bad"), ErrOrgNotFound), http.StatusNotFound, "Not found"},
		{"store unavailable", ErrUnavailable, http.StatusServiceUnavailable, "Service unavailable"},
		{"wrapped unavailable", errors.Join(errors.New("db down"), ErrUnavailable), http.StatusServiceUnavailable, "Service unavailable"},
		{"unclassified error", errors.New("boom"), http.StatusInternalServerError, "Internal Server Error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeStore{err: test.err}
			server := newTestServer(store)
			defer server.Close()

			response, err := getAuthed(server.URL + "/api/v1/internal/acr/entitlements/org-1")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.wantStatus)
			}
			raw, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			// FastAPI's JSONResponse bytes: compact, no trailing newline.
			want := `{"detail":"` + test.wantDetail + `"}`
			if string(raw) != want {
				t.Fatalf("body = %q, want %q", raw, want)
			}
		})
	}
}

func TestEntitlementHandlerUnknownOrgCarriesTheStableCode(t *testing.T) {
	server := newTestServer(&fakeStore{err: ErrOrgUnknown})
	defer server.Close()
	response, err := getAuthed(server.URL + "/api/v1/internal/acr/entitlements/6f1c0c1e-1d4b-4a55-9a39-0d0d6f3b8a10")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", response.StatusCode)
	}
	if want := `{"detail":"Not found","code":"acr_organization_unknown"}`; string(raw) != want {
		t.Fatalf("body = %q, want %q", raw, want)
	}
}

func TestEntitlementHandlerOnlyTheUnknownOrgAnswerCarriesTheCode(t *testing.T) {
	for _, storeErr := range []error{ErrOrgNotFound, ErrUnavailable, errors.New("boom")} {
		server := newTestServer(&fakeStore{err: storeErr})
		response, err := getAuthed(server.URL + "/api/v1/internal/acr/entitlements/org-1")
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(response.Body)
		response.Body.Close()
		server.Close()
		if strings.Contains(string(raw), UnknownOrganizationCode) {
			t.Fatalf("error %v answered with the unknown-organization code: %s", storeErr, raw)
		}
	}
}

func TestEntitlementHandlerNilStoreIsServiceUnavailable(t *testing.T) {
	server := newTestServer(nil)
	defer server.Close()

	response, err := getAuthed(server.URL + "/api/v1/internal/acr/entitlements/org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestRoutesSetNoServerDefaultOverrides(t *testing.T) {
	for _, route := range Routes(Deps{}) {
		if route.RateLimitPerSecond != 0 || route.RateLimitBurst != 0 || route.MaxBodyBytes != 0 {
			t.Fatalf("route %s %s sets a server-default override; internal routes use plain server defaults, nothing route-specific",
				route.Method, route.Pattern)
		}
	}
}

// TestResponseBodyHasNoTrailingBytesAfterTheJSONValue: acr's own client
// decoder (internal/entitlements/response.go in the acr repo) reads the body
// with json.Decoder and requires io.EOF right after the value, so no
// trailing bytes may follow it. The policy writers send none.
func TestResponseBodyHasNoTrailingBytesAfterTheJSONValue(t *testing.T) {
	server := newTestServer(&fakeStore{})
	defer server.Close()

	response, err := getAuthed(server.URL + "/api/v1/internal/acr/health")
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
	if raw[len(raw)-1] != '}' {
		t.Fatalf("body %q ends after the JSON value", raw)
	}
}
