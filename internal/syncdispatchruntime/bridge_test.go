package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPBridgeSendsOnlyAuthenticatedReference(t *testing.T) {
	t.Parallel()
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		path = request.URL.Path
		if request.Method != http.MethodPost || request.Header.Get("Authorization") != "Bearer bridge-token" ||
			request.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request method=%s auth=%q content-type=%q", request.Method, request.Header.Get("Authorization"), request.Header.Get("Content-Type"))
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if len(payload) != 4 || payload["organization_id"] != testOrg || payload["sync_run_id"] != testRun ||
			payload["outbox_id"] != testOutbox || payload["route_generation"] != float64(7) {
			t.Errorf("payload=%#v", payload)
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bridge, err := NewHTTPBridge(HTTPBridgeConfig{
		BaseURL: server.URL, BearerToken: "bridge-token", Timeout: time.Second, AllowInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	args := DispatchSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: testOrg, RunID: testRun, DispatchOutbox: testOutbox, RouteGeneration: 7,
	}}
	if err := bridge.Dispatch(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if path != "/api/internal/worker-sync/dispatch" {
		t.Fatalf("path=%q", path)
	}
}

func TestHTTPBridgeRejectsUnsafeOrUnsuccessfulDelivery(t *testing.T) {
	t.Parallel()
	if _, err := NewHTTPBridge(HTTPBridgeConfig{
		BaseURL: "http://worker.example", BearerToken: "token", Timeout: time.Second,
	}); !errors.Is(err, ErrInvalidBridge) {
		t.Fatalf("insecure bridge error=%v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	bridge, err := NewHTTPBridge(HTTPBridgeConfig{
		BaseURL: server.URL, BearerToken: "token", Timeout: time.Second, AllowInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	args := FinalizeSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: testOrg, RunID: testRun, DispatchOutbox: testOutbox, RouteGeneration: 1,
	}}
	if err := bridge.Finalize(context.Background(), args); !errors.Is(err, ErrBridgeRequest) {
		t.Fatalf("Finalize() error=%v", err)
	}
}

// TestHTTPBridgePopulatesReferenceDiscoveryWithIdentifiersOnly pins the
// CHAOS-4175 (b) ruling's security property directly at the wire level: the
// request body carries exactly organization_id/sync_run_id (no field this
// call could carry credential material through), and the response body's
// summary dict is decoded and returned verbatim.
func TestHTTPBridgePopulatesReferenceDiscoveryWithIdentifiersOnly(t *testing.T) {
	t.Parallel()
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		path = request.URL.Path
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if len(payload) != 2 || payload["organization_id"] != testOrg || payload["sync_run_id"] != testRun {
			t.Errorf("payload=%#v want identifiers only", payload)
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{
			"status":               "success",
			"provider":             "linear",
			"reference_team_keys":  []string{"ENG"},
			"reference_sprint_ids": []string{"sprint-1"},
		})
	}))
	defer server.Close()

	bridge, err := NewHTTPBridge(HTTPBridgeConfig{
		BaseURL: server.URL, BearerToken: "bridge-token", Timeout: time.Second, AllowInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := bridge.PopulateReferenceDiscovery(context.Background(), testOrg, testRun)
	if err != nil {
		t.Fatalf("PopulateReferenceDiscovery: %v", err)
	}
	if path != "/api/internal/worker-sync/reference-discovery-populate" {
		t.Fatalf("path=%q", path)
	}
	if summary["status"] != "success" || summary["provider"] != "linear" {
		t.Fatalf("summary=%#v", summary)
	}
	teamKeys, ok := summary["reference_team_keys"].([]any)
	if !ok || len(teamKeys) != 1 || teamKeys[0] != "ENG" {
		t.Fatalf("summary[reference_team_keys]=%#v", summary["reference_team_keys"])
	}
}

// TestHTTPBridgePopulateReferenceDiscoveryFailsClosedOnBadInputOrResponse
// mutation-proves both failure edges: an invalid identifier never reaches
// the network, and a non-2xx or non-JSON response is a genuine error, not
// a silently empty summary.
func TestHTTPBridgePopulateReferenceDiscoveryFailsClosedOnBadInputOrResponse(t *testing.T) {
	t.Parallel()
	var handler func(http.ResponseWriter, *http.Request)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		handler(response, request)
	}))
	defer server.Close()
	bridge, err := NewHTTPBridge(HTTPBridgeConfig{
		BaseURL: server.URL, BearerToken: "bridge-token", Timeout: time.Second, AllowInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := bridge.PopulateReferenceDiscovery(context.Background(), "not-a-uuid", testRun); !errors.Is(err, ErrInvalidBridge) {
		t.Fatalf("invalid orgID error=%v want=%v", err, ErrInvalidBridge)
	}

	handler = func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusServiceUnavailable)
	}
	if _, err := bridge.PopulateReferenceDiscovery(context.Background(), testOrg, testRun); !errors.Is(err, ErrBridgeRequest) {
		t.Fatalf("non-2xx error=%v want=%v", err, ErrBridgeRequest)
	}

	handler = func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("not json"))
	}
	if _, err := bridge.PopulateReferenceDiscovery(context.Background(), testOrg, testRun); !errors.Is(err, ErrBridgeRequest) {
		t.Fatalf("invalid JSON body error=%v want=%v", err, ErrBridgeRequest)
	}
}

func TestHTTPBridgeConnectionBudgetDoesNotCapWholeRequest(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		time.Sleep(150 * time.Millisecond)
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	bridge, err := NewHTTPBridge(HTTPBridgeConfig{
		BaseURL: server.URL, BearerToken: "token", Timeout: 100 * time.Millisecond, AllowInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bridge.client.Timeout != 0 {
		t.Fatalf("bridge whole-request timeout=%v want=0", bridge.client.Timeout)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	args := DispatchSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: testOrg, RunID: testRun, DispatchOutbox: testOutbox, RouteGeneration: 7,
	}}
	if err := bridge.Dispatch(ctx, args); err != nil {
		t.Fatal(err)
	}
}
