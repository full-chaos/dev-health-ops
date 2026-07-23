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

func TestHTTPBridgePostsPostSyncReferenceToDedicatedEndpoint(t *testing.T) {
	t.Parallel()
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		path = request.URL.Path
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	bridge, err := NewHTTPBridge(HTTPBridgeConfig{
		BaseURL: server.URL, BearerToken: "token", Timeout: time.Second, AllowInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	args := PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: testOrg, RunID: testRun, DispatchOutbox: testOutbox, RouteGeneration: 2,
	}}
	if err := bridge.PostSync(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if path != "/api/internal/worker-sync/post-sync" {
		t.Fatalf("path=%q", path)
	}
}
