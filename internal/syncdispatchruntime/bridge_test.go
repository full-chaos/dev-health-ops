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

func TestHTTPBridgeRejectsUnsafeConfig(t *testing.T) {
	t.Parallel()
	if _, err := NewHTTPBridge(HTTPBridgeConfig{
		BaseURL: "http://worker.example", BearerToken: "token", Timeout: time.Second,
	}); !errors.Is(err, ErrInvalidBridge) {
		t.Fatalf("insecure bridge error=%v", err)
	}
}

func TestHTTPBridgeConnectionBudgetDoesNotCapWholeRequest(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		time.Sleep(150 * time.Millisecond)
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{"estimates": map[string]any{}})
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
	if _, err := bridge.DispatchBudgetEstimate(ctx, testOrg, testRun, []string{testUnit}); err != nil {
		t.Fatal(err)
	}
}
