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

const testUnit = "00000000-0000-4000-8000-000000000030"

func TestHTTPBridgeDispatchBudgetEstimateSendsIdentifiersOnly(t *testing.T) {
	t.Parallel()
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		path = request.URL.Path
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if len(payload) != 3 || payload["organization_id"] != testOrg || payload["sync_run_id"] != testRun {
			t.Errorf("payload=%#v want identifiers only", payload)
		}
		unitIDs, ok := payload["unit_ids"].([]any)
		if !ok || len(unitIDs) != 1 || unitIDs[0] != testUnit {
			t.Errorf("payload[unit_ids]=%#v", payload["unit_ids"])
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{
			"estimates": map[string]any{
				testUnit: []map[string]any{
					{
						"bucket": map[string]any{
							"provider": "github", "org_id": testOrg, "host": "api.github.com",
							"credential_fingerprint": "fp-1", "dimension": "rest_core",
						},
						"estimated_units": 42,
						"confidence":      "high",
						"route_family":    "work-items",
						"notes":           []string{"first pass"},
					},
				},
			},
		})
	}))
	defer server.Close()

	bridge, err := NewHTTPBridge(HTTPBridgeConfig{
		BaseURL: server.URL, BearerToken: "bridge-token", Timeout: time.Second, AllowInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	estimates, err := bridge.DispatchBudgetEstimate(context.Background(), testOrg, testRun, []string{testUnit})
	if err != nil {
		t.Fatalf("DispatchBudgetEstimate: %v", err)
	}
	if path != "/api/internal/worker-sync/dispatch-budget-estimate" {
		t.Fatalf("path=%q", path)
	}
	unitEstimates, ok := estimates[testUnit]
	if !ok || len(unitEstimates) != 1 {
		t.Fatalf("estimates[testUnit]=%#v", estimates[testUnit])
	}
	got := unitEstimates[0]
	if got.Bucket.Provider != "github" || got.Bucket.Host != "api.github.com" || got.Bucket.CredentialFingerprint != "fp-1" ||
		got.Bucket.Dimension != "rest_core" || got.EstimatedUnits != 42 || got.Confidence != "high" ||
		got.RouteFamily != "work-items" || len(got.Notes) != 1 || got.Notes[0] != "first pass" {
		t.Fatalf("estimate=%#v", got)
	}
}

// TestHTTPBridgeDispatchBudgetEstimateFailsClosedOnBadInputOrResponse
// mutation-proves the failure edges: an empty unit id list never reaches
// the network, a non-2xx or non-JSON response is a genuine error, and --
// the strict-decode half of the ruling -- a response carrying a field the
// Go schema doesn't declare is rejected outright rather than silently
// ignored.
func TestHTTPBridgeDispatchBudgetEstimateFailsClosedOnBadInputOrResponse(t *testing.T) {
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

	if _, err := bridge.DispatchBudgetEstimate(context.Background(), testOrg, testRun, nil); !errors.Is(err, ErrInvalidBridge) {
		t.Fatalf("empty unit ids error=%v want=%v", err, ErrInvalidBridge)
	}

	handler = func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusServiceUnavailable)
	}
	if _, err := bridge.DispatchBudgetEstimate(context.Background(), testOrg, testRun, []string{testUnit}); !errors.Is(err, ErrBridgeRequest) {
		t.Fatalf("non-2xx error=%v want=%v", err, ErrBridgeRequest)
	}

	handler = func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("not json"))
	}
	if _, err := bridge.DispatchBudgetEstimate(context.Background(), testOrg, testRun, []string{testUnit}); !errors.Is(err, ErrBridgeRequest) {
		t.Fatalf("invalid JSON body error=%v want=%v", err, ErrBridgeRequest)
	}

	handler = func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{
			"estimates": map[string]any{},
			// A field the Go schema does not declare -- strict decode must
			// reject this outright, matching extra="forbid" on the Python
			// request model's own enforcement of the identical discipline
			// in the other direction.
			"unexpected_field": "should never be silently ignored",
		})
	}
	if _, err := bridge.DispatchBudgetEstimate(context.Background(), testOrg, testRun, []string{testUnit}); !errors.Is(err, ErrBridgeRequest) {
		t.Fatalf("unknown response field error=%v want=%v", err, ErrBridgeRequest)
	}
}
