package daily

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestHTTPCompatibilityExecutorSendsOnlyAuthoritativeIDs(t *testing.T) {
	requests := make([]map[string]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/internal/worker/daily-metrics/v1/execute" || request.Header.Get("Authorization") != "Bearer token" {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		var body map[string]string
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		requests = append(requests, body)
		_, _ = writer.Write([]byte(`{"status":"success"}`))
	}))
	defer server.Close()
	executor, err := NewHTTPCompatibilityExecutor(&http.Client{Timeout: time.Second}, HTTPCompatibilityConfig{Endpoint: server.URL + "/internal/worker/daily-metrics/v1/execute", BearerToken: "token"})
	if err != nil {
		t.Fatal(err)
	}
	run := Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"}
	if err := executor.ComputePartition(t.Context(), run, Partition{ID: testPartitionID, RunID: testRunID}, nil); err != nil {
		t.Fatal(err)
	}
	if err := executor.Finalize(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	want := []map[string]string{
		{"operation": "partition", "run_id": testRunID, "partition_id": testPartitionID},
		{"operation": "finalize", "run_id": testRunID},
	}
	if len(requests) != len(want) {
		t.Fatalf("requests = %#v", requests)
	}
	for index := range want {
		if len(requests[index]) != len(want[index]) {
			t.Fatalf("request %d leaked non-identity input: %#v", index, requests[index])
		}
		for key, value := range want[index] {
			if requests[index][key] != value {
				t.Fatalf("request %d = %#v, want %#v", index, requests[index], want[index])
			}
		}
	}
}

func TestHTTPCompatibilityRetryUsesAuthoritativeGenerationAndSkipsCompletedOutput(t *testing.T) {
	var mutex sync.Mutex
	writes := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body compatibilityRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		// The bridge resolves generation and ownership from the persisted IDs;
		// the request cannot choose either value. This models a crash after the
		// first successful response but before the Go lease is completed.
		authoritativeGeneration := map[string]string{testRunID: "daily-v1"}[body.RunID]
		authoritativeRun := map[string]string{testPartitionID: testRunID}[body.PartitionID]
		if authoritativeGeneration == "" || authoritativeRun != body.RunID {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		key := authoritativeGeneration + ":" + body.RunID + ":" + body.PartitionID
		mutex.Lock()
		status := "skipped"
		if writes[key] == 0 {
			writes[key]++
			status = "success"
		}
		mutex.Unlock()
		_ = json.NewEncoder(writer).Encode(compatibilityResponse{Status: status})
	}))
	defer server.Close()
	executor, err := NewHTTPCompatibilityExecutor(
		&http.Client{Timeout: time.Second},
		HTTPCompatibilityConfig{
			Endpoint:    server.URL + "/internal/worker/daily-metrics/v1/execute",
			BearerToken: "token",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"}
	partition := Partition{ID: testPartitionID, RunID: testRunID}
	if err := executor.ComputePartition(t.Context(), run, partition, nil); err != nil {
		t.Fatal(err)
	}
	if err := executor.ComputePartition(t.Context(), run, partition, nil); err != nil {
		t.Fatal(err)
	}
	if writes["daily-v1:"+testRunID+":"+testPartitionID] != 1 {
		t.Fatalf("compatibility retry duplicated authoritative output: %#v", writes)
	}
}

// CHAOS-4264: the Python bridge's error body now carries a bounded "reason"
// on both the 503 a failed execution returns and the 409 a refused claim
// returns. This is the falsifier for classifyCompatibilityError: an
// unrecognized/missing reason must still fall back to ErrUnavailable
// unchanged (no regression for every error shape that predates this
// ticket), while each of the three named reasons must map to its own
// sentinel so daily.go's retryCompatibilityError can attach the matching
// jobruntime.Reason.
func TestHTTPCompatibilityExecutorClassifiesBoundedFailureReasons(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
		body       string
		wantErr    error
	}{
		{
			// CHAOS-4264 R2: bodies here are the REAL shape a FastAPI
			// HTTPException(detail={...}) actually serializes to -- verified
			// against a live fastapi.testclient.TestClient response, not
			// assumed. An earlier version of this fixture used a flat
			// top-level body matching a bug in classifyCompatibilityError's
			// struct (a top-level "reason" field that real responses never
			// have), so the test passed while the parser silently matched
			// nothing in production.
			name:       "signaled runner on a 503",
			statusCode: http.StatusServiceUnavailable,
			body:       `{"detail":{"message":"Metric execution failed before any output was produced","state":"failed","reason":"process_signaled"}}`,
			wantErr:    ErrCompatibilityProcessSignaled,
		},
		{
			name:       "resource-exhausted runner on a 503",
			statusCode: http.StatusServiceUnavailable,
			body:       `{"detail":{"message":"Metric execution failed before any output was produced","state":"failed","reason":"resource_exhausted"}}`,
			wantErr:    ErrCompatibilityResourceExhausted,
		},
		{
			name:       "refused claim on a 409",
			statusCode: http.StatusConflict,
			body:       `{"detail":{"message":"Execution outcome requires readback","state":"ambiguous","reason":"ambiguous_refused"}}`,
			wantErr:    ErrCompatibilityAmbiguousRefused,
		},
		{
			name:       "true ambiguous 503 with no reason predates this ticket",
			statusCode: http.StatusServiceUnavailable,
			body:       `{"detail":{"message":"Metric execution outcome is ambiguous","state":"ambiguous"}}`,
			wantErr:    ErrUnavailable,
		},
		{
			name:       "unrecognized reason value",
			statusCode: http.StatusServiceUnavailable,
			body:       `{"detail":{"message":"x","state":"failed","reason":"something_new"}}`,
			wantErr:    ErrUnavailable,
		},
		{
			name:       "non-JSON body",
			statusCode: http.StatusInternalServerError,
			body:       "not json",
			wantErr:    ErrUnavailable,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(testCase.statusCode)
				_, _ = writer.Write([]byte(testCase.body))
			}))
			defer server.Close()
			executor, err := NewHTTPCompatibilityExecutor(
				&http.Client{Timeout: time.Second},
				HTTPCompatibilityConfig{Endpoint: server.URL + "/internal/worker/daily-metrics/v1/execute", BearerToken: "token"},
			)
			if err != nil {
				t.Fatal(err)
			}
			run := Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"}
			got := executor.ComputePartition(t.Context(), run, Partition{ID: testPartitionID, RunID: testRunID}, nil)
			if !errors.Is(got, testCase.wantErr) {
				t.Fatalf("ComputePartition error = %v, want %v", got, testCase.wantErr)
			}
		})
	}
}

func TestHTTPCompatibilityExecutorAllowsContractOwnedDeadlineBeyondThirtySeconds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"status":"success"}`))
	}))
	defer server.Close()
	executor, err := NewHTTPCompatibilityExecutor(
		&http.Client{},
		HTTPCompatibilityConfig{
			Endpoint:    server.URL + "/internal/worker/daily-metrics/v1/execute",
			BearerToken: "token",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 31*time.Second)
	defer cancel()
	run := Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"}
	if err := executor.ComputePartition(ctx, run, Partition{ID: testPartitionID, RunID: testRunID}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPCompatibilityExecutorRejectsGenericOrUntrustedEndpoints(t *testing.T) {
	client := &http.Client{Timeout: time.Second}
	for _, endpoint := range []string{
		"https://worker.example/internal/worker/daily-metrics/v1/other",
		"https://worker.example/internal/worker/daily-metrics/v1/execute?command=anything",
		"http://worker.example/internal/worker/daily-metrics/v1/execute",
	} {
		if executor, err := NewHTTPCompatibilityExecutor(client, HTTPCompatibilityConfig{Endpoint: endpoint, BearerToken: "token"}); err == nil || executor != nil {
			t.Fatalf("accepted unsafe endpoint %q", endpoint)
		}
	}
}

func TestHTTPCompatibilityExecutorAllowsExplicitInternalComposeService(t *testing.T) {
	client := &http.Client{Timeout: time.Second}
	endpoint := "http://api:8000/internal/worker/daily-metrics/v1/execute"
	if executor, err := NewHTTPCompatibilityExecutor(client, HTTPCompatibilityConfig{
		Endpoint: endpoint, BearerToken: "token", AllowInsecureInternal: true,
	}); err != nil || executor == nil {
		t.Fatalf("explicit internal endpoint rejected: executor=%v err=%v", executor, err)
	}
	if executor, err := NewHTTPCompatibilityExecutor(client, HTTPCompatibilityConfig{
		Endpoint: endpoint, BearerToken: "token",
	}); err == nil || executor != nil {
		t.Fatal("internal HTTP endpoint accepted without explicit opt-in")
	}
	if executor, err := NewHTTPCompatibilityExecutor(client, HTTPCompatibilityConfig{
		Endpoint:    "http://worker.example/internal/worker/daily-metrics/v1/execute",
		BearerToken: "token", AllowInsecureInternal: true,
	}); err == nil || executor != nil {
		t.Fatal("public HTTP endpoint accepted with internal opt-in")
	}
}
