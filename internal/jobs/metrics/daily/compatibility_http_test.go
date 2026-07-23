package daily

import (
	"encoding/json"
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
	if err := executor.ComputePartition(t.Context(), run, Partition{ID: testPartitionID, RunID: testRunID}); err != nil {
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
	if err := executor.ComputePartition(t.Context(), run, partition); err != nil {
		t.Fatal(err)
	}
	if err := executor.ComputePartition(t.Context(), run, partition); err != nil {
		t.Fatal(err)
	}
	if writes["daily-v1:"+testRunID+":"+testPartitionID] != 1 {
		t.Fatalf("compatibility retry duplicated authoritative output: %#v", writes)
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
