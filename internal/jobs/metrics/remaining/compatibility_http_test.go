package remaining

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPCompatibilityExecutorSendsOnlyAuthoritativeIDs(t *testing.T) {
	var received map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost ||
			request.URL.Path != "/internal/worker/remaining-metrics/v1/execute" ||
			request.Header.Get("Authorization") != "Bearer token" {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = writer.Write([]byte(`{"status":"success","execution_id":"ignored"}`))
	}))
	defer server.Close()

	executor, err := NewHTTPCompatibilityExecutor(
		&http.Client{Timeout: time.Second},
		HTTPCompatibilityConfig{
			Endpoint:    server.URL + "/internal/worker/remaining-metrics/v1/execute",
			BearerToken: "token",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{
		ID:             "11111111-1111-4111-8111-111111111111",
		OrganizationID: "22222222-2222-4222-8222-222222222222",
		Family:         "capacity",
		Generation:     "capacity-v1",
		Status:         "running",
	}
	partition := Partition{
		ID:    "33333333-3333-4333-8333-333333333333",
		RunID: run.ID,
	}
	if err := executor.ComputePartition(t.Context(), run, partition); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"operation":    "partition",
		"run_id":       run.ID,
		"partition_id": partition.ID,
	}
	if len(received) != len(want) {
		t.Fatalf("request leaked non-identity input: %#v", received)
	}
	for key, value := range want {
		if received[key] != value {
			t.Fatalf("request = %#v, want %#v", received, want)
		}
	}
}

func TestHTTPCompatibilityExecutorRejectsGenericOrUntrustedEndpoints(t *testing.T) {
	client := &http.Client{Timeout: time.Second}
	for _, endpoint := range []string{
		"https://worker.example/internal/worker/remaining-metrics/v1/other",
		"https://worker.example/internal/worker/remaining-metrics/v1/execute?command=anything",
		"http://worker.example/internal/worker/remaining-metrics/v1/execute",
	} {
		executor, err := NewHTTPCompatibilityExecutor(
			client,
			HTTPCompatibilityConfig{Endpoint: endpoint, BearerToken: "token"},
		)
		if err == nil || executor != nil {
			t.Fatalf("accepted unsafe endpoint %q", endpoint)
		}
	}
}

func TestHTTPCompatibilityExecutorRejectsAmbiguousResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"detail":{"state":"ambiguous"}}`))
	}))
	defer server.Close()
	executor, err := NewHTTPCompatibilityExecutor(
		&http.Client{Timeout: time.Second},
		HTTPCompatibilityConfig{
			Endpoint:    server.URL + "/internal/worker/remaining-metrics/v1/execute",
			BearerToken: "token",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = executor.ComputePartition(
		t.Context(),
		Run{ID: "11111111-1111-4111-8111-111111111111"},
		Partition{
			ID:    "33333333-3333-4333-8333-333333333333",
			RunID: "11111111-1111-4111-8111-111111111111",
		},
	)
	if err != ErrUnavailable {
		t.Fatalf("error = %v, want %v", err, ErrUnavailable)
	}
}
