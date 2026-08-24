package remaining

import (
	"context"
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
	if _, err := executor.ComputePartition(t.Context(), run, partition); err != nil {
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

func TestHTTPCompatibilityExecutorAllowsExplicitInternalComposeService(t *testing.T) {
	client := &http.Client{Timeout: time.Second}
	endpoint := "http://api:8000/internal/worker/remaining-metrics/v1/execute"
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
		Endpoint:    "http://worker.example/internal/worker/remaining-metrics/v1/execute",
		BearerToken: "token", AllowInsecureInternal: true,
	}); err == nil || executor != nil {
		t.Fatal("public HTTP endpoint accepted with internal opt-in")
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
	_, err = executor.ComputePartition(
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

// recordingCompatibilityObserver captures ObserveCompatibilityPartition calls
// for assertion, mirroring the fake DORAObserver/CapacityObserver test
// doubles already used elsewhere in this package.
type recordingCompatibilityObserver struct {
	family      string
	rowsWritten *int
	calls       int
}

func (observer *recordingCompatibilityObserver) ObserveCompatibilityPartition(
	family string, rowsWritten *int,
) error {
	observer.family = family
	observer.rowsWritten = rowsWritten
	observer.calls++
	return nil
}

// TestHTTPCompatibilityExecutorReportsAZeroRowCompletionDistinctly is the
// CHAOS-4243 acceptance case: before this, a partition that wrote real rows
// and a partition that reported success while writing zero were
// indistinguishable outside the Python bridge -- the response's rows_written
// field must now be surfaced through CompatibilityOutcome and the observer,
// and an explicit 0 must never collapse into the "not applicable" nil case.
func TestHTTPCompatibilityExecutorReportsAZeroRowCompletionDistinctly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"status":"success","rows_written":0}`))
	}))
	defer server.Close()
	observer := &recordingCompatibilityObserver{}
	executor, err := NewHTTPCompatibilityExecutor(
		&http.Client{Timeout: time.Second},
		HTTPCompatibilityConfig{
			Endpoint:    server.URL + "/internal/worker/remaining-metrics/v1/execute",
			BearerToken: "token",
			Observer:    observer,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{
		ID:             "11111111-1111-4111-8111-111111111111",
		OrganizationID: "22222222-2222-4222-8222-222222222222",
		Family:         "release_impact",
		Status:         "running",
	}
	partition := Partition{ID: "33333333-3333-4333-8333-333333333333", RunID: run.ID}

	outcome, err := executor.ComputePartition(t.Context(), run, partition)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.RowsWritten == nil {
		t.Fatal("outcome carries no rows_written; a reported 0 must not collapse to nil")
	}
	if *outcome.RowsWritten != 0 {
		t.Fatalf("outcome.RowsWritten = %d, want 0", *outcome.RowsWritten)
	}
	if observer.calls != 1 || observer.family != "release_impact" {
		t.Fatalf("observer calls=%d family=%q, want one call for release_impact", observer.calls, observer.family)
	}
	if observer.rowsWritten == nil || *observer.rowsWritten != 0 {
		t.Fatalf("observer.rowsWritten = %v, want a pointer to 0", observer.rowsWritten)
	}
}

// TestHTTPCompatibilityExecutorLeavesRowsWrittenNilWhenAbsent proves the
// converse: a family/response that carries no rows_written field at all must
// report nil ("not applicable"), never coerce to a false zero.
func TestHTTPCompatibilityExecutorLeavesRowsWrittenNilWhenAbsent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"status":"success"}`))
	}))
	defer server.Close()
	observer := &recordingCompatibilityObserver{}
	executor, err := NewHTTPCompatibilityExecutor(
		&http.Client{Timeout: time.Second},
		HTTPCompatibilityConfig{
			Endpoint:    server.URL + "/internal/worker/remaining-metrics/v1/execute",
			BearerToken: "token",
			Observer:    observer,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{
		ID:             "11111111-1111-4111-8111-111111111111",
		OrganizationID: "22222222-2222-4222-8222-222222222222",
		Family:         "membership_backfill",
		Status:         "running",
	}
	partition := Partition{ID: "33333333-3333-4333-8333-333333333333", RunID: run.ID}

	outcome, err := executor.ComputePartition(t.Context(), run, partition)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.RowsWritten != nil {
		t.Fatalf("outcome.RowsWritten = %v, want nil for a response with no rows_written field", *outcome.RowsWritten)
	}
	if observer.calls != 1 || observer.rowsWritten != nil {
		t.Fatalf("observer calls=%d rowsWritten=%v, want one call with nil", observer.calls, observer.rowsWritten)
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
			Endpoint:    server.URL + "/internal/worker/remaining-metrics/v1/execute",
			BearerToken: "token",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 31*time.Second)
	defer cancel()
	run := Run{
		ID:             "11111111-1111-4111-8111-111111111111",
		OrganizationID: "22222222-2222-4222-8222-222222222222",
		Family:         "complexity",
		Generation:     "post-sync:generation",
		Status:         "running",
	}
	if _, err := executor.ComputePartition(ctx, run, Partition{
		ID:    "33333333-3333-4333-8333-333333333333",
		RunID: run.ID,
	}); err != nil {
		t.Fatal(err)
	}
}
