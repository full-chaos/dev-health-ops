package workgraph

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPCompatibilityExecutorSendsOnlyRequestIdentityAndClaim(t *testing.T) {
	var received map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/internal/worker/workgraph/v1/execute" || request.Header.Get("Authorization") != "Bearer token" {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = writer.Write([]byte(`{"status":"success","output_evidence":{"edges":1}}`))
	}))
	defer server.Close()
	executor, err := NewHTTPCompatibilityExecutor(&http.Client{Timeout: time.Second}, HTTPCompatibilityConfig{Endpoint: server.URL + "/internal/worker/workgraph/v1/execute", BearerToken: "token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Execute(t.Context(), *testClaim(time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(received) != 2 || received["request_id"] != testRequestID || received["claim_token"] != testToken {
		t.Fatalf("request leaked non-identity input: %#v", received)
	}
}

func TestHTTPCompatibilityExecutorRejectsGenericOrUntrustedEndpoints(t *testing.T) {
	client := &http.Client{Timeout: time.Second}
	for _, endpoint := range []string{
		"https://worker.example/internal/worker/workgraph/v1/other",
		"https://worker.example/internal/worker/workgraph/v1/execute?command=anything",
		"http://worker.example/internal/worker/workgraph/v1/execute",
	} {
		if executor, err := NewHTTPCompatibilityExecutor(client, HTTPCompatibilityConfig{Endpoint: endpoint, BearerToken: "token"}); err == nil || executor != nil {
			t.Fatalf("accepted unsafe endpoint %q", endpoint)
		}
	}
}

func TestHTTPCompatibilityExecutorHonorsExecutionContextCancellation(t *testing.T) {
	requestStarted := make(chan struct{})
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		close(requestStarted)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	executor, err := NewHTTPCompatibilityExecutor(client, HTTPCompatibilityConfig{
		Endpoint:    "https://worker.example/internal/worker/workgraph/v1/execute",
		BearerToken: "token",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, executeErr := executor.Execute(ctx, *testClaim(time.Second))
		result <- executeErr
	}()
	<-requestStarted
	cancel()
	if executeErr := <-result; !errors.Is(executeErr, ErrUnavailable) {
		t.Fatalf("Execute() error=%v", executeErr)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
