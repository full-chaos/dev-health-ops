package pagerduty

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

func TestHTTPCompatibilityReconcilerClassifiesBridgeResultContract(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		permanent  bool
		wantError  bool
	}{
		{name: "processed", statusCode: http.StatusOK, body: `{"status":"processed"}`},
		{name: "duplicate receipt", statusCode: http.StatusOK, body: `{"status":"skipped"}`},
		{name: "error body", statusCode: http.StatusOK, body: `{"status":"error"}`, permanent: true, wantError: true},
		{name: "malformed body", statusCode: http.StatusOK, body: `{`, wantError: true},
		{name: "unprocessable", statusCode: http.StatusUnprocessableEntity, body: `{}`, permanent: true, wantError: true},
		{name: "request timeout", statusCode: http.StatusRequestTimeout, body: `{}`, wantError: true},
		{name: "rate limited", statusCode: http.StatusTooManyRequests, body: `{}`, wantError: true},
		{name: "unavailable", statusCode: http.StatusServiceUnavailable, body: `{}`, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{
				Timeout: time.Second,
				Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: test.statusCode,
						Body:       io.NopCloser(strings.NewReader(test.body)),
						Header:     make(http.Header),
					}, nil
				}),
			}
			reconciler, err := NewHTTPCompatibilityReconciler(client, HTTPCompatibilityConfig{
				Endpoint:    "https://api.internal.example/api/internal/worker-operational/pagerduty",
				BearerToken: "test-token",
			})
			if err != nil {
				t.Fatal(err)
			}
			err = reconciler.Reconcile(context.Background(), testEvent())
			if (err != nil) != test.wantError || streamrunner.IsPermanent(err) != test.permanent {
				t.Fatalf("error=%v permanent=%v", err, streamrunner.IsPermanent(err))
			}
		})
	}
}

func TestHTTPCompatibilityReconcilerPostsDurableIdentityAndPayload(t *testing.T) {
	var body []byte
	var authorization string
	client := &http.Client{
		Timeout: time.Second,
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			body, _ = io.ReadAll(request.Body)
			authorization = request.Header.Get("Authorization")
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"status":"processed"}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	reconciler, err := NewHTTPCompatibilityReconciler(client, HTTPCompatibilityConfig{
		Endpoint:    "https://api.internal.example/api/internal/worker-operational/pagerduty",
		BearerToken: "test-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(context.Background(), testEvent()); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		BindingID  string          `json:"binding_id"`
		EventID    string          `json:"event_id"`
		ReceiptID  string          `json:"receipt_id"`
		ReceivedAt string          `json:"received_at"`
		Payload    json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.BindingID != "binding" || decoded.EventID != "evt-1" ||
		decoded.ReceiptID != "pagerduty:binding:evt-1" ||
		decoded.ReceivedAt != "2026-07-23T00:00:00Z" ||
		string(decoded.Payload) != `{"event":{"id":"evt-1"}}` {
		t.Fatalf("bridge request = %s", body)
	}
	if authorization != "Bearer test-token" {
		t.Fatalf("authorization header = %q", authorization)
	}
}

func TestHTTPCompatibilityReconcilerRequiresBoundedTimeoutAndDeployableEndpoint(t *testing.T) {
	config := HTTPCompatibilityConfig{
		Endpoint:    "https://api.internal.example/api/internal/worker-operational/pagerduty",
		BearerToken: "test-token",
	}
	if _, err := NewHTTPCompatibilityReconciler(&http.Client{}, config); err == nil {
		t.Fatal("unbounded client accepted")
	}
	if _, err := NewHTTPCompatibilityReconciler(&http.Client{Timeout: time.Second}, config); err != nil {
		t.Fatalf("internal TLS endpoint rejected: %v", err)
	}
	missingToken := config
	missingToken.BearerToken = "  "
	if _, err := NewHTTPCompatibilityReconciler(&http.Client{Timeout: time.Second}, missingToken); err == nil {
		t.Fatal("unauthenticated bridge accepted")
	}
	config.Endpoint = "http://api:8080/api/internal/worker-operational/pagerduty"
	if _, err := NewHTTPCompatibilityReconciler(&http.Client{Timeout: time.Second}, config); err == nil {
		t.Fatal("unencrypted service-DNS endpoint accepted")
	}
	config.AllowInsecureInternal = true
	if _, err := NewHTTPCompatibilityReconciler(&http.Client{Timeout: time.Second}, config); err != nil {
		t.Fatalf("explicit internal service-DNS endpoint rejected: %v", err)
	}
	config.Endpoint = "http://api.example.com/api/internal/worker-operational/pagerduty"
	if _, err := NewHTTPCompatibilityReconciler(&http.Client{Timeout: time.Second}, config); err == nil {
		t.Fatal("public unencrypted endpoint accepted with internal opt-in")
	}
}

// The handler wraps reconciler failures, so a permanent bridge rejection must
// still reach the runner as a quarantine decision rather than a retry.
func TestHandlerPropagatesPermanentBridgeRejection(t *testing.T) {
	receipts := &receiptStore{proceed: true}
	handler, err := NewHandler(receipts, &reconciler{
		err: &streamrunner.PermanentError{Reason: "pagerduty_bridge_rejected"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), validMessage())
	if !streamrunner.IsPermanent(err) || receipts.completed {
		t.Fatalf("permanent bridge rejection err=%v completed=%t", err, receipts.completed)
	}
}

func testEvent() Event {
	event, err := parse(validMessage())
	if err != nil {
		panic(err)
	}
	return event
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
