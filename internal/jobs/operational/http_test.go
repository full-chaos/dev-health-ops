package operational

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestHTTPDispatcherClassifiesBridgeResultContract exercises post()'s
// status-code/body classification directly, including a TWO-success-status
// call (mirroring the deleted DispatchWebhook's "success"/"skipped" pair --
// CHAOS-5320 removed the only caller with more than one success status, but
// the underlying post() mechanism stays generic and must stay correct for
// it, not just for DispatchBilling/DispatchHeartbeat's single-status calls).
func TestHTTPDispatcherClassifiesBridgeResultContract(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		permanent  bool
		wantError  bool
	}{
		{name: "success", statusCode: http.StatusOK, body: `{"status":"success"}`},
		{name: "duplicate", statusCode: http.StatusOK, body: `{"status":"skipped"}`},
		{name: "error body", statusCode: http.StatusOK, body: `{"status":"error"}`, permanent: true, wantError: true},
		{name: "dropped body", statusCode: http.StatusOK, body: `{"status":"dropped"}`, permanent: true, wantError: true},
		{name: "malformed body", statusCode: http.StatusOK, body: `{`, wantError: true},
		{name: "unprocessable", statusCode: http.StatusUnprocessableEntity, body: `{}`, permanent: true, wantError: true},
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
			dispatcher, err := NewHTTPDispatcher(client, HTTPDispatcherConfig{
				BillingEndpoint:   "https://api.internal.example/billing",
				HeartbeatEndpoint: "https://api.internal.example/heartbeat",
				BearerToken:       "test-token",
			})
			if err != nil {
				t.Fatal(err)
			}
			err = dispatcher.post(context.Background(), "https://api.internal.example/webhook", map[string]string{
				"delivery_id": webhookID,
			}, "success", "skipped")
			if (err != nil) != test.wantError || errors.Is(err, ErrDispatchPermanent) != test.permanent {
				t.Fatalf("error=%v permanent=%v", err, errors.Is(err, ErrDispatchPermanent))
			}
		})
	}
}

// CHAOS-3952: the whole fix is worthless if the key never leaves the
// process — assert the wire body Go actually sends, not just that
// DispatchBilling returns nil.
func TestDispatchBillingSendsIdempotencyKeyAcrossTheBridge(t *testing.T) {
	var captured map[string]string
	client := &http.Client{
		Timeout: time.Second,
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(body, &captured); err != nil {
				t.Fatal(err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"status":"sent"}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	dispatcher, err := NewHTTPDispatcher(client, HTTPDispatcherConfig{
		BillingEndpoint:   "https://api.internal.example/billing",
		HeartbeatEndpoint: "https://api.internal.example/heartbeat",
		BearerToken:       "test-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	notification := BillingNotification{
		ID: billingID, OrganizationID: "00000000-0000-4000-8000-000000000010",
		NotificationType: "invoice_receipt", IdempotencyKey: "billing:wire-key",
	}
	if err := dispatcher.DispatchBilling(context.Background(), notification); err != nil {
		t.Fatal(err)
	}
	if captured["idempotency_key"] != "billing:wire-key" {
		t.Fatalf("idempotency_key did not cross the bridge: body=%#v", captured)
	}
	if captured["notification_id"] != billingID {
		t.Fatalf("notification_id missing from wire body: %#v", captured)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestHTTPDispatcherRequiresBoundedTimeoutAndDeployableEndpoint(t *testing.T) {
	config := HTTPDispatcherConfig{
		BillingEndpoint:   "https://api.internal.example/worker/billing",
		HeartbeatEndpoint: "https://api.internal.example/worker/heartbeat",
		BearerToken:       "test-token",
	}
	if _, err := NewHTTPDispatcher(&http.Client{}, config); err == nil {
		t.Fatal("unbounded client accepted")
	}
	if _, err := NewHTTPDispatcher(&http.Client{Timeout: time.Second}, config); err != nil {
		t.Fatalf("internal TLS endpoint rejected: %v", err)
	}
	config.BillingEndpoint = "http://api:8080/worker/billing"
	if _, err := NewHTTPDispatcher(&http.Client{Timeout: time.Second}, config); err == nil {
		t.Fatal("unencrypted service-DNS endpoint accepted")
	}
	config.AllowInsecureInternal = true
	if _, err := NewHTTPDispatcher(&http.Client{Timeout: time.Second}, config); err != nil {
		t.Fatalf("explicit internal service-DNS endpoint rejected: %v", err)
	}
	config.BillingEndpoint = "http://api.example.com/worker/billing"
	if _, err := NewHTTPDispatcher(&http.Client{Timeout: time.Second}, config); err == nil {
		t.Fatal("public unencrypted endpoint accepted with internal opt-in")
	}
}
