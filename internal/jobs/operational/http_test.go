package operational

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

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
				WebhookEndpoint:   "https://api.internal.example/webhook",
				BillingEndpoint:   "https://api.internal.example/billing",
				HeartbeatEndpoint: "https://api.internal.example/heartbeat",
				BearerToken:       "test-token",
			})
			if err != nil {
				t.Fatal(err)
			}
			err = dispatcher.DispatchWebhook(context.Background(), WebhookDelivery{ID: webhookID})
			if (err != nil) != test.wantError || errors.Is(err, ErrDispatchPermanent) != test.permanent {
				t.Fatalf("error=%v permanent=%v", err, errors.Is(err, ErrDispatchPermanent))
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestHTTPDispatcherRequiresBoundedTimeoutAndDeployableEndpoint(t *testing.T) {
	config := HTTPDispatcherConfig{
		WebhookEndpoint:   "https://api.internal.example/worker/webhook",
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
	config.WebhookEndpoint = "http://api:8080/worker/webhook"
	if _, err := NewHTTPDispatcher(&http.Client{Timeout: time.Second}, config); err == nil {
		t.Fatal("unencrypted service-DNS endpoint accepted")
	}
	config.AllowInsecureInternal = true
	if _, err := NewHTTPDispatcher(&http.Client{Timeout: time.Second}, config); err != nil {
		t.Fatalf("explicit internal service-DNS endpoint rejected: %v", err)
	}
	config.WebhookEndpoint = "http://api.example.com/worker/webhook"
	if _, err := NewHTTPDispatcher(&http.Client{Timeout: time.Second}, config); err == nil {
		t.Fatal("public unencrypted endpoint accepted with internal opt-in")
	}
}
