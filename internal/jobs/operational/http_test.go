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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestHTTPDispatcherRequiresBoundedTimeoutAndDeployableEndpoint(t *testing.T) {
	config := HTTPDispatcherConfig{
		HeartbeatEndpoint: "https://api.internal.example/worker/heartbeat",
		BearerToken:       "test-token",
	}
	if _, err := NewHTTPDispatcher(&http.Client{}, config); err == nil {
		t.Fatal("unbounded client accepted")
	}
	if _, err := NewHTTPDispatcher(&http.Client{Timeout: time.Second}, config); err != nil {
		t.Fatalf("internal TLS endpoint rejected: %v", err)
	}
	// CHAOS-5320/CHAOS-5353 left the heartbeat as the only endpoint on this
	// dispatcher, so the endpoint-validation ladder below now exercises it
	// rather than the deleted billing one. The property under test is
	// unchanged: validInternalEndpoint's scheme/host rules, including that
	// the insecure opt-in widens service-DNS names but never public ones.
	config.HeartbeatEndpoint = "http://api:8080/worker/heartbeat"
	if _, err := NewHTTPDispatcher(&http.Client{Timeout: time.Second}, config); err == nil {
		t.Fatal("unencrypted service-DNS endpoint accepted")
	}
	config.AllowInsecureInternal = true
	if _, err := NewHTTPDispatcher(&http.Client{Timeout: time.Second}, config); err != nil {
		t.Fatalf("explicit internal service-DNS endpoint rejected: %v", err)
	}
	config.HeartbeatEndpoint = "http://api.example.com/worker/heartbeat"
	if _, err := NewHTTPDispatcher(&http.Client{Timeout: time.Second}, config); err == nil {
		t.Fatal("public unencrypted endpoint accepted with internal opt-in")
	}
}
