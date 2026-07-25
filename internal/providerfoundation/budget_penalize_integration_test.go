//go:build integration

package providerfoundation

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestValkeyBackoffGatePenalizeAgainstRealValkey is CHAOS-3132 evidence.
// backoffPenalizeLua's `return applied` is a Lua number, which Valkey
// RESP-encodes as an integer reply; valkey-go's .AsFloat64() only parses
// string-typed replies, so against a *mock* client (which never round-trips
// through the real RESP encoder) this bug is invisible — it only appears
// once a real server is on the other end. This test fails on the
// pre-fix script (Penalize returns ErrBudgetUnavailable wrapping "message
// type int64 is not a string") and passes once the script returns
// tostring(applied).
func TestValkeyBackoffGatePenalizeAgainstRealValkey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Valkey: %v", err)
		}
	})
	client, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)

	gate := ValkeyBackoffGate{
		Client: client, Provider: "github", OrgID: "org-1", Host: "api.github.com",
		MaxBackoff: time.Minute,
	}

	if err := gate.Penalize(ctx, 2*time.Second); err != nil {
		t.Fatalf("Penalize() against a real Valkey server = %v, want nil (backoffPenalizeLua must return a "+
			"string-typed reply valkey-go's AsFloat64 can parse)", err)
	}

	// EVAL is atomic and the SET runs server-side before Penalize's reply, so
	// even on the pre-fix script the key is durably written; a subsequent
	// GET-based Wait() sees the penalty regardless. Assert that here too so a
	// regression that broke the SET itself (as opposed to only the reply
	// decoding) would still be caught.
	wait, err := gate.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if wait <= 0 {
		t.Fatalf("Wait() = %v, want a positive backoff written by Penalize", wait)
	}
}

// TestHTTPClientClassifiesRateLimitAgainstRealValkeyBackoffGate reproduces
// the production symptom of CHAOS-3132: HTTPClient.Do calls Gate.Penalize on
// a 429 response and, on the pre-fix script, receives ErrBudgetUnavailable
// back instead of nil — so the caller sees a budget-store outage instead of
// the intended ProviderError{Class: ErrorRateLimited}. A mocked Gate (as
// used in TestHTTPClientPenalizesSharedGateWithLocalBackoff) can never
// observe this: only a real Valkey server RESP-encodes the Lua script's
// numeric return as an integer reply.
func TestHTTPClientClassifiesRateLimitAgainstRealValkeyBackoffGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Valkey: %v", err)
		}
	})
	valkeyClient, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(valkeyClient.Close)

	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusTooManyRequests, nil, `{"error":"limited"}`), nil
	}), RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})
	client.Gate = &ValkeyBackoffGate{Client: valkeyClient, Provider: "github", MaxBackoff: time.Minute}

	_, err = client.Do(ctx, http.MethodGet, "/items", nil)

	if errors.Is(err, ErrBudgetUnavailable) {
		t.Fatalf("Do() misclassified a rate limit as a budget-store outage: %v", err)
	}
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited {
		t.Fatalf("Do() error = %v, want ProviderError{Class: ErrorRateLimited}", err)
	}
}
