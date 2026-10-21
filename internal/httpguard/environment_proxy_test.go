package httpguard

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func sendCanary(ctx context.Context, client *http.Client) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxycapture.Target, nil)
	if err != nil {
		panic(err)
	}
	request.Header.Set("Authorization", "Bearer "+proxycapture.Canary)
	if response, err := client.Do(request); err == nil {
		_ = response.Body.Close()
	}
}

func TestNewClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) { sendCanary(ctx, NewClient(5*time.Second)) }).AssertNothingReached(t)
}

func TestTransportAndSharedTransportNeverDialTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		sendCanary(ctx, &http.Client{Transport: Transport()})
		sendCanary(ctx, &http.Client{Transport: SharedTransport()})
	}).AssertNothingReached(t)
}

func TestSharedTransportIsOnePool(t *testing.T) {
	if SharedTransport() != SharedTransport() {
		t.Fatal("SharedTransport returned two transports: every caller would get a pool of its own")
	}
	if Transport() == Transport() {
		t.Fatal("Transport returned the same transport twice: a caller that changes a field would change every client")
	}
	if SharedTransport().Proxy != nil || Transport().Proxy != nil {
		t.Fatal("a transport of this package has a Proxy function")
	}
}
