package stripeclient

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries the secret key to another origin (D4124).
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	provider := New(Options{Key: "sk_test_redirect_probe", BaseURL: probe.Base.URL, HTTPClient: probe.Client()})
	_, _ = provider.RawGet(context.Background(), "/v1/customers/cus_probe")
	probe.Assert(t)
}
