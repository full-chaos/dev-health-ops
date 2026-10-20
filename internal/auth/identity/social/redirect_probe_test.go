package social

import (
	"context"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A supplied client and the default (nil HTTP) client follow no redirect: the
// request that carries the provider token never reaches another origin.
func TestProviderCallsNeverFollowARedirectToAnotherOrigin(t *testing.T) {
	for name, supplied := range map[string]bool{"supplied client": true, "default client": false} {
		t.Run(name, func(t *testing.T) {
			probe := redirectprobe.New(t)
			c := &ProviderClient{}
			if supplied {
				c.HTTP = probe.Client()
			}
			var out map[string]any
			_ = c.do(context.Background(), request{method: http.MethodGet, url: probe.Base.URL + "/user",
				headers: map[string]string{"Authorization": "Bearer probe-token"}, step: "probe"}, &out)
			probe.Assert(t)
		})
	}
}
