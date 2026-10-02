package admin

import (
	"context"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries the stored credential to another origin:
// the base answers a redirect and the other origin sees no request at all (D4124).
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	get := func(t *testing.T, url string) *http.Request {
		t.Helper()
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Token token=SECRET")
		return request
	}
	t.Run("the PagerDuty services client", func(t *testing.T) {
		probe := redirectprobe.New(t)
		if response, err := pagerDutyServicesClient(probe.Client()).Do(get(t, probe.Base.URL+"/services")); err == nil {
			response.Body.Close()
		}
		probe.Assert(t)
	})
	t.Run("the LLM settings readiness prober", func(t *testing.T) {
		probe := redirectprobe.New(t)
		prober := newOpenAICompatibleReadinessProber(probe.Client())
		if response, err := prober.client.Do(get(t, probe.Base.URL+"/v1/models")); err == nil {
			response.Body.Close()
		}
		probe.Assert(t)
	})
	t.Run("the revoke doer of the admin routes (supplied Deps.HTTPDoer)", func(t *testing.T) {
		probe := redirectprobe.New(t)
		doer := revokeDoer(probe.Client())
		if response, err := doer.Do(get(t, probe.Base.URL+"/oauth/revoke")); err == nil {
			response.Body.Close()
		}
		probe.Assert(t)
	})
	t.Run("the defaults production builds (pagerDutyServicesClient, readiness prober, revokeDoer with nil)", func(t *testing.T) {
		probe := redirectprobe.New(t)
		clients := map[string]providerfoundation.HTTPDoer{
			"services": pagerDutyServicesClient(nil),
			"prober":   newOpenAICompatibleReadinessProber(nil).client,
			"revoke":   revokeDoer(nil),
		}
		for name, doer := range clients {
			built, ok := doer.(*http.Client)
			if !ok {
				t.Fatalf("%s: default is %T", name, doer)
			}
			if response, err := redirectprobe.Reach(built).Do(get(t, probe.Base.URL+"/x")); err == nil {
				response.Body.Close()
			}
		}
		probe.Assert(t)
	})
}
