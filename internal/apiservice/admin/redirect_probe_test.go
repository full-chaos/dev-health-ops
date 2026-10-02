package admin

import (
	"context"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A client of the default redirect policy never carries the stored credential to another origin: the base answers a
// redirect and the other origin sees no request at all (D4124). Each call is the callee that sends the credential;
// "supplied" is a client of net/http's default policy, "default" is the client production builds (Deps.HTTPDoer is nil
// in production: apiservice/service.go:265 passes deps.HTTPDoer, which nothing assigns), its transport aside.
func TestAClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	services := func(t *testing.T, doer providerfoundation.HTTPDoer, probe *redirectprobe.Probe) {
		t.Helper()
		_, _ = (&handlers{}).pagerDutyGET(context.Background(), fakehttp.Client(doer), probe.Base.URL+"/services", pagerDutyRequestAuth{header: "Token token=SECRET"})
	}
	prober := func(t *testing.T, doer providerfoundation.HTTPDoer, probe *redirectprobe.Probe) {
		t.Helper()
		_, _ = newOpenAICompatibleReadinessProber(fakehttp.Client(doer)).doCompletionOnce(context.Background(), probe.Base.URL, "SECRET", chatCompletionRequest{})
	}
	revoke := func(t *testing.T, doer providerfoundation.HTTPDoer, probe *redirectprobe.Probe) {
		t.Helper()
		_ = providerfoundation.RevokePagerDutyOAuthToken(context.Background(), fakehttp.Client(doer),
			providerfoundation.PagerDutyRevokeConfig{ClientID: "c", RevokeURL: probe.Base.URL + "/oauth/revoke"}, "SECRET")
	}
	for name, call := range map[string]func(*testing.T, providerfoundation.HTTPDoer, *redirectprobe.Probe){"PagerDuty services": services, "LLM readiness prober": prober, "PagerDuty revoke": revoke} {
		t.Run(name+", supplied", func(t *testing.T) {
			probe := redirectprobe.New(t)
			call(t, probe.Client(), probe)
			probe.Assert(t)
		})
	}
	defaults := map[string]func() providerfoundation.HTTPDoer{
		"PagerDuty services":   func() providerfoundation.HTTPDoer { return pagerDutyServicesClient(nil) },
		"LLM readiness prober": func() providerfoundation.HTTPDoer { return newOpenAICompatibleReadinessProber(nil).client },
		"PagerDuty revoke":     func() providerfoundation.HTTPDoer { return revokeDoer(nil) },
	}
	calls := map[string]func(*testing.T, providerfoundation.HTTPDoer, *redirectprobe.Probe){"PagerDuty services": services, "LLM readiness prober": prober, "PagerDuty revoke": revoke}
	for name, build := range defaults {
		t.Run(name+", default", func(t *testing.T) {
			probe := redirectprobe.New(t)
			built, ok := build().(*http.Client)
			if !ok {
				t.Fatalf("the default is not an *http.Client")
			}
			calls[name](t, redirectprobe.Reach(built), probe)
			probe.Assert(t)
		})
	}
}

// The defaults' own policies (httpx follows no redirect: Python parity) are pinned on their own, besides the callee guard.
func TestTheDefaultAdminClientsRefuseRedirectsOnTheirOwn(t *testing.T) {
	for name, doer := range map[string]providerfoundation.HTTPDoer{
		"PagerDuty services":   pagerDutyServicesClient(nil),
		"LLM readiness prober": newOpenAICompatibleReadinessProber(nil).client,
		"PagerDuty revoke":     revokeDoer(nil),
	} {
		built, ok := doer.(*http.Client)
		if !ok || built.CheckRedirect == nil || built.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
			t.Fatalf("%s: the default follows redirects", name)
		}
	}
}
