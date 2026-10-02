package providersync

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A request-counting decorator around a supplied client of the default redirect policy still never carries the credential
// to another origin: the provider client's origin guard looks through the decorator (CHAOS-7910 r1 P1).
func TestACountingDecoratorNeverHidesAFollowingClientFromTheOriginGuard(t *testing.T) {
	probe := redirectprobe.New(t)
	client, err := providerfoundation.NewHTTPClient("gitlab", probe.Base.URL, CountRequests(probe.Client()),
		providerfoundation.TokenAuth("PRIVATE-TOKEN", "", secrets.NewValue("SECRET")),
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if response, err := client.Do(context.Background(), http.MethodGet, "/user", nil); err == nil && response != nil {
		response.Body.Close()
	}
	probe.Assert(t)
}

// A ROUTE decorator (the production types every route builds around client.Doer) cannot hide a following client either: the
// guard sits inside the client from its construction, below every decorator (CHAOS-7910 r1 fix, vetter-2 measured 43 of them).
func TestARouteDecoratorNeverHidesAFollowingClientFromTheOriginGuard(t *testing.T) {
	for name, decorate := range map[string]func(providerfoundation.HTTPDoer, *int) providerfoundation.HTTPDoer{
		"gitlab commits": func(d providerfoundation.HTTPDoer, n *int) providerfoundation.HTTPDoer {
			return gitLabCommitsCountingDoer{delegate: d, attempts: n}
		},
		"github commits": func(d providerfoundation.HTTPDoer, n *int) providerfoundation.HTTPDoer {
			return gitHubCommitsCountingDoer{delegate: d, attempts: n}
		},
		"gitlab pull request": func(d providerfoundation.HTTPDoer, n *int) providerfoundation.HTTPDoer {
			return gitLabPullRequestCountingDoer{delegate: d, attempts: n}
		},
	} {
		t.Run(name, func(t *testing.T) {
			probe := redirectprobe.New(t)
			client, err := providerfoundation.NewHTTPClient("gitlab", probe.Base.URL, probe.Client(),
				providerfoundation.TokenAuth("PRIVATE-TOKEN", "", secrets.NewValue("SECRET")),
				providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
				providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
			if err != nil {
				t.Fatal(err)
			}
			requests := 0
			counted := *client
			counted.Doer = decorate(client.Doer, &requests)
			if response, err := counted.Do(context.Background(), http.MethodGet, "/user", nil); err == nil && response != nil {
				response.Body.Close()
			}
			probe.Assert(t)
		})
	}
}
