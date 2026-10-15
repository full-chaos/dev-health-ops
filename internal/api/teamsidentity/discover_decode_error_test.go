package teamsidentity

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type malformedDoer struct{}

func (malformedDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{}, Request: request,
		Body: io.NopCloser(strings.NewReader(`{"not":"a list"}`)),
	}, nil
}

// A decode error of a discovery list page is logged by handlers.internal; it
// must not carry the request path (an org, team or group name).
func TestDiscoveryDecodeErrorsCarryNoRequestPath(t *testing.T) {
	client, err := providerfoundation.NewHTTPClient("github", "https://api.example.test", fakehttp.Client(malformedDoer{}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, githubErr := githubListAllPages[map[string]any](ctx, client, "/orgs/{org}/teams/{slug}/repos", "/orgs/secret-org/teams/secret-team/repos?per_page=100")
	_, _, groupErr := gitlabListGroupPages(ctx, client, "/groups/{group}/subgroups", "/api/v4/groups/secret%2Forg/subgroups", 100)
	_, _, projectErr := gitlabListProjectPages(ctx, client, "/groups/{group}/projects", "/api/v4/groups/secret-group/projects", 100, false)
	for name, err := range map[string]error{"github": githubErr, "gitlab group": groupErr, "gitlab project": projectErr} {
		if err == nil {
			t.Fatalf("%s: want a decode error", name)
		}
		for _, secret := range []string{"secret-org", "secret-team", "secret-group", "secret%2Forg", "/orgs/", "/api/v4"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("%s: error %q carries the request path (%s)", name, err.Error(), secret)
			}
		}
	}
}
