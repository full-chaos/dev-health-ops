package providersync

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

type paddedArrayDoer struct{ padding int }

func (d paddedArrayDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Request: request,
		Body: io.NopCloser(io.MultiReader(strings.NewReader("[]"), &sizeRepeatReader{n: d.padding}))}, nil
}

func TestGitLabSecurityBodyOverTheCapFailsWithRoute(t *testing.T) {
	client, err := providerfoundation.NewHTTPClient("gitlab", "https://api.example.test", fakehttp.Client(paddedArrayDoer{padding: 2<<20 + 100}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	_, _, err = fetchGitLabSecurityObjects(context.Background(), client, "/projects/{project}/vulnerability_findings", "/projects/acme-secret/vulnerability_findings", &gitLabSecurityResponseObserver{attempts: &n})
	if err == nil {
		t.Fatal("a body over the 2 MiB cap was accepted")
	}
	requireRouteFailure(t, err, "/projects/{project}/vulnerability_findings", providerfoundation.ReasonBodyTooLarge)
}
