package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type reproBrokenBody struct{ head io.Reader }

func (b *reproBrokenBody) Read(p []byte) (int, error) {
	n, err := b.head.Read(p)
	if err == io.EOF {
		return n, errors.New("synthetic connection reset")
	}
	return n, err
}
func (*reproBrokenBody) Close() error { return nil }

type reproDoer func(*http.Request) (*http.Response, error)

func (f reproDoer) Do(r *http.Request) (*http.Response, error) { return f(r) }

func reproReply(r *http.Request, status int, body string) (*http.Response, error) {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

func reproBroken404(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Request: r,
		Body: &reproBrokenBody{head: strings.NewReader(`{"message":"Not Fo`)}}, nil
}

func reproGoodState(t *testing.T, batch CompleteRouteBatch, err error, rows int) {
	t.Helper()
	t.Logf("err=%v watermark=%v rows=%d result=%v", err, batch.Watermark, rows, batch.Result)
	if err == nil && batch.Watermark != nil && rows == 0 {
		t.Fatalf("interrupted 404: no error and the watermark advanced to %v with 0 rows", *batch.Watermark)
	}
}

func reproRows(batch CompleteRouteBatch) int {
	n := 0
	for _, e := range batch.Effects {
		n += len(e.Rows)
	}
	return n
}

var reproNow = time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

// REPRO claim 1, github deployments list.
func TestReproInterrupted404GitHubDeploymentsList(t *testing.T) {
	doer := reproDoer(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/repos/acme/api/deployments":
			return reproBroken404(r)
		case "/repos/acme/api/releases":
			return reproReply(r, 200, `[]`)
		}
		return reproReply(r, 200, gitHubRepositoryFixture)
	})
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "deployments"), providerfoundation.Credential{}, gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com"), reproNow)
	reproGoodState(t, batch, err, reproRows(batch))
}

// REPRO control: a CLEAN 404 on the same route. Documents today's design (best effort); GREEN means it advances too.
func TestReproControlClean404GitHubDeploymentsListAdvances(t *testing.T) {
	doer := reproDoer(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/repos/acme/api/deployments":
			return reproReply(r, 404, `{"message":"Not Found"}`)
		case "/repos/acme/api/releases":
			return reproReply(r, 200, `[]`)
		}
		return reproReply(r, 200, gitHubRepositoryFixture)
	})
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "deployments"), providerfoundation.Credential{}, gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com"), reproNow)
	t.Logf("clean 404: err=%v watermark=%v", err, batch.Watermark)
	if err != nil || batch.Watermark == nil {
		t.Fatalf("control: clean 404 did not advance: err=%v", err)
	}
}

// REPRO claim 1, gitlab deployments.
func TestReproInterrupted404GitLabDeployments(t *testing.T) {
	doer := reproDoer(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v4/projects/123/deployments":
			return reproBroken404(r)
		case "/api/v4/projects/123/releases":
			return reproReply(r, 200, `[]`)
		}
		return reproReply(r, 200, gitLabRepositoryFixture)
	})
	batch, err := (GitLabDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("gitlab", "deployments"), providerfoundation.Credential{}, gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"), reproNow)
	reproGoodState(t, batch, err, reproRows(batch))
}

// REPRO claim 1, github security (one source).
func TestReproInterrupted404GitHubSecuritySource(t *testing.T) {
	doer := reproDoer(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/repos/acme/api/dependabot/alerts":
			return reproBroken404(r)
		case "/repos/acme/api/code-scanning/alerts", "/repos/acme/api/security-advisories":
			return reproReply(r, 200, `[]`)
		}
		return reproReply(r, 200, gitHubRepositoryFixture)
	})
	batch, err := (GitHubSecurityRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "security"), providerfoundation.Credential{}, gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com"), reproNow)
	reproGoodState(t, batch, err, reproRows(batch))
}

// REPRO claim 1, launchdarkly code refs.
func TestReproInterrupted404LaunchDarklyCodeRefs(t *testing.T) {
	doer := reproDoer(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/v2/code-refs/repositories" {
			return reproBroken404(r)
		}
		return reproReply(r, 200, `{"items":[],"totalCount":0}`)
	})
	client, err := providerfoundation.NewHTTPClient("launchdarkly", "https://app.launchdarkly.com", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("launchdarkly", "feature-flags")
	claim.DatasetOptions = map[string]any{"project_key": "payments"}
	batch, cerr := (LaunchDarklyRouteHandler{CodeReferences: staticLaunchDarklyReferenceResolver{}}).Collect(context.Background(), claim, providerfoundation.Credential{Provider: "launchdarkly"}, client, reproNow)
	if cerr == nil && batch.Result["code_references_error"] == nil {
		t.Fatalf("interrupted 404 on code refs: no error, code_references_error nil, watermark=%v", batch.Watermark)
	}
	t.Logf("err=%v result=%v", cerr, batch.Result)
}

// REPRO claim 2: one dependabot item with a numeric created_at drops the whole source.
func TestReproSecurityNumericCreatedAtDoesNotDropSource(t *testing.T) {
	doer := reproDoer(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/repos/acme/api/dependabot/alerts":
			return reproReply(r, 200, `[{"number":1,"state":"open","created_at":"2026-07-22T10:00:00Z","security_advisory":{"severity":"high"},"dependency":{"package":{"name":"w"}}},{"number":2,"state":"open","created_at":1753178400}]`)
		case "/repos/acme/api/code-scanning/alerts", "/repos/acme/api/security-advisories":
			return reproReply(r, 200, `[]`)
		}
		return reproReply(r, 200, gitHubRepositoryFixture)
	})
	batch, err := (GitHubSecurityRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "security"), providerfoundation.Credential{}, gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com"), reproNow)
	reproGoodState(t, batch, err, reproRows(batch))
}
