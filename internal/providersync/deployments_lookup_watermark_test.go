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

// deploymentsLookupCase is how the commit-to-PR (GitHub) or commit-to-MR (GitLab) lookup of the one deployment answers.
type deploymentsLookupCase string

const (
	deploymentsLookup429PastDeadline deploymentsLookupCase = "429 with a Retry-After past the deadline"
	deploymentsLookup429Exhausted    deploymentsLookupCase = "429 with its retries spent inside the deadline"
	deploymentsLookupDeadline        deploymentsLookupCase = "the deadline fires during the lookup"
	deploymentsLookup503             deploymentsLookupCase = "503"
	deploymentsLookupNoPR            deploymentsLookupCase = "no pull request (an empty list)"
	deploymentsLookupHappy           deploymentsLookupCase = "one merged pull request"
)

// deploymentsLookupDoer serves one repository with one deployment (sha abc) and answers its lookup per the case.
type deploymentsLookupDoer struct {
	provider string
	lookup   deploymentsLookupCase
}

func (d deploymentsLookupDoer) Do(request *http.Request) (*http.Response, error) {
	reply := func(status int, header http.Header, body string) (*http.Response, error) {
		header.Set("Content-Type", "application/json")
		return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: header, Request: request,
			Body: io.NopCloser(strings.NewReader(body))}, nil
	}
	path := request.URL.Path
	if strings.HasSuffix(path, "/commits/abc/pulls") || strings.HasSuffix(path, "/repository/commits/abc/merge_requests") {
		switch d.lookup {
		case deploymentsLookup429PastDeadline:
			return reply(http.StatusTooManyRequests, http.Header{"Retry-After": {"30"}}, `{"message":"rate limited"}`)
		case deploymentsLookup429Exhausted:
			return reply(http.StatusTooManyRequests, http.Header{"Retry-After": {"0"}}, `{"message":"rate limited"}`)
		case deploymentsLookupDeadline:
			<-request.Context().Done()
			return nil, request.Context().Err()
		case deploymentsLookup503:
			return reply(http.StatusServiceUnavailable, http.Header{}, `{"message":"unavailable"}`)
		case deploymentsLookupNoPR:
			return reply(http.StatusOK, http.Header{}, `[]`)
		}
		if d.provider == "github" {
			return reply(http.StatusOK, http.Header{}, `[{"number":42,"merged_at":"2026-07-21T10:00:00Z","merge_commit_sha":"abc"}]`)
		}
		return reply(http.StatusOK, http.Header{}, `[{"iid":45,"state":"merged","merged_at":"2026-07-21T10:00:00Z"}]`)
	}
	switch path {
	case "/repos/acme/api":
		return reply(http.StatusOK, http.Header{}, gitHubRepositoryFixture)
	case "/repos/acme/api/deployments":
		return reply(http.StatusOK, http.Header{}, `[{"id":101,"sha":"abc","environment":"production","created_at":"2026-07-22T10:00:00Z"}]`)
	case "/api/v4/projects/123":
		return reply(http.StatusOK, http.Header{}, gitLabRepositoryFixture)
	case "/api/v4/projects/123/deployments":
		return reply(http.StatusOK, http.Header{}, `[{"id":501,"iid":7,"status":"success","environment":{"name":"production"},"created_at":"2026-07-22T10:00:00Z","sha":"abc"}]`)
	}
	return reply(http.StatusOK, http.Header{}, `[]`)
}

// collectDeploymentsLookup runs one deployments unit of the provider with a client of three attempts and a short
// backoff, under a 300 ms deadline.
func collectDeploymentsLookup(t *testing.T, provider string, lookup deploymentsLookupCase) (CompleteRouteBatch, error, Claim) {
	t.Helper()
	base := map[string]string{"github": "https://api.github.com", "gitlab": "https://gitlab.example"}[provider]
	client, err := providerfoundation.NewHTTPClient(provider, base, fakehttp.Client(deploymentsLookupDoer{provider: provider, lookup: lookup}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	defaultGitHubTreeWalkCache.clear()
	claim := nativeTestClaim(provider, "deployments")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	var batch CompleteRouteBatch
	if provider == "github" {
		batch, err = (GitHubDeploymentsRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{}, client, now)
	} else {
		batch, err = (GitLabDeploymentsRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{}, client, now)
	}
	return batch, err, claim
}

// A commit-to-PR lookup that FAILED (rate limited, deadline, 5xx) must not let the unit acknowledge its window: the
// route returns the error, as it does for a failed releases or deployments read (serveLookupFailure), so the batch has
// no watermark and the window is read again; a rate limit keeps its rate_limited class (the unit defers without using
// an attempt). A lookup that answers (no pull request, or one) acknowledges the window as before. RED at main for every
// GitHub failure case and for the GitLab cases that are not a deadline error (gitLabDeploymentControlError already
// returns those).
func TestDeploymentsLookupFailureHoldsTheWatermark(t *testing.T) {
	for _, provider := range []string{"github", "gitlab"} {
		for _, lookup := range []deploymentsLookupCase{deploymentsLookup429PastDeadline, deploymentsLookup429Exhausted, deploymentsLookupDeadline, deploymentsLookup503} {
			t.Run(provider+"/"+string(lookup), func(t *testing.T) {
				batch, err, _ := collectDeploymentsLookup(t, provider, lookup)
				if err == nil || batch.Watermark != nil || len(batch.Effects) != 0 {
					t.Fatalf("err = %v, watermark = %v, %d effect(s): want the lookup's error and no watermark", err, batch.Watermark, len(batch.Effects))
				}
				var providerErr *providerfoundation.ProviderError
				switch lookup {
				case deploymentsLookup429PastDeadline, deploymentsLookup429Exhausted:
					if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
						t.Errorf("err = %v, want the rate_limited class", err)
					}
				case deploymentsLookupDeadline:
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("err = %v, want a deadline error", err)
					}
				}
			})
		}
	}
}

// deploymentsLookupRowsAtMain is the effect rows a lookup that answers gives, as ops main 9c3e2a074c5a writes them
// (recorded with this cell's fixtures).
var deploymentsLookupRowsAtMain = map[string]string{
	"github/" + string(deploymentsLookupNoPR):  `[{"repo_id":"c7198fbc-1945-3717-05d8-eb78866b4e79","deployment_id":"101","status":null,"environment":"production","started_at":null,"finished_at":null,"deployed_at":"2026-07-22T10:00:00Z","merged_at":null,"pull_request_number":null,"release_ref":"101","release_ref_confidence":0.3,"org_id":"org-acme","last_synced":"2026-07-23T12:30:00Z"}]`,
	"github/" + string(deploymentsLookupHappy): `[{"repo_id":"c7198fbc-1945-3717-05d8-eb78866b4e79","deployment_id":"101","status":null,"environment":"production","started_at":null,"finished_at":null,"deployed_at":"2026-07-22T10:00:00Z","merged_at":"2026-07-21T10:00:00Z","pull_request_number":42,"pull_request_tier":"exact_merge_sha","release_ref":"101","release_ref_confidence":0.3,"org_id":"org-acme","last_synced":"2026-07-23T12:30:00Z"}]`,
	"gitlab/" + string(deploymentsLookupNoPR):  `[{"repo_id":"c7198fbc-1945-3717-05d8-eb78866b4e79","deployment_id":"501","status":"success","environment":"production","started_at":null,"finished_at":null,"deployed_at":"2026-07-22T10:00:00Z","merged_at":null,"pull_request_number":null,"release_ref":"7","release_ref_confidence":0.3,"org_id":"org-acme","last_synced":"2026-07-23T12:30:00Z"}]`,
	"gitlab/" + string(deploymentsLookupHappy): `[{"repo_id":"c7198fbc-1945-3717-05d8-eb78866b4e79","deployment_id":"501","status":"success","environment":"production","started_at":null,"finished_at":null,"deployed_at":"2026-07-22T10:00:00Z","merged_at":"2026-07-21T10:00:00Z","pull_request_number":45,"pull_request_tier":"merged_pr","release_ref":"7","release_ref_confidence":0.3,"org_id":"org-acme","last_synced":"2026-07-23T12:30:00Z"}]`,
}

// A lookup that answers acknowledges the window exactly as main: the watermark is the claim's end and the effect rows
// are byte-equal to main's.
func TestDeploymentsLookupAnswerAdvancesTheWatermarkAsMain(t *testing.T) {
	for _, provider := range []string{"github", "gitlab"} {
		for _, lookup := range []deploymentsLookupCase{deploymentsLookupNoPR, deploymentsLookupHappy} {
			t.Run(provider+"/"+string(lookup), func(t *testing.T) {
				batch, err, claim := collectDeploymentsLookup(t, provider, lookup)
				if err != nil || batch.Watermark == nil || claim.BeforeAt == nil || !batch.Watermark.Equal(*claim.BeforeAt) || len(batch.Effects) != 1 {
					t.Fatalf("err = %v, watermark = %v, %d effect(s): want the claim's end and one effect", err, batch.Watermark, len(batch.Effects))
				}
				var rows []string
				for _, row := range batch.Effects[0].Rows {
					rows = append(rows, string(row))
				}
				got := "[" + strings.Join(rows, ",") + "]"
				if want := deploymentsLookupRowsAtMain[provider+"/"+string(lookup)]; got != want {
					t.Errorf("rows = %s\nwant main's %s", got, want)
				}
			})
		}
	}
}
