package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabCommitsResponse struct {
	status  int
	body    string
	headers http.Header
}

type gitLabCommitsDoer struct {
	t         *testing.T
	responses []gitLabCommitsResponse
	requests  []*http.Request
}

func (doer *gitLabCommitsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request)
	if len(doer.responses) == 0 {
		doer.t.Fatalf("unexpected request %s", request.URL)
	}
	response := doer.responses[0]
	doer.responses = doer.responses[1:]
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     response.headers,
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}

func TestGitLabCommitsRouteEmitsCompleteNullableEffectAcrossPages(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 987654321, time.UTC)
	fallbackNow := time.Date(2026, 8, 3, 11, 59, 0, 0, time.UTC)
	doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[
			{"id":"after-window","message":"future","author_name":"A","authored_date":"2026-08-01T00:00:00Z","committer_name":"C","committed_date":"2026-08-04T00:00:00Z","parent_ids":[]},
			{"id":"sha-1","message":null,"author_name":null,"authored_date":null,"committer_name":"Grace","committed_date":"2026-07-20T10:00:00Z","parent_ids":["p1","p2"]}
		]`, headers: http.Header{"X-Next-Page": []string{"4"}}},
		{body: `[
			{"id":"sha-2","message":"ship it","author_name":"Ada","authored_date":"2026-07-10T09:00:00Z","committer_name":null,"committed_date":null,"parent_ids":[]},
			{"id":"before-window","message":"old","author_name":"A","authored_date":"2026-06-01T00:00:00Z","committer_name":"C","committed_date":"2026-06-01T00:00:00Z","parent_ids":[]}
		]`},
		{body: `[]`},
	}}
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	claim := nativeTestClaim("gitlab", "commits")
	batch, err := (GitLabCommitsRouteHandler{
		PerPage:  2,
		MaxPages: 10,
		Now:      func() time.Time { return fallbackNow },
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 4 {
		t.Fatalf("requests=%d", len(doer.requests))
	}
	if got := doer.requests[1].URL.Query(); got.Get("page") != "1" ||
		got.Get("per_page") != "2" || got.Get("since") != "2026-07-01T00:00:00Z" ||
		got.Get("until") != "2026-07-31T23:59:59Z" {
		t.Fatalf("first page query=%s", got.Encode())
	}
	if got := doer.requests[2].URL.Query().Get("page"); got != "4" {
		t.Fatalf("second page=%q want 4", got)
	}
	if got := doer.requests[3].URL.Query().Get("page"); got != "5" {
		t.Fatalf("fallback page=%q want 5", got)
	}
	if batch.Watermark == nil || claim.BeforeAt == nil ||
		!batch.Watermark.Equal(*claim.BeforeAt) || batch.Evidence.Requests != 4 ||
		batch.Evidence.Pages != 3 || batch.Evidence.Records != 2 ||
		batch.Evidence.CapReached {
		t.Fatalf("watermark=%v evidence=%+v", batch.Watermark, batch.Evidence)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "git_commits" ||
		batch.Effects[0].Recovery != EffectReadbackRequired ||
		len(batch.Effects[0].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var first, second gitCommitRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(batch.Effects[0].Rows[1], &second); err != nil {
		t.Fatal(err)
	}
	if first.OrgID != claim.OrgID || first.RepoID != "c7198fbc-1945-3717-05d8-eb78866b4e79" ||
		first.Hash != "sha-1" || first.Message != nil || first.AuthorName != "Unknown" ||
		first.AuthorEmail != nil || !first.AuthorWhen.Equal(fallbackNow) ||
		first.CommitterName != "Grace" || first.CommitterEmail != nil ||
		first.Parents != 2 || !first.LastSynced.Equal(normalizedAt.Truncate(time.Millisecond)) {
		t.Fatalf("first=%+v", first)
	}
	if second.Message == nil || *second.Message != "ship it" ||
		second.CommitterName != "Unknown" || !second.CommitterWhen.Equal(fallbackNow) {
		t.Fatalf("second=%+v", second)
	}
}

func TestGitLabCommitsRouteFailsClosedOnCapMalformedAndPartialFetch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("gitlab", "commits")
	valid := `{"id":"sha-1","committed_date":"2026-07-20T10:00:00Z","parent_ids":[]}`
	for name, test := range map[string]struct {
		responses []gitLabCommitsResponse
		handler   GitLabCommitsRouteHandler
		want      error
	}{
		"cap": {
			responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}, {body: `[` + valid + `]`, headers: http.Header{"X-Next-Page": []string{"2"}}}},
			handler:   GitLabCommitsRouteHandler{PerPage: 1, MaxPages: 1},
			want:      ErrPaginationCapExceeded,
		},
		"malformed commit": {
			responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}, {body: `[{"message":"missing identity"}]`}},
			handler:   GitLabCommitsRouteHandler{PerPage: 100, MaxPages: 2},
			want:      providerfoundation.ErrNormalizationInvalid,
		},
		"partial fetch": {
			responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}, {body: `[` + valid + `]`, headers: http.Header{"X-Next-Page": []string{"2"}}}, {status: http.StatusServiceUnavailable, body: `{}`}},
			handler:   GitLabCommitsRouteHandler{PerPage: 1, MaxPages: 2},
			want:      &providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient},
		},
	} {
		t.Run(name, func(t *testing.T) {
			doer := &gitLabCommitsDoer{t: t, responses: test.responses}
			batch, err := test.handler.Collect(
				context.Background(), claim, providerfoundation.Credential{},
				gitLabRepositoryClient(t, doer, "https://gitlab.example"), now,
			)
			matches := errors.Is(err, test.want)
			var gotProvider, wantProvider *providerfoundation.ProviderError
			if errors.As(err, &gotProvider) && errors.As(test.want, &wantProvider) {
				matches = gotProvider.Class == wantProvider.Class
			}
			if !matches {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
			if len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("failed batch=%+v", batch)
			}
		})
	}
}

func TestGitLabCommitsRouteRejectsCrossScopeAndProjectMismatch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	wrongProviderClaim := nativeTestClaim("github", "commits")
	wrongProviderClaim.SourceExternalID = "42"
	for name, test := range map[string]struct {
		claim          Claim
		clientProvider string
		project        string
		want           error
	}{
		"wrong claim provider": {
			claim: wrongProviderClaim, clientProvider: "gitlab",
			project: gitLabRepositoryFixture, want: ErrInvalidConfiguration,
		},
		"wrong claim dataset": {
			claim: nativeTestClaim("gitlab", "deployments"), clientProvider: "gitlab",
			project: gitLabRepositoryFixture, want: ErrInvalidConfiguration,
		},
		"wrong client provider": {
			claim: nativeTestClaim("gitlab", "commits"), clientProvider: "github",
			project: gitLabRepositoryFixture, want: ErrInvalidConfiguration,
		},
		"project id mismatch": {
			claim: nativeTestClaim("gitlab", "commits"), clientProvider: "gitlab",
			project: `{"id":99,"name":"widgets","path_with_namespace":"acme/widgets"}`,
			want:    providerfoundation.ErrNormalizationInvalid,
		},
	} {
		t.Run(name, func(t *testing.T) {
			doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
				{body: test.project}, {body: `[]`},
			}}
			client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
			client.Provider = test.clientProvider
			batch, err := (GitLabCommitsRouteHandler{}).Collect(
				context.Background(), test.claim, providerfoundation.Credential{}, client, now,
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want %v", err, test.want)
			}
			if len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("failed scope emitted batch=%+v", batch)
			}
		})
	}
}

func TestCompareGitCommitVersionDistinguishesNullAndEmptyMessage(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	expected := gitCommitRow{
		OrgID: "org", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
		Hash: "abc", Message: nil, AuthorName: "author", AuthorWhen: now,
		CommitterName: "committer", CommitterWhen: now, LastSynced: now,
	}
	empty := ""
	actual := gitCommitVersion{Row: expected, LastSynced: now, Found: true}
	actual.Row.Message = &empty
	if got := compareCommitVersion(expected, actual); got != EffectConflict {
		t.Fatalf("null versus empty message inspection=%s", got)
	}
}

func TestGitLabCommitsRoutePreservesHTTPClassification(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		status int
		class  providerfoundation.ErrorClass
	}{
		{http.StatusUnauthorized, providerfoundation.ErrorAuthentication},
		{http.StatusForbidden, providerfoundation.ErrorAuthentication},
		{http.StatusNotFound, providerfoundation.ErrorNotFound},
		{http.StatusTooManyRequests, providerfoundation.ErrorRateLimited},
		{http.StatusServiceUnavailable, providerfoundation.ErrorTransient},
	} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{{
				status: test.status, body: `{}`,
			}}}
			batch, err := (GitLabCommitsRouteHandler{}).Collect(
				context.Background(), nativeTestClaim("gitlab", "commits"),
				providerfoundation.Credential{},
				gitLabRepositoryClient(t, doer, "https://gitlab.example"), now,
			)
			var providerErr *providerfoundation.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != test.class {
				t.Fatalf("error=%v class=%v", err, test.class)
			}
			if len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("failed batch=%+v", batch)
			}
		})
	}
}
