package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabCommitStatsResponse struct {
	status int
	body   string
	header http.Header
	before func()
	err    error
}

type gitLabCommitStatsDoer struct {
	t         *testing.T
	responses []gitLabCommitStatsResponse
	requests  []string
}

type gitLabCommitStatsReservation struct{}

func (gitLabCommitStatsReservation) Release(context.Context) error { return nil }

type gitLabCommitStatsBudget struct {
	acquires int
	failAt   int
}

func (budget *gitLabCommitStatsBudget) Acquire(
	context.Context,
	providerfoundation.BudgetKey,
) (providerfoundation.Reservation, error) {
	budget.acquires++
	if budget.acquires == budget.failAt {
		return nil, providerfoundation.ErrBudgetUnavailable
	}
	return gitLabCommitStatsReservation{}, nil
}

func (doer *gitLabCommitStatsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request.URL.RequestURI())
	if len(doer.responses) == 0 {
		doer.t.Fatalf("unexpected request %s", request.URL.RequestURI())
	}
	response := doer.responses[0]
	doer.responses = doer.responses[1:]
	if response.before != nil {
		response.before()
	}
	if response.err != nil {
		return nil, response.err
	}
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     response.header,
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}

func TestGitLabCommitStatsRouteFetchesAggregateStatsAcrossCommitPages(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 3, 15, 0, 0, 987654321, time.UTC)
	doer := &gitLabCommitStatsDoer{t: t, responses: []gitLabCommitStatsResponse{
		{body: `{"id":123,"name":"api","path":"group/api","path_with_namespace":"group/api"}`},
		{body: `[{"id":"sha-1","committed_date":"2026-07-22T10:00:00Z"}]`, header: http.Header{"X-Next-Page": []string{"2"}}},
		{body: `[{"id":"sha-2","committed_date":"2026-07-21T10:00:00Z"}]`},
		{body: `{"id":"sha-1","stats":{"additions":"4","deletions":2}}`},
		{body: `{"id":"sha-2","stats":{"additions":1,"deletions":"3"}}`},
	}}
	claim := nativeTestClaim("gitlab", "commit-stats")
	batch, err := (GitLabCommitStatsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "git_commit_stats" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || len(batch.Effects[0].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) ||
		batch.Evidence.Requests != 5 || batch.Evidence.Pages != 2 || batch.Evidence.Records != 2 {
		t.Fatalf("batch=%+v", batch)
	}
	wantRequests := []string{
		"/api/v4/projects/123",
		"/api/v4/projects/123/repository/commits?page=1&per_page=100&since=2026-07-01T00%3A00%3A00Z&until=2026-07-31T23%3A59%3A59Z",
		"/api/v4/projects/123/repository/commits?page=2&per_page=100&since=2026-07-01T00%3A00%3A00Z&until=2026-07-31T23%3A59%3A59Z",
		"/api/v4/projects/123/repository/commits/sha-1",
		"/api/v4/projects/123/repository/commits/sha-2",
	}
	if len(doer.requests) != len(wantRequests) {
		t.Fatalf("requests=%v want=%v", doer.requests, wantRequests)
	}
	for index := range wantRequests {
		if doer.requests[index] != wantRequests[index] {
			t.Fatalf("request[%d]=%q want %q", index, doer.requests[index], wantRequests[index])
		}
	}
	var first commitStatsRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &first); err != nil {
		t.Fatal(err)
	}
	if first.OrgID != claim.OrgID || first.RepoID != "a6a5cafb-6680-a10a-9e41-a5ef763ca016" ||
		first.CommitHash != "sha-1" || first.FilePath != "__AGGREGATE__" ||
		first.Additions != 4 || first.Deletions != 2 || first.OldFileMode != "unknown" ||
		first.NewFileMode != "unknown" || !first.LastSynced.Equal(normalizedAt.Truncate(time.Millisecond)) {
		t.Fatalf("first=%+v", first)
	}
}

func TestGitLabCommitStatsRouteMirrorsPythonSoftDetailFailure(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)
	doer := &gitLabCommitStatsDoer{t: t, responses: []gitLabCommitStatsResponse{
		{body: `{"id":123,"name":"api","path_with_namespace":"group/api"}`},
		{body: `[{"id":"sha-good"},{"id":"sha-degraded"}]`},
		{body: `{"stats":{"additions":4,"deletions":2}}`},
		{status: http.StatusInternalServerError, body: `{"message":"temporary"}`},
	}}
	claim := nativeTestClaim("gitlab", "commit-stats")
	batch, err := (GitLabCommitStatsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 ||
		batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("Python-compatible degraded batch=%+v", batch)
	}
}

func TestGitLabCommitStatsRoutePropagatesAuthenticationDetailFailure(t *testing.T) {
	t.Parallel()
	doer := &gitLabCommitStatsDoer{t: t, responses: []gitLabCommitStatsResponse{
		{body: `{"id":123,"name":"api","path_with_namespace":"group/api"}`},
		{body: `[{"id":"sha-1"}]`},
		{status: http.StatusUnauthorized, body: `{"message":"invalid token"}`},
	}}
	batch, err := (GitLabCommitStatsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "commit-stats"),
		providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"),
		time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorAuthentication {
		t.Fatalf("error=%v want authentication", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("authentication failure advanced state: %+v", batch)
	}
}

func TestGitLabCommitStatsRoutePropagatesDetailLeaseLoss(t *testing.T) {
	t.Parallel()
	doer := &gitLabCommitStatsDoer{t: t, responses: []gitLabCommitStatsResponse{
		{body: `{"id":123,"name":"api","path_with_namespace":"group/api"}`},
		{body: `[{"id":"sha-1"}]`},
	}}
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	assertions := 0
	client.Lease = providerfoundation.LeaseGuardFunc(func(context.Context) error {
		assertions++
		if assertions == 5 {
			return providerfoundation.ErrLeaseLost
		}
		return nil
	})
	batch, err := (GitLabCommitStatsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "commit-stats"),
		providerfoundation.Credential{}, client,
		time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("error=%v want lease lost", err)
	}
	if assertions != 5 || len(doer.requests) != 2 ||
		!strings.Contains(doer.requests[1], "/repository/commits?") {
		t.Fatalf("assertions=%d requests=%v; loss was not at detail entry", assertions, doer.requests)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("lease loss advanced state: %+v", batch)
	}
}

func TestGitLabCommitStatsRoutePropagatesDetailBudgetDenial(t *testing.T) {
	t.Parallel()
	doer := &gitLabCommitStatsDoer{t: t, responses: []gitLabCommitStatsResponse{
		{body: `{"id":123,"name":"api","path_with_namespace":"group/api"}`},
		{body: `[{"id":"sha-1"}]`},
	}}
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	budget := &gitLabCommitStatsBudget{failAt: 3}
	client.Budget = budget
	batch, err := (GitLabCommitStatsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "commit-stats"),
		providerfoundation.Credential{}, client,
		time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, providerfoundation.ErrBudgetUnavailable) {
		t.Fatalf("error=%v want budget unavailable", err)
	}
	if budget.acquires != 3 || len(doer.requests) != 2 ||
		!strings.Contains(doer.requests[1], "/repository/commits?") {
		t.Fatalf("acquires=%d requests=%v; denial was not at detail entry", budget.acquires, doer.requests)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("budget denial advanced state: %+v", batch)
	}
}

func TestGitLabCommitStatsRoutePropagatesDetailContextFailure(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		setup func() (context.Context, func(), error)
	}{
		{
			name: "canceled",
			setup: func() (context.Context, func(), error) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel, context.Canceled
			},
		},
		{
			name: "deadline exceeded",
			setup: func() (context.Context, func(), error) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				return ctx, func() {
					defer cancel()
					<-ctx.Done()
				}, context.DeadlineExceeded
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, before, want := test.setup()
			doer := &gitLabCommitStatsDoer{t: t, responses: []gitLabCommitStatsResponse{
				{body: `{"id":123,"name":"api","path_with_namespace":"group/api"}`},
				{body: `[{"id":"sha-1"}]`},
				{before: before, err: want},
			}}
			batch, err := (GitLabCommitStatsRouteHandler{}).Collect(
				ctx, nativeTestClaim("gitlab", "commit-stats"),
				providerfoundation.Credential{},
				gitLabRepositoryClient(t, doer, "https://gitlab.example"),
				time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC),
			)
			if !errors.Is(err, want) {
				t.Fatalf("error=%v want %v", err, want)
			}
			if len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("context failure advanced state: %+v", batch)
			}
		})
	}
}

func TestGitLabCommitStatsFatalDetailErrorClassification(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		err   error
		fatal bool
	}{
		{"authentication", &providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication}, true},
		{"rate limited", &providerfoundation.ProviderError{Class: providerfoundation.ErrorRateLimited}, true},
		{"provider cancelled", &providerfoundation.ProviderError{Class: providerfoundation.ErrorCancelled}, true},
		{"context canceled", context.Canceled, true},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"foundation lease lost", providerfoundation.ErrLeaseLost, true},
		{"sync lease lost", ErrLeaseLost, true},
		{"budget unavailable", providerfoundation.ErrBudgetUnavailable, true},
		{"credential invalid", providerfoundation.ErrCredentialInvalid, true},
		{"transient detail", &providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient}, false},
		{"not found detail", &providerfoundation.ProviderError{Class: providerfoundation.ErrorNotFound}, false},
		{"conflict detail", &providerfoundation.ProviderError{Class: providerfoundation.ErrorConflict}, false},
		{"permanent detail", &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent}, false},
		{"malformed detail", providerfoundation.ErrNormalizationInvalid, false},
		{
			"wrapped malformed detail",
			fmt.Errorf("decode detail: %w", providerfoundation.ErrNormalizationInvalid),
			false,
		},
		{
			"joined malformed details",
			errors.Join(
				providerfoundation.ErrNormalizationInvalid,
				providerfoundation.ErrNormalizationInvalid,
			),
			false,
		},
		{"unknown provider class", &providerfoundation.ProviderError{Class: "future-control-plane-class"}, true},
		{
			"joined transient and budget",
			errors.Join(
				&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient},
				providerfoundation.ErrBudgetUnavailable,
			),
			true,
		},
		{
			"nested not found and lease",
			fmt.Errorf(
				"detail request: %w",
				errors.Join(
					&providerfoundation.ProviderError{Class: providerfoundation.ErrorNotFound},
					fmt.Errorf("release reservation: %w", providerfoundation.ErrLeaseLost),
				),
			),
			true,
		},
		{
			"joined normalization and budget",
			errors.Join(
				providerfoundation.ErrNormalizationInvalid,
				providerfoundation.ErrBudgetUnavailable,
			),
			true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := gitLabCommitStatsFatalDetailError(context.Background(), test.err)
			if (got != nil) != test.fatal {
				t.Fatalf("fatal error=%v want fatal=%t", got, test.fatal)
			}
		})
	}
}

func TestGitLabCommitStatsRouteDeduplicatesAcceptedHashesBeforeSelection(t *testing.T) {
	t.Parallel()
	doer := &gitLabCommitStatsDoer{t: t, responses: []gitLabCommitStatsResponse{
		{body: `{"id":123,"name":"api","path_with_namespace":"group/api"}`},
		{body: `[{"id":"sha-1"},{"id":"sha-1"},{"id":"sha-2"}]`},
		{body: `{"stats":{"additions":1,"deletions":2}}`},
		{body: `{"stats":{"additions":3,"deletions":4}}`},
	}}
	batch, err := (GitLabCommitStatsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "commit-stats"),
		providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"),
		time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 4 || len(batch.Effects) != 1 ||
		len(batch.Effects[0].Rows) != 2 || batch.Evidence.Records != 2 {
		t.Fatalf("requests=%v batch=%+v", doer.requests, batch)
	}
	var first, second commitStatsRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(batch.Effects[0].Rows[1], &second); err != nil {
		t.Fatal(err)
	}
	if first.CommitHash != "sha-1" || first.Additions != 1 ||
		second.CommitHash != "sha-2" || second.Additions != 3 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
}

func TestGitLabCommitStatsRouteFailsClosedOnListCapAndRateLimit(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		handler   GitLabCommitStatsRouteHandler
		responses []gitLabCommitStatsResponse
		want      providerfoundation.ErrorClass
	}{
		{
			name: "list cap", handler: GitLabCommitStatsRouteHandler{MaxPages: 1},
			responses: []gitLabCommitStatsResponse{
				{body: `{"id":123,"name":"api","path_with_namespace":"group/api"}`},
				{body: `[{"id":"sha-1"}]`, header: http.Header{"X-Next-Page": []string{"2"}}},
			},
			want: providerfoundation.ErrorPermanent,
		},
		{
			name: "detail rate limit",
			responses: []gitLabCommitStatsResponse{
				{body: `{"id":123,"name":"api","path_with_namespace":"group/api"}`},
				{body: `[{"id":"sha-1"}]`},
				{status: http.StatusTooManyRequests, body: `{"message":"slow down"}`},
			},
			want: providerfoundation.ErrorRateLimited,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitLabCommitStatsDoer{t: t, responses: test.responses}
			batch, err := test.handler.Collect(
				context.Background(), nativeTestClaim("gitlab", "commit-stats"),
				providerfoundation.Credential{},
				gitLabRepositoryClient(t, doer, "https://gitlab.example"), now,
			)
			if test.name == "list cap" {
				if !errors.Is(err, ErrPaginationCapExceeded) {
					t.Fatalf("error=%v want %v", err, ErrPaginationCapExceeded)
				}
			} else {
				var providerErr *providerfoundation.ProviderError
				if !errors.As(err, &providerErr) || providerErr.Class != test.want {
					t.Fatalf("error=%v want class %s", err, test.want)
				}
			}
			if len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("incomplete batch leaked state: %+v", batch)
			}
		})
	}
}

func TestGitLabCommitStatsRouteRejectsCrossScopeMalformedIdentityAndProjectMismatch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)
	wrongProvider := nativeTestClaim("github", "commit-stats")
	wrongProvider.SourceExternalID = "123"
	for name, test := range map[string]struct {
		claim          Claim
		clientProvider string
		project        string
		commits        string
		want           error
	}{
		"wrong claim provider": {
			claim: wrongProvider, clientProvider: "gitlab",
			project: `{"id":123,"path_with_namespace":"group/api"}`, commits: `[]`,
			want: ErrInvalidConfiguration,
		},
		"wrong claim dataset": {
			claim: nativeTestClaim("gitlab", "commits"), clientProvider: "gitlab",
			project: `{"id":123,"path_with_namespace":"group/api"}`, commits: `[]`,
			want: ErrInvalidConfiguration,
		},
		"wrong client provider": {
			claim: nativeTestClaim("gitlab", "commit-stats"), clientProvider: "github",
			project: `{"id":123,"path_with_namespace":"group/api"}`, commits: `[]`,
			want: ErrInvalidConfiguration,
		},
		"project id mismatch": {
			claim: nativeTestClaim("gitlab", "commit-stats"), clientProvider: "gitlab",
			project: `{"id":99,"path_with_namespace":"group/api"}`, commits: `[]`,
			want: providerfoundation.ErrNormalizationInvalid,
		},
		"missing commit identity": {
			claim: nativeTestClaim("gitlab", "commit-stats"), clientProvider: "gitlab",
			project: `{"id":123,"path_with_namespace":"group/api"}`,
			commits: `[{}]`, want: providerfoundation.ErrNormalizationInvalid,
		},
	} {
		t.Run(name, func(t *testing.T) {
			doer := &gitLabCommitStatsDoer{t: t, responses: []gitLabCommitStatsResponse{
				{body: test.project}, {body: test.commits},
			}}
			client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
			client.Provider = test.clientProvider
			batch, err := (GitLabCommitStatsRouteHandler{}).Collect(
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

func TestGitLabCommitStatsRouteFiltersWindowBeforeDetailExpansion(t *testing.T) {
	t.Parallel()
	doer := &gitLabCommitStatsDoer{t: t, responses: []gitLabCommitStatsResponse{
		{body: `{"id":123,"name":"api","path_with_namespace":"group/api"}`},
		{body: `[
			{"id":"future","committed_date":"2026-08-01T00:00:00Z"},
			{"id":"inside","committed_date":"2026-07-20T00:00:00Z"},
			{"id":"old","committed_date":"2026-06-01T00:00:00Z"}
		]`},
		{body: `{"stats":{"additions":1,"deletions":2}}`},
	}}
	batch, err := (GitLabCommitStatsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "commit-stats"),
		providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"),
		time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 3 || !strings.HasSuffix(doer.requests[2], "/inside") {
		t.Fatalf("detail requests=%v", doer.requests)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("batch=%+v", batch)
	}
}

func TestGitLabCommitStatsRouteUsesProjectPathBeforeAmbiguousName(t *testing.T) {
	t.Parallel()
	repoIDs := map[string]string{}
	for _, group := range []string{"group-a", "group-b"} {
		fullPath := group + "/widgets"
		wantID, err := repositoryIdentity(fullPath)
		if err != nil {
			t.Fatal(err)
		}
		doer := &gitLabCommitStatsDoer{t: t, responses: []gitLabCommitStatsResponse{
			{body: `{"id":123,"name":"widgets","path":"` + fullPath + `","path_with_namespace":""}`},
			{body: `[{"id":"sha-1"}]`},
			{body: `{"stats":{"additions":1,"deletions":2}}`},
		}}
		batch, collectErr := (GitLabCommitStatsRouteHandler{}).Collect(
			context.Background(), nativeTestClaim("gitlab", "commit-stats"),
			providerfoundation.Credential{},
			gitLabRepositoryClient(t, doer, "https://gitlab.example"),
			time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC),
		)
		if collectErr != nil {
			t.Fatal(collectErr)
		}
		var row commitStatsRow
		if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
			t.Fatal(err)
		}
		if row.RepoID != wantID || batch.Result["repo"] != fullPath {
			t.Fatalf("repo=%v row=%+v want id=%s", batch.Result["repo"], row, wantID)
		}
		repoIDs[group] = row.RepoID
	}
	if repoIDs["group-a"] == repoIDs["group-b"] {
		t.Fatalf("cross-group repositories collapsed to %s", repoIDs["group-a"])
	}
}

func TestGitLabCommitStatsRoutePreservesPythonSelectionLimits(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		fullSync  bool
		available int
		want      int
	}{
		{name: "full sync samples fifty", fullSync: true, available: 51, want: 50},
		{name: "incremental respects three hundred cap", available: 301, want: 300},
	} {
		t.Run(test.name, func(t *testing.T) {
			responses := []gitLabCommitStatsResponse{{
				body: `{"id":123,"path_with_namespace":"group/api"}`,
			}}
			pages := 0
			for start := 0; start < test.available; start += defaultGitLabCommitStatsPerPage {
				end := min(start+defaultGitLabCommitStatsPerPage, test.available)
				var commits strings.Builder
				commits.WriteByte('[')
				for index := start; index < end; index++ {
					if index > start {
						commits.WriteByte(',')
					}
					commits.WriteString(`{"id":"sha-`)
					commits.WriteString(strconv.Itoa(index))
					commits.WriteString(`"}`)
				}
				commits.WriteByte(']')
				response := gitLabCommitStatsResponse{body: commits.String()}
				if end < test.available {
					response.header = http.Header{
						"X-Next-Page": []string{strconv.Itoa(pages + 2)},
					}
				}
				responses = append(responses, response)
				pages++
			}
			for range test.want {
				responses = append(responses, gitLabCommitStatsResponse{
					body: `{"stats":{"additions":1,"deletions":2}}`,
				})
			}
			doer := &gitLabCommitStatsDoer{t: t, responses: responses}
			claim := nativeTestClaim("gitlab", "commit-stats")
			if test.fullSync {
				claim.SinceAt = nil
			}
			batch, err := (GitLabCommitStatsRouteHandler{}).Collect(
				context.Background(), claim, providerfoundation.Credential{},
				gitLabRepositoryClient(t, doer, "https://gitlab.example"),
				time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC),
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != test.want ||
				batch.Evidence.Records != test.want || len(doer.requests) != test.want+pages+1 {
				t.Fatalf("batch=%+v requests=%d want rows=%d", batch, len(doer.requests), test.want)
			}
		})
	}
}
